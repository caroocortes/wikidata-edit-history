import requests
import bz2
from pathlib import Path
import re
from psycopg2.extras import execute_batch
import psycopg2
from math import radians, cos, sin, asin, sqrt
from bs4 import BeautifulSoup
import json
import hashlib
from urllib.parse import urljoin
import sys
from io import StringIO
from dateutil import parser
from io import StringIO
import os
import pandas as pd
import os
import psutil

from scripts.const import WIKIDATA_SERVICE_URL, DOWNLOAD_LINKS_FILE_PATH

def total_memory_usage():
    """Get total memory including all child processes in MB"""
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss
    
    for child in process.children(recursive=True):
        try:
            mem += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    
    return mem / (1024 * 1024)  # to MB

def get_time_unit(elapsed_time):
    """
    Convert elapsed time in seconds to appropriate unit.
    Returns (value, unit)
    """
    if elapsed_time >= 86400:  # 60*60*24 = 86400 seconds in a day
        return elapsed_time / 86400, 'days'
    elif elapsed_time >= 3600:  # 60*60 = 3600 seconds in an hour
        return elapsed_time / 3600, 'hours'
    elif elapsed_time >= 60:
        return elapsed_time / 60, 'minutes'
    else:
        return elapsed_time, 'seconds'

"""
    Helper methods for magnitude of change calculation
"""
def haversine_metric(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers.
    return c * r

def to_astronomical(year):
    """
        From Wikidata documentation:
        Years BCE are represented as negative numbers, using the historical numbering, 
        in which year 0 is undefined, and the year 1 BCE is represented as -0001, the year 44 BCE is represented as -0044, etc., 
        like XSD 1.0 (ISO 8601:1988) does.

        From BCE to astronomical:
        - Subtract one from the BCE year and prepend a negative sign (e.g. 1 BCE -> 0, 2 BCE -> -1, 10 BCE -> -9, and 100 BCE -> -99)
        Since Wikidata stores BCE as negative numbers, we need to add 1 if the year is negative:
    """

    if year < 0:
        return year + 1  # convert historical BCE to astronomical
    return year

def gregorian_to_julian(y, month, day):
    """
        See https://en.wikipedia.org/wiki/Julian_day for formula
    """
    year = to_astronomical(y)

    m_14_12 = (month - 14) // 12

    A = (1461 * (year + 4800 + m_14_12) ) // 4
    B = (367 * (month - 2 - 12 * m_14_12) ) // 12
    C = (3 * ( (year + 4900 + m_14_12) // 100)) // 4

    return A + B - C + day - 32075

def get_time_dict(timestring):

    STANDARD_DATE_REGEX = re.compile(
        r"""
        (?P<year>[+-]?\d+?)-
        (?P<month>\d\d)-
        (?P<day>\d\d)T
        (?P<hour>\d\d):
        (?P<minute>\d\d):
        (?P<second>\d\d)Z?""",
        re.VERBOSE,
    )

    datetime_dict = {}
    match = STANDARD_DATE_REGEX.fullmatch(timestring)
    if match:
        datetime_dict = {
            "year": int(match.group("year")), # year already has the sign
            "month": int(match.group("month")),
            "day": int(match.group("day")),
            "hour": int(match.group("hour")),
            "minute": int(match.group("minute")),
            "second": int(match.group("second")),
        }

    return datetime_dict

    
"""
    Methods for inserting in DB
"""

def update_entity_label(conn, entity_id, entity_label):
    """
    Update entity_label in the entity table.
    
    :param conn: psycopg2 connection
    """

    query = """
        UPDATE revision
        SET entity_label = %s
        WHERE entity_id = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (entity_label, entity_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Update of label ({entity_label}) for entity {entity_id} failed: {e}")

def insert_rows_copy(conn, table_name, rows, columns, conflict_column=None):
    """
    Insert rows with conflict handling
    
    Args:
        conflict_column: Primary key column(s) for conflict detection
        update_columns: Columns to update on conflict (None = skip updates, DO NOTHING)
    """
    if not rows:
        return
    
    cursor = conn.cursor()
    buffer = StringIO()
    try:
        temp_table = f"{table_name}_temp_{os.getpid()}"
        
        # Create temp table
        cursor.execute(f"""
            CREATE TEMP TABLE {temp_table} 
            (LIKE {table_name} INCLUDING DEFAULTS)
            ON COMMIT DROP
        """)
        
        # COPY to temp table
        for row in rows:
            line_items = []
            for i, val in enumerate(row):
                if val is None:
                    line_items.append('\\N')
                elif val == '':
                    line_items.append('')
                else:
                    val_str = str(val)
                    val_str = val_str.replace('\\', '\\\\')
                    val_str = val_str.replace('"', '\\"')
                    val_str = val_str.replace('\t', '\\t')
                    val_str = val_str.replace('\n', '\\n')
                    val_str = val_str.replace('\r', '\\r')
                    line_items.append(val_str)
            buffer.write('\t'.join(line_items) + '\n')
        
        buffer.seek(0)
        column_names = ', '.join(columns)
        copy_query = f"COPY {temp_table} ({column_names}) FROM STDIN"
        cursor.copy_expert(copy_query, buffer)
        
        # Insert with conflict handling
        if conflict_column:
            if isinstance(conflict_column, list):
                conflict_cols = ', '.join(conflict_column)
            else:
                conflict_cols = conflict_column
            
            
            if 'entity_stat' not in table_name and 'feature' not in table_name:
                # DO NOTHING on conflict
                # if it's not an entity_stats or feature table, then I don't need to update existing rows
                insert_query = f"""
                    INSERT INTO {table_name} ({column_names})
                    SELECT {column_names} FROM {temp_table}
                    ON CONFLICT ({conflict_cols}) DO NOTHING
                """
            else:
            # if it's an entity_stats or feature table, then I need to update existing rows, because I want to update the counts
            # update only the non-conflict columns (no key columns)
                insert_query = f"""
                    INSERT INTO {table_name} ({column_names})
                    SELECT {column_names} FROM {temp_table}
                    ON CONFLICT ({conflict_cols}) DO UPDATE SET         
                    {', '.join([f'{col} = EXCLUDED.{col}' for col in columns if col not in conflict_column])}
                """
        else:
            insert_query = f"""
                INSERT INTO {table_name} ({column_names})
                SELECT {column_names} FROM {temp_table}
            """
        
        cursor.execute(insert_query)
        rows_affected = cursor.rowcount
        
        conn.commit()
        
        return rows_affected
        
    except Exception as e:
        conn.rollback()
        print(f"COPY failed for {table_name}: {e}")
        raise
    finally:
        cursor.close()
        buffer.close()

    
def insert_rows(conn, table_name, rows, columns):
    if not rows:
        return
    
    cursor = conn.cursor()
    
    try:
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        cursor.executemany(query, rows)
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        print(f"Insert failed for {table_name}: {e}")
        print(f"Sample row: {rows[0]}")
        print(f"Columns: {columns}")
        raise
    finally:
        cursor.close()

def create_db_schema(set_up):
    base_dir = Path(__file__).resolve().parent.parent
    
    change_extraction_filters = set_up['change_extraction_filters']

    change_schema_file_path = f"{base_dir}/sql/change_schema.sql"
    features_file_path = f"{base_dir}/sql/features_schema.sql"
    datatype_metadata_file_path = f"{base_dir}/sql/datatype_metadata_schema.sql"
    
    with open(change_schema_file_path, "r", encoding="utf-8") as f:
        change_schema_template = f.read()

    with open(features_file_path, "r", encoding="utf-8") as f:
        features_file_template = f.read()

    with open(datatype_metadata_file_path, "r", encoding="utf-8") as f:
        datatype_metadata_schema_template = f.read()
    
    base_query = change_schema_template.replace("{suffix}", "")

    filters_rest = change_extraction_filters.get('rest', {})
    if filters_rest.get('feature_extraction', False):
        # load schema for rest features
        query_fe_rest = features_file_template.replace("{suffix}", "")
        base_query += "\n" + query_fe_rest

    if filters_rest.get('datatype_metadata_extraction', False):
        # load schema for datatype metadata
        query_dm = datatype_metadata_schema_template.replace("{suffix}", "")
        base_query += "\n" + query_dm

    #  ---------------------------------------------
    #  Scholarly articles
    #  ---------------------------------------------
    filters_sa = change_extraction_filters.get('scholarly_articles_filter', {})
    if filters_sa.get('extract', False):
        # load schema for scholarly articles
        query_sa = change_schema_template.replace("{suffix}", "_sa")
        base_query += "\n" + query_sa

        if filters_sa.get('feature_extraction', False):
            # load feature extraction schema for scholarly articles
            query_fe_sa = features_file_template.replace("{suffix}", "_sa")
            base_query += "\n" + query_fe_sa

        if filters_sa.get('datatype_metadata_extraction', False):
            # load schema for datatype metadata
            query_dm_sa = datatype_metadata_schema_template.replace("{suffix}", "_sa")
            base_query += "\n" + query_dm_sa

    #  ---------------------------------------------
    #  Astronomical objects
    #  ---------------------------------------------
    filters_ao = change_extraction_filters.get('astronomical_objects_filter', {})
    if filters_ao.get('extract', False):
        # load schema for astronomical objects
        query_ao = change_schema_template.replace("{suffix}", "_ao")
        base_query += "\n" + query_ao

        if filters_ao.get('feature_extraction', False):
            query_fe_ao = features_file_template.replace("{suffix}", "_ao")
            base_query += "\n" + query_fe_ao

        if filters_ao.get('datatype_metadata_extraction', False):
            # load schema for datatype metadata
            query_dm_ao = datatype_metadata_schema_template.replace("{suffix}", "_ao")
            base_query += "\n" + query_dm_ao
    
    #  ---------------------------------------------
    #  Less than X value & rank changes
    #  ---------------------------------------------
    filters_less = change_extraction_filters.get('less_filter', {})
    if filters_less.get('extract', False):
        # load schema for less
        query_less = change_schema_template.replace("{suffix}", "_less")
        base_query += "\n" + query_less

        if filters_less.get('feature_extraction', False):
            query_fe_less = features_file_template.replace("{suffix}", "_less")
            base_query += "\n" + query_fe_less

        if filters_less.get('datatype_metadata_extraction', False):
            # load schema for datatype metadata
            query_dm_less = datatype_metadata_schema_template.replace("{suffix}", "_less")
            base_query += "\n" + query_dm_less

    try:
        script_dir = Path(__file__).parent
        db_config_path = script_dir.parent / set_up.get("db_config_path", "config/db_config.json")
        with open(db_config_path) as f:
            config = json.load(f)

        conn = psycopg2.connect(
            dbname=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASS"], 
            host=config["DB_HOST"],
            port=config["DB_PORT"]
        )

        cursor = conn.cursor()

        cursor.execute(query=base_query)

        conn.commit()
        cursor.close()

    except Exception as e:
        print(f'Error when saving or connecting to DB: {e}')
        sys.exit(1)


""" Other utility methods """

def human_readable_size(size, decimal_places=2):
    for unit in ['B','KB','MB','GB','TB']:
        if size < 1024:
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024

def print_exception_details(e, file_path):
    # Get the error position
    err_line = e.getLineNumber()
    err_col = e.getColumnNumber()

    print(f"Error at line {err_line}, column {err_col}")

    # Reopen the file and get surrounding lines
    with bz2.open(file_path, 'rt', encoding='utf-8') as f_err:
        lines = []
        for i, line in enumerate(f_err, start=1):
            if i >= err_line - 14 and i <= err_line + 4:  # 14 lines before, 4 after
                lines.append((i, line.rstrip("\n")))
            if i > err_line + 1:
                break

    print("\n--- XML snippet around error ---")
    for ln, txt in lines:
        prefix = ">>" if ln == err_line else "  "
        print(f"{prefix} Line {ln}: {txt}")
    print("-------------------------------")

def get_dump_links():
    #  Get list of .bz2 files from the wikidata dump service (Scrapper)
    response = requests.get(WIKIDATA_SERVICE_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    bz2_links = []
    for link in soup.find_all("a"):
        href = link.get("href", "")
        if "pages-meta-history" in href and href.endswith(".bz2"):
            full_url = urljoin(WIKIDATA_SERVICE_URL, href)
            bz2_links.append(full_url)

    print(f"Found {len(bz2_links)} .bz2 dump files.")
    print(f"Saving download links to {DOWNLOAD_LINKS_FILE_PATH}")
    with open(DOWNLOAD_LINKS_FILE_PATH, 'w', encoding='utf-8') as f:
        for file in bz2_links:
            f.write(f"{file}\n")
    
    return bz2_links

def id_to_int(wd_id):
    """
    Converts Wikidata ID like Q38830 or P31 to integer.
    """
    return int(wd_id[1:])

def make_sah1_value_id(value_json):
    # creates a sha1 hash from the json representation of the value
    # to uniquely identify it

    # sha1 always returns the same hash for the same input
    norm = json.dumps(value_json, sort_keys=True, separators=(',', ':'))
    return hashlib.sha1(norm.encode('utf-8')).hexdigest()

def get_time_feature(timestamp, option='year'):

    if isinstance(timestamp, str):
        dt = parser.parse(timestamp)
    else:
        dt = timestamp  
    
    if option == 'year':
        return str(dt.year)
    
    elif option == 'year_month':
        return dt.strftime('%Y-%m')  # e.g., '2017-09'
    
    elif option == 'week':
        # ISO week number with year
        return dt.strftime('%Y-W%V')  # e.g., '2017-W37'
    else:
        return timestamp


def query_to_df(conn, query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            
            if cur.description is not None:
                # Get column names
                colnames = [desc[0] for desc in cur.description]
                # Fetch all rows
                rows = cur.fetchall()
                # Return as Pandas DataFrame
                return pd.DataFrame(rows, columns=colnames)
            else:
                print('Query did not return any rows')
                return pd.DataFrame()
    except Exception as e:
        raise e