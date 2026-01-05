import requests
import os
import bz2
from pathlib import Path
import re
from psycopg2.extras import execute_batch
import psycopg2
from dotenv import load_dotenv
from math import radians, cos, sin, asin, sqrt
from bs4 import BeautifulSoup
import json
import hashlib
from urllib.parse import urljoin
from scripts.const import WIKIDATA_SERVICE_URL, DOWNLOAD_LINKS_FILE_PATH
import io
import sys

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

def insert_rows(conn, table_name, rows, columns):
    if not rows:
        return

    col_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))

    query = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
    """

    try:
        with conn.cursor() as cur:
            execute_batch(cur, query, rows)
        conn.commit()
        return 1
    except Exception as e:
        conn.rollback()  # reset the transaction
        bad_rows = []
        for row in rows:
            try:
                with conn.cursor() as cur:
                    cur.execute(query, row)
                conn.commit()
            except Exception as e_row:
                conn.rollback()
                bad_rows.append((row, str(e_row)))

                # Try to parse PK columns from the error
                # Example error: duplicate key value violates unique constraint "entity_pkey"
                # DETAIL: Key (revision_id, entity_id)=(123, 'abc') already exists.
                match = re.search(r"Key \((.*?)\)=\((.*?)\)", str(e_row))
                if match:
                    key_cols = [col.strip() for col in match.group(1).split(',')]
                    key_vals = [val.strip() for val in match.group(2).split(',')]

                    # Build WHERE clause dynamically
                    where_clause = ' AND '.join([f"{col} = %s" for col in key_cols])
                    select_query = f"SELECT * FROM {table_name} WHERE {where_clause}"

                    try:
                        with conn.cursor() as cur:
                            cur.execute(select_query, key_vals)
                            existing = cur.fetchone()
                            if existing:
                                print(f'Rows: {e_row} \n {row}')
                    except Exception as select_err:
                        print(f"Error checking for existing row: {select_err}")


def insert_rows_copy(conn, table_name, rows, columns):
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
        raise
    finally:
        cursor.close()

def create_db_schema():
    base_dir = Path(__file__).resolve().parent.parent
    
    sql_file_path = f"{base_dir}/change_schema.sql"
    print(f"Creating DB schema from {sql_file_path}...")

    with open(sql_file_path, "r", encoding="utf-8") as f:
        query = f.read()
    
    try:

        # dotenv_path = Path(__file__).resolve().parent.parent / ".env"
        # load_dotenv(dotenv_path)

        # DB_USER = os.environ.get("DB_USER")
        # DB_PASS = os.environ.get("DB_PASS")
        # DB_NAME = os.environ.get("DB_NAME")
        # DB_HOST = os.environ.get("DB_HOST")
        # DB_PORT = os.environ.get("DB_PORT")
        SCRIPT_DIR = Path(__file__).parent
        CONFIG_PATH = SCRIPT_DIR.parent / 'db_config.json'
        with open(CONFIG_PATH) as f:
            config = json.load(f)

        conn = psycopg2.connect(
            dbname=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASS"], 
            host=config["DB_HOST"],
            port=config["DB_PORT"]
        )

        cursor = conn.cursor()

        cursor.execute(query=query)

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