import requests
import pandas as pd
import os
import csv
import time
import bz2
from pathlib import Path
import re
from psycopg2.extras import execute_batch
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from math import radians, cos, sin, asin, sqrt
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from scripts.const import WIKIDATA_SERVICE_URL, DOWNLOAD_LINKS_FILE_PATH
 

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
    Methods for obtaining class and property, from the wikidata query service
"""
def fetch_wikidata_properties():
    """
        Querys Wikidata to obtain english labels for properties in the change table.
        Stores them in the table change 
    """

    dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path)

    DB_USER = os.environ.get("DB_USER")
    DB_PASS = os.environ.get("DB_PASS")
    DB_NAME = os.environ.get("DB_NAME")
    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = os.environ.get("DB_PORT")

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS, 
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()

    query_get_prop_ids = """
        SELECT DISTINCT property_id 
        FROM change
        WHERE property_label IS NOT NULL 
    """  # only properties without label yet
    cur.execute(query_get_prop_ids)
    property_ids = list(cur.fetchall())

    query = """
    SELECT ?pid ?propertyLabel
    WHERE {
        ?property a wikibase:Property.
        BIND(STRAFTER(STR(?property), "http://www.wikidata.org/entity/") AS ?pid)
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    ORDER BY ASC(xsd:integer(SUBSTR(?pid, 2)))
    """

    batch_size = 50
    
    url = "https://query.wikidata.org/sparql"
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataFetcher/1.0 (carolina.cortes@hpi.de)"
    }

    for i in range(0, len(property_ids), batch_size):
        batch = property_ids[i:i + batch_size]
        values_str = " ".join(f"wd:{cid[0]}" for cid in batch)

        query = f"""
        SELECT ?property ?propertyLabel
        WHERE {{
            VALUES ?property {{ {values_str} }}
            ?class rdfs:label ?propertyLabel.
            FILTER(LANG(?propertyLabel) = "en")
        }}
        """

        try:
            response = requests.get(url, params={'query': query, 'format': 'json'}, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch properties on batch {i} - {i+ batch_size}: {e}")
            break

        results = response.json()["results"]["bindings"]
        if not results:
            print("No more results.")
            break
        
        query = """
            UPDATE change
            SET property_label = %s
            WHERE property_id = %s
        """
        
        properties = []
        for result in results:
            property_label = result["propertyLabel"]["value"]
            property_id = result["property"]["value"].split("/")[-1]
            properties.append((property_label, property_id))  # order matches %s

        cur.executemany(query, properties)
        conn.commit()
        
        time.sleep(10)
    
    conn.close()


def fetch_entity_types():
    """ 
        Obtains class_id, class_label from wikidata's SPARQL query service 
    """

    dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path)

    DB_USER = os.environ.get("DB_USER")
    DB_PASS = os.environ.get("DB_PASS")
    DB_NAME = os.environ.get("DB_NAME")
    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = os.environ.get("DB_PORT")

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS, 
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()
    # entities that haven't been added to the entity_types table yet (without a class)
    cur.execute("""
        SELECT DISTINCT r.entity_id 
        FROM revision r LEFT JOIN entity_type et ON r.entity_id = et.entity_id
        WHERE et.entity_id IS NULL
    """) 
    entity_ids = cur.fetchall()

    url = "https://query.wikidata.org/sparql"
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataFetcher/1.0 (carolina.cortes@hpi.de)"
    }

    batch_size = 50
    entity_list = list(entity_ids)

    for i in range(0, len(entity_list), batch_size):
        batch = entity_list[i:i + batch_size]
        values_str = " ".join(f"wd:{eid[0]}" for eid in batch) # add base uri (wd:) to each entity id
        
        query = f"""
        SELECT ?entity ?class ?classLabel ?rank
        WHERE {{
            VALUES ?entity {{ {values_str} }}
            ?entity p:P31 ?statement.
            ?statement ps:P31 ?class.
            ?statement wikibase:rank ?rank.
          
            SERVICE wikibase:label {{
                bd:serviceParam wikibase:language "en".
            }}
        }}
        """

        try:
            response = requests.get(url, params={'query': query, 'format': 'json'}, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data on page {i} - {i + batch_size}: {e}")
            break

        results = response.json()["results"]["bindings"]
        if not results:
            print("No more results.")
            break
        
        entity_types_data = []
        class_data = []

        for result in results:
            entity_id = result["entity"]["value"].split("/")[-1]
            class_id = result["class"]["value"].split("/")[-1]
            class_label = result["classLabel"]["value"]

            rank_wd = result["rank"]["value"].split("/")[-1]
            rank = "normal"  # default
            if rank_wd == "PreferredRank":
                rank = "preferred"
            elif rank_wd == "DeprecatedRank":
                rank = "deprecated"

            entity_types_data.append((entity_id, class_id))
            class_data.append((class_id, class_label, rank))

        query_class = """
            INSERT INTO class (class_id, class_label, rank)
            VALUES (%s, %s, %s)
            ON CONFLICT (class_id) DO NOTHING
        """
        cur.executemany(query_class, class_data)

        query_entity_types = """
            INSERT INTO entity_type (entity_id, class_id)
            VALUES (%s, %s)
            ON CONFLICT (entity_id, class_id) DO NOTHING
        """
        cur.executemany(query_entity_types, entity_types_data)

        conn.commit()

        time.sleep(10) 
        

    # close db connection
    conn.close()
    
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
    
    if table_name == 'entity':
        # check if entity exists
        query_select = "SELECT entity_id FROM entity WHERE entity_id = %s"

        with conn.cursor() as cur:
            cur.execute(query_select, (rows[0][0],))
            exists = cur.fetchone() is not None
            if exists:
                # will skip this entity
                return 0

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
                                print(f"Existing conflicting row for {dict(zip(key_cols, key_vals))} in table {table_name}: {existing}")
                                print(f'Row that retunrs error: {e_row}')
                    except Exception as select_err:
                        print(f"Error checking for existing row: {select_err}")

        print("\nProblematic rows:")
        for br, err in bad_rows:
            print(f"{br} -> {err}")

        print("\nOriginal batch insert error:")
        print(e)

def create_db_schema():
    base_dir = Path(__file__).resolve().parent.parent
    
    sql_file_path = f"{base_dir}/change_schema.sql"
    print(f"Creating DB schema from {sql_file_path}...")

    with open(sql_file_path, "r", encoding="utf-8") as f:
        query = f.read()
    
    try:

        dotenv_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(dotenv_path)

        DB_USER = os.environ.get("DB_USER")
        DB_PASS = os.environ.get("DB_PASS")
        DB_NAME = os.environ.get("DB_NAME")
        DB_HOST = os.environ.get("DB_HOST")
        DB_PORT = os.environ.get("DB_PORT")

        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS, 
            host=DB_HOST,
            port=DB_PORT
        )

        cursor = conn.cursor()

        cursor.execute(query=query)

        conn.commit()
        cursor.close()

    except Exception as e:
        print(f'Error when saving or connecting to DB: {e}')


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

