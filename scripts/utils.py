import requests
import pandas as pd
import os
import csv
import time
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

def human_readable_size(size, decimal_places=2):
    for unit in ['B','KB','MB','GB','TB']:
        if size < 1024:
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024


"""
    Methods for obtaining data for tables entity_type, class, property, from the wikidata query service
"""
def fetch_wikidata_properties():
    """
        Querys Wikidata to obtain properties and their english labels.
        Stores them in a csv
    """
    output_dir = '../data/output_csvs'
    os.makedirs(output_dir, exist_ok=True)

    property_file_path = f'{output_dir}/property.csv'

    if os.path.isfile(property_file_path):
        print(f'File property.csv already exists in {property_file_path}')
    else:
        query = """
        SELECT ?pid ?propertyLabel
        WHERE {
            ?property a wikibase:Property.
            BIND(STRAFTER(STR(?property), "http://www.wikidata.org/entity/") AS ?pid)
            SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
        }
        ORDER BY ASC(xsd:integer(SUBSTR(?pid, 2)))
        """

        url = "https://query.wikidata.org/sparql"
        headers = {"Accept": "application/sparql-results+json"}

        response = requests.get(url, params={'query': query, 'format': 'json'}, headers=headers)

        if response.status_code == 200:
            results = response.json()["results"]["bindings"]
            data = []
            for result in results:
                prop_id = result["pid"]["value"]
                label = result["propertyLabel"]["value"]
                data.append({"Property_ID": prop_id, "Label": label})

            df = pd.DataFrame(data)
            df.to_csv(property_file_path, index=False)
            print(f"Saved {len(df)} properties to '{property_file_path}'")
        else:
            print("Failed to fetch data:", response.status_code)

def fetch_class_label():
    """ 
        Obtains class_id, class_label from wikidata's SPARQL query service

        NOTE: fetch_entity_types needs to be ran before running this method, since it depends on the list of entity_id, class_id pairs
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
    cur.execute("SELECT DISTINCT class_id FROM entity_type")
    class_ids = cur.fetchall()

    print(f"Loaded {len(class_ids)} unique class IDs from entity_type table")
    
    url = "https://query.wikidata.org/sparql"
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataFetcher/1.0 (carolina.cortes@hpi.de)"
    }

    batch_size = 50
    class_list = list(class_ids)
    
    for i in range(0, len(class_list), batch_size):
        classes = []
        batch = class_list[i:i + batch_size]
        values_str = " ".join(f"wd:{cid[0]}" for cid in batch)
        print(f"Fetching classes (Batch: {i} - {i + batch_size})...")

        query = f"""
        SELECT ?class ?classLabel
        WHERE {{
            VALUES ?class {{ {values_str} }}
            ?class rdfs:label ?classLabel.
            FILTER(LANG(?classLabel) = "en")
        }}
        """

        try:
            response = requests.get(url, params={'query': query, 'format': 'json'}, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch classes on batch {i} - {i+ batch_size}: {e}")
            break

        results = response.json()["results"]["bindings"]
        if not results:
            print("No more results.")
            break

        for result in results:
            class_id = result["class"]["value"].split("/")[-1]
            class_label = result["classLabel"]["value"]

            classes.append((class_id, class_label))

        insert_rows(conn, 'class', rows=classes, columns=['class_id', 'class_label'])
        time.sleep(10)
    
    conn.close()

def fetch_entity_types():
    """ 
        Obtains entity_id, class_id, class_label from wikidata's SPARQL query service
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
    cur.execute("SELECT entity_id FROM entity")
    entity_ids = cur.fetchall()

    url = "https://query.wikidata.org/sparql"
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "WikidataFetcher/1.0 (carolina.cortes@hpi.de)"
    }

    batch_size = 50
    entity_list = list(entity_ids)

    for i in range(0, len(entity_list), batch_size):
        entities = []
        batch = entity_list[i:i + batch_size]
        values_str = " ".join(f"wd:{eid[0]}" for eid in batch)
        print(f"Fetching page (Batch size: {i} - {i + batch_size})...")
        
        query = f"""
        SELECT ?entity ?class
        WHERE {{
            VALUES ?entity {{ {values_str} }}
            ?entity wdt:P31 ?class.
          
            SERVICE wikibase:label {{
                bd:serviceParam wikibase:language "en".
            }}
        }}
        """
        print('Query to use:')

        print(query)
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

        for result in results:
            entity_id = result["entity"]["value"].split("/")[-1]
            class_id = result["class"]["value"].split("/")[-1]

            entities.append((entity_id, class_id))

        time.sleep(10) 
        
        #insert in batches
        insert_rows(conn, table_name='entity_type', rows=entities, columns=['entity_id', 'class_id'])

    # close db connection
    conn.close()
    
"""
    Methods for inserting in DB / CSV files
"""

def initialize_csv_files(suffix=None):
    output_dir = Path("data/output_csvs")
    output_dir.mkdir(parents=True, exist_ok=True)

    if suffix:
        files_info = {
            f"entity_{suffix}.csv": ["entity_id", "label"],
            f"{suffix}.jsonl": ["entity_id", "revision_id", "property_id", "value_id", "old_value", "new_value", "datatype", "datatype_metadata", "change_type", "change_magnitude"],
            f"revision_{suffix}.csv": ["entity_id", "revision_id", "timestamp", "user_id", "username", "comment"]
        }
    else:
        files_info = {
            "entity.csv": ["entity_id", "label"],
            "change.csv": ["entity_id", "revision_id", "property_id", "value_id", "old_value", "new_value", "datatype", "datatype_metadata", "change_type", "change_magnitude"],
            "revision.csv": ["entity_id", "revision_id", "timestamp", "user_id", "username", "comment"]
        }

    writers = {}
    paths = {}

    for filename, headers in files_info.items():
        file_path = output_dir / filename
        file_exists = file_path.exists()

        f = open(file_path, mode="a", newline='', encoding="utf-8")
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow(headers)

        writers[filename] = writer
        paths[filename] = str(file_path)

    if suffix:
        return (
            paths[f'entity_{suffix}.csv'],
            paths[f'{suffix}.jsonl'],
            paths[f'revision_{suffix}.csv']
        )

    else:
        return (
            paths['entity.csv'],
            paths['change.csv'],
            paths['revision.csv']
        )


def update_entity_label(conn, entity_id, entity_label):
    """
    Update entity_label in the entity table.
    
    :param conn: psycopg2 connection
    """

    query = """
        UPDATE entity
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

def load_csv_to_db(csv_path, table_name):
    """
        Stores csv in BD
        CSV column names must match the column names in the table
    """
    
    # Load CSV
    df = pd.read_csv(csv_path, dtype=str)
    
    # Prepare data for insertion
    cols = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    
    # Build INSERT statement
    cols_str = ', '.join(cols)
    insert_query = f"INSERT INTO {table_name} ({cols_str}) VALUES %s ON CONFLICT DO NOTHING;"
    
    dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path)

    # Connect and insert
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
    try:
        execute_values(cur, insert_query, values)
        conn.commit()
        print(f"Inserted {len(values)} rows into {table_name}.")
    except Exception as e:
        conn.rollback()
        print("Error inserting data:", e)
    finally:
        cur.close()
        conn.close()

def create_db_schema(conn):

    query = """

        CREATE TABLE Entity (
            Id TEXT PRIMARY KEY,
            Label TEXT
        );

        CREATE TABLE Class (
            Id TEXT PRIMARY KEY,
            Label TEXT
        );

        CREATE TABLE Entity_Types (
            Entity_Id TEXT,
            Class_Id TEXT,
            PRIMARY KEY (Entity_Id, Class_Id),
            FOREIGN KEY (Class_Id) REFERENCES Class(Id),
            FOREIGN KEY (Entity_Id) REFERENCES Entity(Id)
        );

        CREATE TABLE Property (
            Id TEXT PRIMARY KEY,
            Label TEXT
        );

        CREATE TABLE Revision (
            Revision_Id TEXT,
            Entity_Id TEXT,
            Timestamp TIMESTAMP WITH TIME ZONE,
            User_Id TEXT,
            Comment TEXT,
            PRIMARY KEY (Revision_Id, Entity_Id)
            FOREIGN KEY (Entity_Id) REFERENCES Entity(Id)
        );

        CREATE TABLE Change (
            Revision_Id TEXT,
            Property_Id TEXT,
            SubValue_Key TEXT,
            Value_Id TEXT,
            Old_Value JSONB,
            New_Value JSONB,
            Datatype TEXT,
            Datatype_Metadata TEXT,
            Change_Type TEXT,
            PRIMARY KEY (Revision_Id, Property_Id, SubValue_Key, Value_Id, Datatype_Metadata)
            FOREIGN KEY (Revision_Id) REFERENCES Revision(Revision_Id),
        );
    """
    try:
        cursor = conn.cursor()

        cursor.execute(query=query)

        conn.commit()
        cursor.close()

    except Exception as e:
        print(f'Error when saving or connecting to DB: {e}')


def get_dump_links(self):
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
