import requests
import pandas as pd
import os
import csv
import time
from pathlib import Path
import re
from psycopg2.extras import execute_batch

def human_readable_size(size, decimal_places=2):
    for unit in ['B','KB','MB','GB','TB']:
        if size < 1024:
            return f"{size:.{decimal_places}f} {unit}"
        size /= 1024

def initialize_csv_files(suffix=None):
    output_dir = Path("data/output_csvs")
    output_dir.mkdir(parents=True, exist_ok=True)

    if suffix:
        files_info = {
            f"entity_{suffix}.csv": ["Entity_ID", "Label"],
            f"{suffix}.jsonl": ["Entity_ID", "Revision_ID", "Property_ID", "Value_ID", "Old_Value", "New_Value", "Datatype", "Datatype_Metadata", "Change_Type"],
            f"revision_{suffix}.csv": ["Entity_ID", "Revision_ID", "Timestamp", "User", "Comment"]
        }
    else:
        files_info = {
            "entity.csv": ["Entity_ID", "Label"],
            "change.csv": ["Entity_ID", "Revision_ID", "Property_ID", "Value_ID", "Old_Value", "New_Value", "Datatype", "Datatype_Metadata", "Change_Type"],
            "revision.csv": ["Entity_ID", "Revision_ID", "Timestamp", "User", "Comment"]
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
    output_dir = '../data/output_csvs'
    os.makedirs(output_dir, exist_ok=True)

    input_csv_path = '../data/output_csvs/entity_types.csv'
    df = pd.read_csv(input_csv_path, dtype=str)
    class_ids = set(df['class_id'].dropna().unique())
    print(f"Loaded {len(class_ids)} unique class IDs from {input_csv_path}")

    class_file_path = f'{output_dir}/class.csv'
    classes = []
    
    url = "https://query.wikidata.org/sparql"
    headers = {"Accept": "application/sparql-results+json"}

    batch_size = 50
    class_list = list(class_ids)
    
    for i in range(0, len(class_list), batch_size):
        batch = class_list[i:i + batch_size]
        values_str = " ".join(f"wd:{cid}" for cid in batch)
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

            classes.append({"class_id": class_id, "label": class_label})

        time.sleep(10)

    # TODO: change to save to DB
    pd.DataFrame(classes).to_csv(class_file_path, index=False)
    print(f"Saved {len(classes)} class-label pairs to '{class_file_path}'")

def fetch_entity_types():
    """ 
        Obtains entity_id, class_id, class_label from wikidata's SPARQL query service
    """
    output_dir = '../data/output_csvs'
    os.makedirs(output_dir, exist_ok=True)

    input_csv_path = '../data/output_csvs/entity.csv'
    df = pd.read_csv(input_csv_path, dtype=str)
    entity_ids = set(df['Entity_ID'].dropna().unique())
    print(f"Loaded {len(entity_ids)} unique entity IDs from {input_csv_path}")

    entity_file_path = f'{output_dir}/entity_types.csv'
    class_file_path = f'{output_dir}/class.csv'

    entities = []
    classes = {}
    
    url = "https://query.wikidata.org/sparql"
    headers = {"Accept": "application/sparql-results+json"}

    batch_size = 50
    entity_list = list(entity_ids)

    for i in range(0, len(entity_list), batch_size):
        batch = entity_list[i:i + batch_size]
        values_str = " ".join(f"wd:{eid}" for eid in batch)
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

            entities.append({"entity_id": entity_id, "class_id": class_id})

        time.sleep(10) 

    # Save to CSV
    pd.DataFrame(entities).to_csv(entity_file_path, index=False)
    print(f"Saved {len(entities)} entity-class pairs to '{entity_file_path}'")

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
                                print(f"Existing conflicting row for {dict(zip(key_cols, key_vals))}: {existing}")
                    except Exception as select_err:
                        print(f"Error checking for existing row: {select_err}")

        print("\nProblematic rows:")
        for br, err in bad_rows:
            print(f"{br} -> {err}")

        print("\nOriginal batch insert error:")
        print(e)

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
