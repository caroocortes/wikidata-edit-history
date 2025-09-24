import multiprocessing as mp
import requests
import os
import psycopg2
import time
from dotenv import load_dotenv
from pathlib import Path
from scripts.utils import id_to_int
import sys


dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path)

DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

url = "https://query.wikidata.org/sparql"
headers = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "WikidataFetcher/1.0 (carolina.cortes@hpi.de)"
}

"""
    Methods for obtaining class and property, from the wikidata query service
"""
def fetch_wikidata_properties():
    """
        Querys Wikidata to obtain english labels for properties in the change table.
        Stores them in the table change 
    """

    print("[fetch_wikidata_properties] Started")

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
        WHERE property_label IS NULL 
    """  # only properties without label yet
    cur.execute(query_get_prop_ids)
    property_ids = list(cur.fetchall())

    print(f"Found {len(property_ids)} properties without label")

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
    
    # for printing progress
    last_print = 0
    interval = 500 # seconds

    count = 0

    for i in range(0, len(property_ids), batch_size):
        batch = property_ids[i:i + batch_size]
        values_str = " ".join(f"wd:P{pid[0]}" for pid in batch if pid[0] not in (-1, -2))

        query = f"""
        SELECT ?property ?propertyLabel
        WHERE {{
            VALUES ?property {{ {values_str} }}
            ?property rdfs:label ?propertyLabel.
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

        print(f'Property - Resutls lenght: {len(results)}')

        if len(results) > 0:

            count += batch_size
            print('Properties - There are results !!')
            query = """
                UPDATE change
                SET property_label = %s
                WHERE property_id = %s
            """
            
            properties = []
            for result in results:
                property_label = result["propertyLabel"]["value"]
                property_id = id_to_int(result["property"]["value"].split("/")[-1]) # remove the P
                properties.append((property_label, property_id))  # order matches %s

            try:
                cur.executemany(query, properties)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f'Error when saving properties to DB: {e}')     
        else:
            print(f"Properties - No results for batch {i} - {i + batch_size}")
            print(values_str)

        now = time.time()
        if now - last_print >= interval:
            print(f"Properties - Progress at iteration {i} - Fetched {count} properties so far.")
            last_print = now

        sys.stdout.flush()

        time.sleep(10)
    
    conn.close()


def fetch_entity_types():
    """ 
        Obtains class_id, class_label, rank from wikidata's SPARQL query service and inserts into the DB (entity_type and class tables).
    """

    print("[fetch_entity_types] Started")

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

    print(f"Found {len(entity_ids)} without class")

    batch_size = 50

    # for printing progress
    last_print = 0
    interval = 500 # seconds

    entity_list = list(entity_ids)

    count = 0

    for i in range(0, len(entity_list), batch_size):
        batch = entity_list[i:i + batch_size]
        values_str = " ".join(f"wd:Q{eid[0]}" for eid in batch) # add base uri (wd:) to each entity id
        
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

        print(f'Entity types - Resutls lenght: {len(results)}')

        if len(results) > 0:
            count += batch_size
            entity_types_data = []
            class_data = []

            for result in results:
                entity_id = id_to_int(result["entity"]["value"].split("/")[-1])
                class_id = id_to_int(result["class"]["value"].split("/")[-1])
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

            try:
                cur.executemany(query_entity_types, entity_types_data)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f'Error when saving entity types to DB: {e}')

        else:
            print(f"Entity types - No results for batch {i} - {i + batch_size}")
            print(values_str)

        time.sleep(10) 

        now = time.time()
        if now - last_print >= interval:
            print(f"Entity types - Progress at iteration {i} - Fetched {count} entities so far.")
            last_print = now

        sys.stdout.flush()

    # close db connection
    conn.close()

def fetch_wikidata_entity_labels():
    """
        Querys Wikidata to obtain english labels for entities in the change table.
        Stores them in the table change 
    """

    print("[fetch_wikidata_entity_labels] Started")

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS, 
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()

    # # First update with existing ewntity labels in the revision table
    # query_get_ent_ids_revision = """
    #     ALTER TABLE change
    #     ADD COLUMN IF NOT EXISTS new_value_label VARCHAR DEFAULT '',
    #     ADD COLUMN IF NOT EXISTS old_value_label VARCHAR DEFAULT '';

    #     -- Update old_value_label
    #     UPDATE change c
    #     SET old_value_label = r.entity_label
    #     FROM revision r
    #     WHERE 
    #     old_value IS NOT NULL AND 
    #     old_value->>0 LIKE 'Q%' AND
    #     c.datatype = 'wikibase-entityid' AND 
    #     CAST(substring(c.old_value->>0 FROM 2) AS bigint) = r.entity_id;

    #     -- Update new_value_label
    #     UPDATE change c
    #     SET new_value_label = r.entity_label
    #     FROM revision r
    #     WHERE 
    #         new_value IS NOT NULL AND 
    #         new_value->>0 LIKE 'Q%' AND
    #         c.datatype = 'wikibase-entityid' AND
    #         CAST(substring(c.new_value->>0 FROM 2) AS bigint) = r.entity_id;
    # """
    # cur.execute(query_get_ent_ids_revision)

    # for the remaining entities, fetch from wikidata
    query_get_entity_values = """
        SELECT DISTINCT old_value, new_value
        FROM change
        WHERE datatype IN (
            'wikibase-item', 'wikibase-property', 'wikibase-entityid', 'wikibase-lexeme', 
            'wikibase-sense', 'wikibase-form', 'entity-schema'
        )
        AND (old_value_label = '' OR new_value_label = '')
    
    """ 
    cur.execute(query_get_entity_values)
    rows = cur.fetchall()

    entity_ids = set()
    for old_val, new_val in rows:
        if old_val:
            entity_ids.add(old_val)
        if new_val:
            entity_ids.add(new_val)

    entity_ids = list(entity_ids)
    batch_size = 50
    count = 0
    last_print = 0
    interval = 500  # seconds

    for i in range(0, len(entity_ids), batch_size):
        batch = entity_ids[i:i + batch_size]
        values_str = " ".join(f"wd:{eid}" for eid in batch if eid and not eid.startswith("-"))

        query = f"""
            SELECT ?entity ?entityLabel
            WHERE {{
                VALUES ?entity {{ {values_str} }}
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
            }}
        """

        try:
            response = requests.get(url, params={'query': query, 'format': 'json'}, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch entities on batch {i}-{i+batch_size}: {e}")
            break

        results = response.json()["results"]["bindings"]

        entities = []
        for res in results:
            eid = res["entity"]["value"].split("/")[-1]
            label = res["entityLabel"]["value"]
            entities.append((label, eid))

        if entities:
            try:
                # Update both old_value_label and new_value_label
                for label, eid in entities:
                    cur.execute("""
                        UPDATE change
                        SET old_value_label = CASE WHEN old_value->>0 = %s THEN %s ELSE old_value_label END,
                            new_value_label = CASE WHEN new_value->>0 = %s THEN %s ELSE new_value_label END
                        WHERE old_value->>0 = %s OR new_value->>0 = %s
                    """, (eid, label, eid, label, eid, eid))
                conn.commit()
                count += len(entities)
            except Exception as e:
                conn.rollback()
                print(f"Error updating batch {i}-{i+batch_size}: {e}")

        now = time.time()
        if now - last_print >= interval:
            print(f"Progress at batch {i}: {count} labels updated so far.")
            last_print = now

        sys.stdout.flush()
        time.sleep(10)  # avoid overloading S

        time.sleep(10)
    
    conn.close()


# # All classes and subclasses
# SELECT ?class ?classLabel ?superclass ?superclassLabel
# WHERE {
#   ?class wdt:P279 ?superclass.      # subclass of
#   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
# }
# LIMIT 70


# # All properties and subproperties
# SELECT ?property ?propertyLabel ?superproperty ?superpropertyLabel
# WHERE {
#   ?property wdt:P1647 ?superproperty.  # subproperty of
#   SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
# }
# LIMIT 70

if __name__ == "__main__":
    p1 = mp.Process(target=fetch_entity_types)
    p2 = mp.Process(target=fetch_wikidata_properties)

    p1.start()
    p2.start()

    p1.join()
    p2.join()