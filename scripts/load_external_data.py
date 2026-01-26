import psycopg2
from dotenv import load_dotenv
import os
import csv
import json
from pathlib import Path
import tempfile

from const import PROPERTY_LABELS_PATH, ENTITY_LABEL_ALIAS_PATH, SUBCLASS_OF_PATH, INSTANCE_OF_PATH

import csv

def preprocess_csv(input_file, output_file, delimiter=';'):
    """
    Read CSV with mixed quotes and write with standardized double quotes
    """
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        # Read with Python's csv module (handles mixed quotes)
        reader = csv.reader(infile, delimiter=delimiter)
        
        # Write with standard double quotes
        writer = csv.writer(outfile, delimiter=delimiter, quotechar='"', 
                          quoting=csv.QUOTE_MINIMAL)
        
        for row in reader:
            writer.writerow(row)
    
    print(f"Preprocessed CSV saved to {output_file}")


def copy_from_csv_with_std(conn, csv_file_path, table_name, columns, primary_keys, delimiter=';'):
    temp_table = f"{table_name}_temp"

    print("Preprocessing CSV file (standardize quotes '\")", flush=True)
    temp_csv_fd, temp_csv_path = tempfile.mkstemp(suffix='.csv', text=True)
    
    try:
        # Preprocess: standardize the CSV format
        with open(csv_file_path, 'r', encoding='utf-8') as infile, \
             os.fdopen(temp_csv_fd, 'w', encoding='utf-8', newline='') as outfile:
            
            # Read with Python's intelligent CSV parser
            reader = csv.reader(infile, delimiter=delimiter)
            
            # Write with proper escaping - QUOTE_ALL ensures all fields are quoted
            # and internal quotes are properly escaped as ""
            writer = csv.writer(outfile, delimiter=delimiter, 
                              quotechar='"', 
                              quoting=csv.QUOTE_ALL,  # Quote everything
                              doublequote=True)  # Escape " as ""
            
            next(reader)  # Skip header in input
            skipped = 0
            for row in reader:

                if row and row[0] and row[0].strip():
                    writer.writerow(row)
                else:
                    skipped += 1

                # writer.writerow(row)
            print('Skipped rows with empty first column:', skipped, flush=True)

        print("CSV preprocessing complete. Loading into database...", flush=True)
        
        with conn.cursor() as cur:
            cols_definition = ', '.join([f"{col} VARCHAR" for col in columns])
            cur.execute(f"CREATE TEMP TABLE {temp_table} ({cols_definition});")
            
            cols = ','.join(columns)
            with open(temp_csv_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(f"""
                    COPY {temp_table} ({cols})
                    FROM STDIN
                    WITH (FORMAT csv, HEADER FALSE, DELIMITER '{delimiter}', QUOTE '"');
                """, f)

            
            print(f"Loaded data into temp table. Removing duplicates...", flush=True)
            
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT DISTINCT * FROM {temp_table};")

            # add PK
            if primary_keys:
                
                pk_cols_str = ', '.join(primary_keys)
                # remove duplicates based on primary key columns
                print("Removing duplicates...")
                cur.execute(f"""
                    DELETE FROM {table_name} a
                    USING {table_name} b
                    WHERE a.ctid < b.ctid
                    AND {' AND '.join([f'a.{col} = b.{col}' for col in primary_keys])};
                """)

                print("Adding PK")
                cur.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_cols_str});")
        
        conn.commit()
    
    finally:
        if os.path.exists(temp_csv_path):
            os.unlink(temp_csv_path)


def copy_from_csv(conn, csv_file_path, table_name, columns, primary_keys, delimiter=';'):
    temp_table = f"{table_name}_temp"

    print("Loading CSV file into database...", flush=True)
    
    with conn.cursor() as cur:
        # Create temp table
        cols_definition = ', '.join([f"{col} VARCHAR" for col in columns])
        cur.execute(f"CREATE TEMP TABLE {temp_table} ({cols_definition});")
        
        # Load CSV directly into temp table
        cols = ','.join(columns)
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            cur.copy_expert(f"""
                COPY {temp_table} ({cols})
                FROM STDIN
                WITH (FORMAT csv, HEADER TRUE, DELIMITER '{delimiter}', QUOTE '"');
            """, f)
        
        print(f"Loaded data into temp table. Creating final table and removing duplicates...", flush=True)
        
        # Create final table with distinct rows
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT DISTINCT * FROM {temp_table};")

        # Add primary key
        if primary_keys:
            pk_cols_str = ', '.join(primary_keys)
            
            # Remove duplicates based on primary key columns
            print("Removing duplicates based on primary key...", flush=True)
            cur.execute(f"""
                DELETE FROM {table_name} a
                USING {table_name} b
                WHERE a.ctid < b.ctid
                AND {' AND '.join([f'a.{col} = b.{col}' for col in primary_keys])};
            """)

            print("Adding primary key...", flush=True)
            cur.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_cols_str});")
    
    conn.commit()
    print("CSV load complete!", flush=True)


def update_entity_labels(conn, table_name):
    """
        Updates the column "old_value_label" and "new_value_label" in the "value_change", for values that refer to Q-ids
        Creates a table entity_labels from a csv file (csv_file_path) which contains a list of Q-ids, Labels for all entities in WD.
    """

    with conn.cursor() as cur:

        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'entity_labels_aliases'
            );
        """)
        exists = cur.fetchone()[0]
        
    conn.commit()
    
    if not exists: # only load if there's no data
        copy_from_csv(conn, ENTITY_LABEL_ALIAS_PATH, 'entity_labels_aliases', ['id', 'label', 'alias'], ['id'], ';')

    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS new_value_label VARCHAR DEFAULT NULL;")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS old_value_label VARCHAR DEFAULT NULL;")
    conn.commit()

    # labels for redirected entities (the label is the same as the label of the entity it was redirected to)
    print("Updating labels for redirected entities...", flush=True)
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO entity_labels_aliases (id, label, alias)
            SELECT DISTINCT
                r.entity_id as entity_id,  -- redirected Q-id
                ela.label,
                ela.alias
            FROM revision r
            -- target Q-id (second Q-id from comment)
            CROSS JOIN LATERAL (
                SELECT REGEXP_REPLACE(SPLIT_PART(r.comment, '|', 4), '[^Q0-9]', '', 'g') as target_qid
            ) extracted
            -- label + alias from target entity
            JOIN entity_labels_aliases ela 
                ON ela.id = extracted.target_qid
            WHERE r.redirect = TRUE
            -- for duplicates
            ON CONFLICT (id) DO NOTHING;  
        """)
    conn.commit()

    # # Update columns in change table
    print(f"Updating entity labels in {table_name}...", flush=True)
    with conn.cursor() as cur:

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_new_value_qid
            ON {table_name} ((new_value->>0))
            WHERE new_datatype IN ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 
                            'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema');

            CREATE INDEX IF NOT EXISTS idx_old_value_qid
            ON {table_name} ((old_value->>0))
            WHERE old_datatype IN ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 
                            'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema');

            UPDATE {table_name} vc
            SET  
            --  if the label is empty, use the alias
                new_value_label =
                    CASE 
                        WHEN el.label IS NOT NULL and el.label <> '' THEN el.label 
                        ELSE el.alias 
                    END
            FROM entity_labels_aliases el
            WHERE
                (vc.new_value_label IS NULL or vc.new_value_label = '') AND -- only update the ones that don't have a label yet
                vc.new_datatype IN ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 
                                    'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema')
                AND
                vc.new_value->>0 = el.id;

            UPDATE {table_name} vc
            SET 
            --  if the label is empty, use the alias
                old_value_label =
                    CASE 
                        WHEN el.label IS NOT NULL and el.label <> '' THEN el.label
                        ELSE el.alias 
                    END
            FROM entity_labels_aliases el
            WHERE 
                (vc.old_value_label IS NULL or vc.old_value_label = '') AND -- only update the ones that don't have a label yet
                vc.old_datatype IN ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 
                                    'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema')
                AND
                vc.old_value->>0 = el.id;
        """)
    conn.commit()

def update_property_label(conn, table_name, property_id_column, property_label_column):
    """
        Updates the column "property_label_column" in the "table_name", where the column for property_id is "property_id" 
        Creates a table property_labels from a csv file (csv_file_path) which contains a list of P-ids, Labels for all properties in WD.
        Example: 
            For value_change
                - table_name = 'value_change'
                - property_id_column = 'property_id'
                - property_label_column = 'property_label'
    """

    with conn.cursor() as cur:
        
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'property_labels'
            );
        """)
        exists = cur.fetchone()[0]
        
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {property_label_column} VARCHAR DEFAULT NULL;")
    conn.commit()
    
    if not exists:
        SCRIPT_DIR = Path(__file__).parent
        copy_from_csv(conn, SCRIPT_DIR.parent / PROPERTY_LABELS_PATH, 'property_labels', ['property_id', 'property_label'], ['property_id'], ',')

    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_value_change_property_id
                ON {table_name}({property_id_column});

            CREATE INDEX IF NOT EXISTS idx_property_labels_id
                ON property_labels(property_id);

            CREATE INDEX IF NOT EXISTS idx_value_change_property_label_null
                ON {table_name}({property_id_column})
                WHERE {property_label_column} IS NULL;

            UPDATE {table_name} vc
            SET property_label = pl.property_label
            FROM property_labels pl
            WHERE (vc.{property_label_column} IS NULL or vc.{property_label_column} = '') AND vc.{property_id_column}::integer = pl.property_id::integer;

            UPDATE {table_name} vc
            SET property_label = 'label'
            WHERE vc.{property_id_column} = -1 AND vc.{property_label_column} IS NULL;

            UPDATE {table_name} vc
            SET property_label = 'description'
            WHERE vc.{property_id_column} = -2 AND vc.{property_label_column} IS NULL;

        """)
    conn.commit()

def load_entity_type(conn):
    """
        Creates table entity_type from csv file which containes the columns 'entity_id', 'class_id', 'class_label'
    """
    with conn.cursor() as cur:

        cur.execute("""
            SELECT 
                EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'entity_type_p279'
                ) as exists_p279,
                EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'entity_type_p31'
                ) as exists_p31;
        """)
        result = cur.fetchone()
        exists_p279 = result[0]
        exists_p31 = result[1]
        
    conn.commit()

    SCRIPT_DIR = Path(__file__).parent

    if not exists_p279:
        subclass_path = SCRIPT_DIR.parent / SUBCLASS_OF_PATH
        copy_from_csv(conn, subclass_path, 'entity_type_p279', ['entity', 'entity_type', 'type_label', 'type_alias'], ['entity', 'entity_type'], ',')

    if not exists_p31:
        instance_path = SCRIPT_DIR.parent / INSTANCE_OF_PATH
        copy_from_csv(conn, instance_path, 'entity_type_p31', ['entity','entity_type','label_type','alias'], ['entity', 'entity_type'], ',') 


def add_inexes_extra_tables(conn):

    with conn.cursor() as cur:
        cur.execute(f"""
            ALTER TABLE p279_entity_types
            ALTER COLUMN entity_numeric_id TYPE INT using entity_numeric_id::INT;

            ALTER TABLE p31_entity_types
            ALTER COLUMN entity_numeric_id TYPE INT using entity_numeric_id::INT;

            ALTER TABLE p31_entity_types
            ALTER COLUMN entity_type_numeric_id TYPE INT using entity_type_numeric_id::INT;

            ALTER TABLE p279_entity_types
            ALTER COLUMN entity_type_numeric_id TYPE INT using entity_type_numeric_id::INT;

            ALTER TABLE entity_labels_alias_description
            ALTER COLUMN numeric_id TYPE INT using numeric_id::INT;

            CREATE INDEX entity_label_alias_desc_numeric_id
            ON entity_labels_alias_description (numeric_id);

            CREATE INDEX p31_entity_types_numeric_id
            ON p31_entity_types (entity_numeric_id);

            CREATE INDEX p279_entity_types_numeric_id
            ON p279_entity_types (entity_numeric_id);

        """)
    conn.commit()

if "__main__":

    """
        Loads labels for new value and old value, when the value is a Q-id.
        Loads property labels.
        Loads entity_type data
    """

    SCRIPT_DIR = Path(__file__).parent
    CONFIG_PATH = SCRIPT_DIR.parent / 'db_config.json'
    with open(CONFIG_PATH) as f:
        db_config = json.load(f)

    conn = psycopg2.connect(
        dbname=db_config["DB_NAME"],
        user=db_config["DB_USER"],
        password=db_config["DB_PASS"],
        host=db_config["DB_HOST"],
        port=db_config["DB_PORT"],
        connect_timeout=30,
        gssencmode='disable'
    )
    
    # Update new_value_label + old_value_label
    # update_entity_labels(conn, 'value_change')
    # update_entity_labels(conn, 'reference_change')
    # update_entity_labels(conn, 'qualifier_change')

    # Update property label
    # update_property_label(conn, 'gs_quantity', 'property_id', 'property_label') 
    # update_property_label(conn, 'gs_globecoord', 'property_id', 'property_label')
    # update_property_label(conn, 'gs_globe_reformat', 'property_id', 'property_label') 
    # update_property_label(conn, 'gs_text', 'property_id', 'property_label') 

    # update_property_label(conn, 'value_change', 'property_id', 'property_label') 
    # update_property_label(conn, 'reference_change', 'property_id', 'property_label') 
    # update_property_label(conn, 'qualifier_change', 'property_id', 'property_label') 

    # Update qualifier property label + reference property label
    # update_property_label(conn, 'reference_change', 'ref_property_id', 'ref_property_label') 
    # update_property_label(conn, 'qualifier_change', 'qual_property_id', 'qual_property_label') 

    # Load entity type
    # load_entity_type(conn)
    copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/property_labels_with_deleted.csv', 'property_labels', ['property_id', 'property_label'], ['property_id'], ',')
    copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/entity_labels_alias_description.csv', 'entity_labels_alias_description', ['qid','numeric_id', 'label', 'alias', 'description'], ['numeric_id'], ',')
    copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/p31_entity_types.csv', 'p31_entity_types', ['entity','entity_numeric_id','entity_type','entity_type_numeric_id','label_type','alias','type_qids_list','type_numeric_ids_list','type_labels_list'], ['entity_numeric_id', 'entity_type_numeric_id'], ',')
    copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/p279_entity_types.csv', 'p279_entity_types', ['entity','entity_numeric_id','entity_type','entity_type_numeric_id','label_type','alias','type_qids_list','type_numeric_ids_list','type_labels_list'], ['entity_numeric_id', 'entity_type_numeric_id'], ',')
    
    # copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/transitive_closures/has_parts_transitive.csv', 'has_parts_transitive', ['entity_id', 'entity_id_numeric', 'transitive_closure_qids', 'transitive_closure_numeric_ids'], ['entity_id'], ',')
    # copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/transitive_closures/is_metaclass_for_transitive.csv', 'is_metaclass_for_transitive', ['entity_id', 'entity_id_numeric', 'transitive_closure_qids', 'transitive_closure_numeric_ids'], ['entity_id'], ',')
    # copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/transitive_closures/located_in_transitive.csv', 'located_in_transitive', ['entity_id', 'entity_id_numeric', 'transitive_closure_qids', 'transitive_closure_numeric_ids'], ['entity_id'], ',')
    # copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/transitive_closures/part_of_transitive.csv', 'part_of_transitive', ['entity_id', 'entity_id_numeric', 'transitive_closure_qids', 'transitive_closure_numeric_ids'], ['entity_id'], ',')
    # copy_from_csv(conn, '/sc/home/carolina.cortes/wikidata-edit-history/data/transitive_closures/subclass_of_transitive.csv', 'subclass_of_transitive', ['entity_id', 'entity_id_numeric', 'transitive_closure_qids', 'transitive_closure_numeric_ids'], ['entity_id'], ',')

    conn.close() 

    # import pandas as pd
    # df = pd.read_csv('/sc/home/carolina.cortes/wikidata-edit-history/data/p31_entity_types.csv', delimiter=',')

    # df_filt = df[df['entity'] == ''].value_counts()
    # print(df_filt)
