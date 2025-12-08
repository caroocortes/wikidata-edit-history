import psycopg2
from dotenv import load_dotenv
import os
import csv
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


def copy_from_csv(conn, csv_file_path, table_name, columns, primary_keys, delimiter=';'):
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
            for row in reader:
                writer.writerow(row)
        
        print("CSV preprocessing complete. Loading into database...", flush=True)
        

        with conn.cursor() as cur:
            cols_definition = ', '.join([f"{col} VARCHAR" for col in columns])
            cur.execute(f"CREATE TEMP TABLE {temp_table} ({cols_definition});")
            
            cols = ','.join(columns)
            with open(temp_csv_path, 'r', encoding='utf-8') as f:
                next(f)  # skip header
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

    #Add update fro redirected entities
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
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_value_change_qid
            ON {table_name} ((new_value->>0))
            WHERE datatype IN ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 
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
                vc.datatype IN ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 
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
                vc.old_value->>0 LIKE 'Q%' AND
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
        copy_from_csv(conn, PROPERTY_LABELS_PATH, 'property_labels', ['id', 'label'], ['id'], ';')

    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_value_change_property_id
                ON {table_name}({property_id_column});

            CREATE INDEX IF NOT EXISTS idx_property_labels_id
                ON property_labels(id);

            CREATE INDEX IF NOT EXISTS idx_value_change_property_label_null
                ON {table_name}({property_id_column})
                WHERE {property_label_column} IS NULL;

            UPDATE {table_name} vc
            SET property_label = pl.label
            FROM property_labels pl
            WHERE (vc.{property_label_column} IS NULL or vc.{property_label_column} = '') AND 'P' || vc.{property_id_column}::VARCHAR = pl.id;

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

    if not exists_p279:
        copy_from_csv(conn, SUBCLASS_OF_PATH, 'entity_type_p279', ['entity_id', 'class_id', 'rank'], ['entity_id', 'class_id'], ';')

    if not exists_p31:
        copy_from_csv(conn, INSTANCE_OF_PATH, 'entity_type_p31', ['entity_id', 'class_id', 'rank'], None, ';') # set to None so it doesn't create the PK again

    with conn.cursor() as cur:

        cur.execute("""
            ALTER TABLE entity_type_p279
                    ADD COLUMN IF NOT EXISTS class_label VARCHAR DEFAULT NULL;
            ALTER TABLE entity_type_p31
                    ADD COLUMN IF NOT EXISTS class_label VARCHAR DEFAULT NULL; 
        """)
        
    conn.commit()

    # # Update columns in entity_type table
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_value_change_qid
            ON entity_type_p279 (class_id);

            UPDATE entity_type_p279 et
            SET  class_label = 
                    CASE 
                        WHEN (el.label IS NOT NULL and el.label <> '') THEN el.label
                        ELSE el.alias 
                    END
            FROM entity_labels_aliases el
            WHERE
                et.class_label IS NULL  -- only update the ones that don't have a label yet
                AND
                et.class_id = el.id;
                    
            CREATE INDEX IF NOT EXISTS idx_value_change_qid
            ON entity_type_p31 (class_id);

            UPDATE entity_type_p31 et
            SET  class_label = 
                    CASE 
                        WHEN (el.label IS NOT NULL and el.label <> '') THEN el.label
                        ELSE el.alias 
                    END
            FROM entity_labels_aliases el
            WHERE
                et.class_label IS NULL  -- only update the ones that don't have a label yet
                AND
                et.class_id = el.id;
        """)
    conn.commit()


if "__main__":

    """
        Loads labels for new value and old value, when the value is a Q-id.
        Loads property labels.
        Loads entity_type data
    """

    dotenv_path = ".env"
    load_dotenv(dotenv_path)

    # credentials for DB connection
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
    
    # Update new_value_label + old_value_label
    update_entity_labels(conn, 'value_change')
    update_entity_labels(conn, 'reference_change')
    update_entity_labels(conn, 'qualifier_change')

    # Update property label
    update_property_label(conn, 'value_change', 'property_id', 'property_label') 
    update_property_label(conn, 'reference_change', 'property_id', 'property_label') 
    update_property_label(conn, 'qualifier_change', 'property_id', 'property_label') 

    # Update qualifier property label + reference property label
    update_property_label(conn, 'reference_change', 'ref_property_id', 'ref_property_label') 
    update_property_label(conn, 'qualifier_change', 'qual_property_id', 'qual_property_label') 

    # Load entity type
    load_entity_type(conn)
    
    conn.close() 