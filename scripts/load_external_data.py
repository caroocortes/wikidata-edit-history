import psycopg2
from dotenv import load_dotenv
import os
import csv

from const import PROPERTY_LABELS_PATH, ENTITY_LABEL_ALIAS_PATH, SUBCLASS_OF_PATH, INSTANCE_OF_PATH

def copy_from_csv(conn, csv_file_path, table_name, columns, pk_cols, delimiter):
    with conn.cursor() as cur:
        
        cur.execute("SET synchronous_commit = OFF;")
        cols = ','.join(columns)
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            next(f)  # skip header
            
            cur.copy_expert(f"""
                COPY {table_name} ({cols})
                FROM STDIN
                WITH (FORMAT csv, HEADER FALSE, QUOTE '"', ESCAPE '"', DELIMITER '{delimiter}');
            """, f)

        conn.commit()

        print("All data loaded using COPY.")

        print('Adding primary key...')
        pk_cols_str = ','.join(pk_cols)
        cur.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_cols_str});")
        conn.commit()
        print('Finished copy from csv')

def update_value_change_entity_labels(conn, table_name):
    """
        Updates the column "old_value_label" and "new_value_label" in the "value_change", for values that refer to Q-ids
        Creates a table entity_labels from a csv file (csv_file_path) which contains a list of Q-ids, Labels for all entities in WD.
    """

    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS entity_labels_aliases (id VARCHAR, label VARCHAR, alias VARCHAR);")
        
        cur.execute("SELECT COUNT(*) FROM entity_labels_aliases;")
        count = cur.fetchone()[0]
        
    conn.commit()
    
    if count == 0: # only load if there's no data
        copy_from_csv(conn, ENTITY_LABEL_ALIAS_PATH, 'entity_labels_aliases', ['id', 'label', 'alias'], ['id'], ';')

    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS new_value_label VARCHAR DEFAULT NULL;")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS old_value_label VARCHAR DEFAULT NULL;")
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
                vc.new_value_label IS NULL AND -- only update the ones that don't have a label yet
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
                vc.old_value_label IS NULL AND -- only update the ones that don't have a label yet
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
        cur.execute(f"CREATE TABLE IF NOT EXISTS property_labels (id VARCHAR, label VARCHAR);")
        cur.execute("SELECT COUNT(*) FROM property_labels;")
        count = cur.fetchone()[0]
        
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {property_label_column} VARCHAR DEFAULT NULL;")
    conn.commit()
    
    if count == 0:
        copy_from_csv(conn, PROPERTY_LABELS_PATH, 'property_labels', ['id', 'label'], ['id'], ',')

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
            WHERE vc.{property_label_column} IS NULL AND vc.{property_id_column} = pl.id;

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
        cur.execute(f"CREATE TABLE IF NOT EXISTS entity_type (entity_id VARCHAR, class_id VARCHAR, class_label VARCHAR);")
    conn.commit()
    copy_from_csv(conn, SUBCLASS_OF_PATH, 'entity_type', ['entity_id', 'class_id'], ['entity_id', 'class_id'], ',')
    copy_from_csv(conn, INSTANCE_OF_PATH, 'entity_type', ['entity_id', 'class_id'], None, ',') # set to None so it doesn't create the PK again

    # # Update columns in entity_type table
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_value_change_qid
            ON entity_type (class_id);

            UPDATE entity_type et
            SET  class_label = el.label 
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
    update_value_change_entity_labels(conn, 'value_change_sample_30')

    # Update property label
    # update_property_label(conn, 'value_change_sample_30', 'property_id', 'property_label') 

    # Update entity type
    load_entity_type(conn)
    
    conn.close() 