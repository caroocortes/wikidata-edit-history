
import traceback
import sys
import time
import json
import psycopg2
import queue
from pathlib import Path

from scripts.const import *
from scripts.utils import insert_rows_copy

def batch_insert(conn, batch, table_suffix=''):

    """Function to insert into DB in parallel."""
    
    try:

        if len(batch['revision']) > 0:
            insert_rows_copy(conn, f'revision{table_suffix}', batch['revision'], REVISION_COLS, REVISION_PK)
        
        if len(batch['value_change']) > 0:
            insert_rows_copy(conn, f'value_change{table_suffix}', batch['value_change'], VALUE_CHANGE_COLS, VALUE_CHANGE_PK)
            
        # if len(change_metadata) > 0:
        #     insert_rows(conn, f'value_change_metadata{table_suffix}', change_metadata, ['revision_id', 'property_id', 'value_id', 'change_target', 'change_metadata', 'value'])
        
        if len(batch['qualifier_change']) > 0:
            insert_rows_copy(conn, f'qualifier_change{table_suffix}', batch['qualifier_change'], QUALIFIER_CHANGE_COLS, QUALIFIER_CHANGE_PK)
        
        if len(batch['reference_change']) > 0:
            insert_rows_copy(conn, f'reference_change{table_suffix}', batch['reference_change'], REFERENCE_CHANGE_COLS, REFERENCE_CHANGE_PK)
        
        if len(batch['datatype_metadata_change']) > 0:
            insert_rows_copy(conn, f'datatype_metadata_change{table_suffix}', batch['datatype_metadata_change'], DATATYPE_METADATA_CHANGE_COLS, DATATYPE_METADATA_CHANGE_PK)

        # if len(datatype_metadata_changes_metadata) > 0:
        #     insert_rows(conn, f'datatype_metadata_change_metadata{table_suffix}', datatype_metadata_changes_metadata, ['revision_id', 'property_id', 'value_id', 'change_target', 'change_metadata', 'value'])

        if table_suffix == '' or table_suffix == '_less':
            if len(batch['features_entity']) > 0:
                insert_rows_copy(conn, f'features_entity{table_suffix}', batch['features_entity'], ENTITY_FEATURE_COLS, ENTITY_FEATURE_PK)
            
            if len(batch['features_text']) > 0:
                insert_rows_copy(conn, f'features_text{table_suffix}', batch['features_text'], TEXT_FEATURE_COLS, TEXT_FEATURE_PK)
            
            if len(batch['features_time']) > 0:
                insert_rows_copy(conn, f'features_time{table_suffix}', batch['features_time'], TIME_FEATURE_COLS, TIME_FEATURE_PK)
            
            if len(batch['features_globecoordinate']) > 0:
                insert_rows_copy(conn, f'features_globecoordinate{table_suffix}', batch['features_globecoordinate'], GLOBE_FEATURE_COLS, GLOBE_FEATURE_PK)
            
            if len(batch['features_quantity']) > 0:
                insert_rows_copy(conn, f'features_quantity{table_suffix}', batch['features_quantity'], QUANTITY_FEATURE_COLS, QUANTITY_FEATURE_PK)
            
            # if len(batch['features_reverted_edit']) > 0:
            #     insert_rows_copy(conn, f'features_reverted_edit{table_suffix}', batch['features_reverted_edit'], REVERTED_EDIT_FEATURE_COLS, REVERTED_EDIT_FEATURE_PK)
            
            if len(batch['features_property_replacement']) > 0:
                insert_rows_copy(conn, f'features_property_replacement{table_suffix}', batch['features_property_replacement'], PROPERTY_REPLACEMENT_FEATURE_COLS, PROPERTY_REPLACEMENT_PK)
        
        if len(batch['entity_property_time_stats']) > 0:
            insert_rows_copy(conn, f'entity_property_time_stats{table_suffix}', batch['entity_property_time_stats'], ENTITY_PROPERTY_TIME_STATS_COLS, ENTITY_PROPERTY_TIME_STATS_PK)

        if len(batch['entity_stats']) > 0:
            insert_rows_copy(conn, f'entity_stats{table_suffix}', batch['entity_stats'], ENTITY_STATS_COLS, ENTITY_STATS_PK)

    except Exception as e:
        print(f'There was an error when batch inserting revisions and changes: {e}', flush=True)
        print(traceback.format_exc(), flush=True)
        raise e


def db_writer(num_workers, results_queue):

    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    pid = os.getpid()
    log_file = open(f'logs/db_writer_{pid}.log', 'w', buffering=1)  # Line buffered
    
    def log(msg):
        """Helper to write to both stdout and log file"""
        log_file.write(f"{msg}\n")
        log_file.flush()

    log(f"[DB_WRITER] Starting - Num workers: {num_workers}")

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

    log(f"[DB_WRITER] Database connection established")

    base_table_names = [
        'revision',
        'value_change',
        'qualifier_change',
        'reference_change',
        'datatype_metadata_change',
        'features_entity',
        'features_text',
        'features_time',
        'features_globecoordinate',
        'features_quantity',
        'features_reverted_edit',
        'features_property_replacement',
        'entity_property_time_stats',
        'entity_stats'
    ]

    batches = {
        '': {table: [] for table in base_table_names},
        '_sa': {table: [] for table in base_table_names if 'features' not in table},
        '_ao': {table: [] for table in base_table_names if 'features' not in table},
        '_less': {table: [] for table in base_table_names}
    }

    workers_finished = 0
    last_write = time.time()

    last_log = time.time()

    try:
        while workers_finished < num_workers:
            try:
                if time.time() - last_log > 30:
                    log(f"[DB_WRITER] Still waiting... {workers_finished}/{num_workers} workers finished")
                    last_log = time.time()

                result = results_queue.get(timeout=5)
                
                if result is None:
                    # Worker finished
                    workers_finished += 1
                    log(f"[DB_WRITER] Worker finished, total finished: {workers_finished}/{num_workers}")
                    continue

                if result['is_scholarly_article']:
                    table_suffix = '_sa'

                if result['is_astronomical_object']:
                    table_suffix = '_ao'
                
                if not result['is_astronomical_object'] and not result['is_scholarly_article'] and result['has_less_revisions']:
                    table_suffix = '_less'
                
                if not result['is_astronomical_object'] and not result['is_scholarly_article'] and not result['has_less_revisions']:
                    table_suffix = ''
                
                for table_name in base_table_names:
                    if table_name in batches[table_suffix]:
                        batches[table_suffix][table_name].extend(result.get(table_name, []))
                
                current_batch_size = len(batches[table_suffix]['revision'])
                time_since_write = time.time() - last_write
                if len(batches[table_suffix]['revision']) >= BATCH_SIZE or (time_since_write > 20 and current_batch_size > 0):
                    
                    batch_insert(conn, batches[table_suffix], table_suffix=table_suffix)
                    # Clear this batch
                    for table in batches[table_suffix]:
                        batches[table_suffix][table] = []

                    last_write = time.time()
                
            except queue.Empty:
                log(f"[DB_WRITER] Queue empty timeout - flushing batches")
                for suffix, batch in batches.items():
                    if any(len(v) > 0 for v in batch.values()):
                        batch_insert(conn, batch, table_suffix=suffix)
                        for table in batch:
                            batch[table] = []
    
        for suffix, batch in batches.items():
            if any(len(v) > 0 for v in batch.values()):
                batch_insert(conn, batch, table_suffix=suffix)
        
        log(f"[DB_WRITER] Completed successfully")

    except Exception as e:
        log(f'Error in DB writer: {e}')
        log(traceback.format_exc())
        raise e
    finally:
        log(f"[DB_WRITER] Closing connection")
        try:
            conn.close()
        except:
            pass
        log(f"[DB_WRITER] Exiting")
        log_file.close()