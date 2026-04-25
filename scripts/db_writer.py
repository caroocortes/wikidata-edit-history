
import traceback
import time
import json
import psycopg2
import queue
from pathlib import Path
import gc

from scripts.const import *
from scripts.utils import insert_rows_copy

def batch_insert(conn, batch, set_up, table_suffix=''):
    """Function to insert into DB in parallel."""

    change_extraction_filters = set_up.get('change_extraction_filters', {})
    if table_suffix == '_ao':
        extract_features = change_extraction_filters.get('astronomical_objects_filter', {}).get('feature_extraction', False) and change_extraction_filters.get('astronomical_objects_filter', {}).get('extract', False)
        extract_datatype_metadata_changes = change_extraction_filters.get('astronomical_objects_filter', {}).get('datatype_metadata_extraction', False) and change_extraction_filters.get('astronomical_objects_filter', {}).get('extract', False)

    if table_suffix == '_sa':
        extract_features = change_extraction_filters.get('scholarly_articles_filter', {}).get('feature_extraction', False) and change_extraction_filters.get('scholarly_articles_filter', {}).get('extract', False)
        extract_datatype_metadata_changes = change_extraction_filters.get('scholarly_articles_filter', {}).get('datatype_metadata_extraction', False) and change_extraction_filters.get('scholarly_articles_filter', {}).get('extract', False)

    if table_suffix == '_less':
        extract_features = change_extraction_filters.get('less_filter', {}).get('feature_extraction', False) and change_extraction_filters.get('less_filter', {}).get('extract', False)
        extract_datatype_metadata_changes = change_extraction_filters.get('less_filter', {}).get('datatype_metadata_extraction', False) and change_extraction_filters.get('less_filter', {}).get('extract', False)

    if table_suffix == '':
        extract_features = change_extraction_filters.get('rest', {}).get('feature_extraction', False) # rest is extracted by default
        extract_datatype_metadata_changes = change_extraction_filters.get('rest', {}).get('datatype_metadata_extraction', False)

    try:
        if len(batch['revision']) > 0:
            insert_rows_copy(conn, f'revision{table_suffix}', batch['revision'], REVISION_COLS, REVISION_PK)
        
        if len(batch['value_change']) > 0:
            insert_rows_copy(conn, f'value_change{table_suffix}', batch['value_change'], VALUE_CHANGE_COLS, VALUE_CHANGE_PK)
            
        if len(batch['qualifier_change']) > 0:
            insert_rows_copy(conn, f'qualifier_change{table_suffix}', batch['qualifier_change'], QUALIFIER_CHANGE_COLS, QUALIFIER_CHANGE_PK)
        
        if len(batch['reference_change']) > 0:
            insert_rows_copy(conn, f'reference_change{table_suffix}', batch['reference_change'], REFERENCE_CHANGE_COLS, REFERENCE_CHANGE_PK)
        
        if extract_datatype_metadata_changes and len(batch['datatype_metadata_change']) > 0:
            insert_rows_copy(conn, f'datatype_metadata_change{table_suffix}', batch['datatype_metadata_change'], DATATYPE_METADATA_CHANGE_COLS, DATATYPE_METADATA_CHANGE_PK)
        
        if extract_features:
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
        
        # if extract_features and len(batch['features_property_replacement']) > 0:
        #     insert_rows_copy(conn, f'features_property_replacement{table_suffix}', batch['features_property_replacement'], PROPERTY_REPLACEMENT_FEATURE_COLS, PROPERTY_REPLACEMENT_PK)
        
        if len(batch['entity_stats']) > 0:
            insert_rows_copy(conn, f'entity_stats{table_suffix}', batch['entity_stats'], ENTITY_STATS_COLS, ENTITY_STATS_PK)

    except Exception as e:
        print(f'There was an error when batch inserting revisions and changes: {e}', flush=True)
        print(traceback.format_exc(), flush=True)
        raise e


def db_writer(set_up, num_workers, results_queue):

    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    pid = os.getpid()
    log_file = open(f'logs/db_writer_{pid}.log', 'w', buffering=1)  # Line buffered
    
    def log(msg):
        """Helper to write to both stdout and log file"""
        log_file.write(f"{msg}\n")
        log_file.flush()

    log(f"[DB_WRITER] Starting - Num workers: {num_workers}")

    script_dir = Path(__file__).parent
    with open(script_dir.parent / set_up.get('database_config_path', 'config/db_config.json')) as f:
        db_config = json.load(f)
    
    conn = psycopg2.connect(
        dbname=db_config["DB_NAME"],
        user=db_config["DB_USER"],
        password=db_config["DB_PASS"],
        host=db_config["DB_HOST"],
        port=db_config["DB_PORT"],
        connect_timeout=30,
        gssencmode='disable',
        client_encoding='UTF8'
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
        # 'features_property_replacement',
        'entity_stats'
    ]

    batches = {
        '': {table: [] for table in base_table_names},
        '_sa': {table: [] for table in base_table_names},
        '_ao': {table: [] for table in base_table_names},
        '_less': {table: [] for table in base_table_names}
    }

    workers_finished = 0
    last_write = time.time()

    try:
        while workers_finished < num_workers:
            try:

                result = results_queue.get(timeout=60)
                
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
                batch_size = set_up.get('change_extraction_processing', {}).get('db_batch_size', 5000)
                if len(batches[table_suffix]['revision']) >= batch_size or (time_since_write > 15 and current_batch_size > 0):

                    batch_insert(conn, batches[table_suffix], set_up, table_suffix=table_suffix)

                    # Clear this batch
                    for table in batches[table_suffix]:
                        batches[table_suffix][table] = []

                    last_write = time.time()

                gc.collect(generation=0)
                
            except queue.Empty:
                log(f"[DB_WRITER] Queue empty timeout - flushing batches")
                for suffix, batch in batches.items():
                    if any(len(v) > 0 for v in batch.values()):
                        batch_insert(conn, batch, set_up, table_suffix=suffix)
                        for table in batch:
                            batch[table] = []
    
        for suffix, batch in batches.items():
            if any(len(v) > 0 for v in batch.values()):
                batch_insert(conn, batch, set_up, table_suffix=suffix)
        
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