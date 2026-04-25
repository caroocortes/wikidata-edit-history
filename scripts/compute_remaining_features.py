import json
import psycopg2
import time
import yaml
from pathlib import Path

from scripts.feature_creation import FeatureCreation
from scripts.const import SETUP_PATH

if __name__ == "__main__":

    script_dir = Path(__file__).parent
    with open(script_dir.parent / Path(SETUP_PATH), 'r') as f:
        set_up = yaml.safe_load(f)

    db_config_path = script_dir.parent / Path(set_up.get("db_config_path", "config/db_config.json"))
    with open(db_config_path) as f:
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

    feature_creator = FeatureCreation(conn)

    table_suffix = ''
    max_batches = None
    datatypes = ['entity', 'text']
    
    for datatype in datatypes:

        if datatype == 'entity' and set_up.get('update_entity_labels_descriptions', False):
            feature_creator.update_label_description_entity_features(table_suffix)

            # set to False so it doesn't updates hte labels and descriptions again
            set_up['update_entity_labels_descriptions'] = False
            with open(script_dir.parent / Path(SETUP_PATH), 'w') as f:
                yaml.dump(set_up, f)

        start = time.time()
        feature_creator.create_remaining_features(datatype, table_suffix, max_batches=max_batches)
        end_time = time.time()
        print(f"Total time taken for creating remaining features for {datatype} table: {end_time - start} seconds")
    
    conn.close()