import json
import psycopg2

from scripts.feature_creation import FeatureCreation

if __name__ == "__main__":
    

    CONFIG_PATH = 'db_config.json'
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

    feature_creator = FeatureCreation(conn)

    table_suffix = ''
    max_batches = None
    # datatypes = ['quantity', 'time', 'globecoordinate', 'property_replacement', 'text', 'entity']
    datatypes = ['entity']
    for datatype in datatypes:

        # if datatype == 'entity':
        #     # TODO:  ADD PARAMETER TO YAML TO SKIP THIS STEP IF ALREADY DONE
        #     feature_creator.update_label_description_entity_features(table_suffix)

        feature_creator.create_remaining_features(datatype, table_suffix, max_batches=max_batches)
