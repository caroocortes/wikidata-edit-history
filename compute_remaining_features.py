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
    
    # TODO: add text datatype, removed because I already ran that one
    # TODO: add entity
    # datatypes = ['text', 'quantity', 'time', 'quantity', 'globecoordinate', 'entity']
    
    table_suffix = ''
    max_batches = 1
    # datatypes = ['quantity', 'time', 'globecoordinate']
    datatypes = ['entity']
    for datatype in datatypes:
        feature_creator.create_missing_features(datatype, table_suffix, max_batches=max_batches)

