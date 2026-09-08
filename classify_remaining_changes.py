import yaml
import os
import time

from classifiers.ml.ml_classifier import MLClassifier
from classifiers.llm.llm_classifier import LLMClassifier
from classifiers.rule.rule_based_classifier import RuleBasedClassifier
from classifiers.llm.const import LLM_RESULTS_DIR
import pandas as pd
import json
import psycopg2

if __name__ == "__main__":

    set_up_path = 'classifier_setup.yml'
    with open(set_up_path, 'r') as f:
        set_up = yaml.safe_load(f)

    classifier_type = set_up['config']['classifier_type']

    ml_config_path = set_up['config']['ml_config_path']
    llm_config_path = set_up['config']['llm_config_path']

    if classifier_type == 'llm':
        print("Running LLM baseline", flush=True)
        classifier = LLMClassifier(config_path=llm_config_path)
        datatypes = ['text', 'entity']
        for datatype in datatypes:
            
            path_to_file = set_up['classification_llm'][f'path_to_{datatype}_changes']
            print(f"Classifying {datatype} changes from file: {path_to_file}", flush=True)
            df = pd.read_csv(path_to_file)
            start_time = time.time()
            df_new = classifier._run_batch_classification(df, datatype, 'llm_label')
            end_time = time.time()
            print(f"Time taken for LLM classification of {datatype} changes: {end_time - start_time} seconds")
            os.makedirs(LLM_RESULTS_DIR, exist_ok=True)
            df_new.to_csv(f"{LLM_RESULTS_DIR}/gs_{datatype}_with_llm_labels.csv", index=False)
            classifier.evaluate()

    if classifier_type == 'ml':

        ml_classifier = MLClassifier(config_path=ml_config_path)
        if set_up['classification_ml']['train']:
            ml_classifier.train_classifier()
        
        if set_up['classification_ml']['evaluate']:
            ml_classifier.evaluate_cross_validation()

        if set_up.get('update_entity_labels_descriptions', False):
            db_config_path =set_up.get("config", {}).get("db_config_path", None)
            with open(db_config_path) as f:
                db_config = json.load(f)

            try:
                conn = psycopg2.connect(
                    dbname=db_config["DB_NAME"],
                    user=db_config["DB_USER"],
                    password=db_config["DB_PASS"],
                    host=db_config["DB_HOST"],
                    port=db_config["DB_PORT"],
                    connect_timeout=30,
                    gssencmode='disable'
                )
            except Exception as e:
                print(f"Error connecting to the database: {e}")
                exit(1)

            rb_classifier = RuleBasedClassifier(conn=conn, set_up=set_up)
            table_suffix = set_up['classification_ml']['table_suffix']
            print(f'Updating entity labels and descriptions for table suffix: {table_suffix}', flush=True)
            rb_classifier.update_label_description_entity_features(table_suffix)

            # set to False so it doesn't updates hte labels and descriptions again
            set_up['update_entity_labels_descriptions'] = False
            with open(set_up_path, 'w') as f:
                yaml.dump(set_up, f)
            print(f'Finished updating entity labels and descriptions for table suffix: {table_suffix}', flush=True)

        if set_up['classification_ml']['classify']:

            datatypes = ['entity', 'text']
            table_suffix = set_up['classification_ml']['table_suffix']

            db_config_path =set_up.get("config", {}).get("db_config_path", None)

            if db_config_path is None:
                print("Database configuration path not found in the classifier_setup.yml file.")
                exit(1)

            with open(db_config_path) as f:
                db_config = json.load(f)

            try:
                conn = psycopg2.connect(
                    dbname=db_config["DB_NAME"],
                    user=db_config["DB_USER"],
                    password=db_config["DB_PASS"],
                    host=db_config["DB_HOST"],
                    port=db_config["DB_PORT"],
                    connect_timeout=30,
                    gssencmode='disable'
                )
            except Exception as e:
                print(f"Error connecting to the database: {e}")
                exit(1)

            for datatype in datatypes:
                if datatype == 'entity':
                    rb_classifier = RuleBasedClassifier(conn=conn, set_up=set_up)
                    start = time.perf_counter()
                    rb_classifier.entity_rb_classification(table_suffix)
                    end_time = time.perf_counter()
                    print(f"Total time taken for rule-based classification for {datatype} and suffix {table_suffix}: {end_time - start} seconds")
                    conn.close()

                ml_classifier.classify_changes(datatype, table_suffix, db_config_path)