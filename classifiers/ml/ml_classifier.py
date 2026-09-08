import glob
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.utils.class_weight import compute_sample_weight
from xgboost import XGBClassifier
from sklearn.multioutput import MultiOutputClassifier
from sklearn.preprocessing import MultiLabelBinarizer, StandardScaler, LabelBinarizer
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from iterstrat.ml_stratifiers import MultilabelStratifiedKFold
from IPython.display import display
import csv

import io
import matplotlib.pyplot as plt
import pickle
import time
import os
import numpy as np
import pickle
import json

from .utils import get_time_unit
from classifiers.feature_creation import FeatureCreation
from parser_scripts.const import ENTITY_UPDATES_COLS, TEXT_UPDATES_COLS
from .const import BASE_KEY_TYPES, ML_MODELS, ML_MODELS_LABELS, TRAINING_DATASET_DIR, TRAINING_INFO_DIR, FEATURES_DIR, TEXT_SIMPLE_FEATURE_COLS, TEXT_EMBEDDING_FEATURE_COLS, ENTITY_SIMPLE_FEATURES_TYPES, ENTITY_EMBEDDING_FEATURE_COLS
from sql_runner.sql_runner import SQLRunner

class MLClassifier():
    def __init__(self, config_path: str):

        try: 
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            print(f"Error loading ML classifier config from {config_path}: {e}")
            raise e

        self.feature_creation = FeatureCreation()

        self.random_state = self.config.get('random_state', 42)
        self.fold_splits = self.config.get('fold_splits', 5)
        self.prob_threshold = self.config.get('prob_threshold', 0.5)

        self.runtimes = dict()

    # ------------------------------------------------------------------------
    # Methods to train models
    # ------------------------------------------------------------------------

    def get_features(self, datatype, df):

        if datatype == 'text':

            df[TEXT_SIMPLE_FEATURE_COLS] = df.apply(
                lambda row: self.feature_creation.create_text_features('text', row['old_value'], row['new_value']),
                axis=1, result_type='expand'
            )
            df = self.feature_creation.create_embedding_features(df, old_col='old_value', new_col='new_value')
            feature_cols = TEXT_SIMPLE_FEATURE_COLS + TEXT_EMBEDDING_FEATURE_COLS

        elif datatype == 'entity':
            entity_simple_features = list(ENTITY_SIMPLE_FEATURES_TYPES.keys())
            df[entity_simple_features] = df.apply(
                lambda row: self.feature_creation.create_text_features('entity', row['old_value_label'], row['new_value_label']),
                axis=1, result_type='expand'
            )
            df = self.feature_creation.create_embedding_features(df, old_col='old_value_label', new_col='new_value_label')
            feature_cols = entity_simple_features + list(ENTITY_EMBEDDING_FEATURE_COLS.keys())

        return df, feature_cols

    def _load_gs_lookup(self, datatype):
        """
            Loads the gold standard labels once and caches them in memory,
            keyed by (old_value, new_value) -> label.
            Rows matching this lookup already have a known-correct label,
            so they can skip feature computation + model inference entirely.
        """
        cache_attr = f'_gs_lookup_{datatype}'
        cached = getattr(self, cache_attr, None)
        if cached is not None:
            return cached

        gs_path = f'{TRAINING_DATASET_DIR}/gs_{datatype}.csv'
        gs_df = pd.read_csv(gs_path)

        gs_df['old_value'] = gs_df['old_value'].astype(str).str.strip().str.strip('"')
        gs_df['new_value'] = gs_df['new_value'].astype(str).str.strip().str.strip('"')

        lookup = gs_df.set_index(['old_value', 'new_value'])['label'].to_dict()
        setattr(self, cache_attr, lookup)
        return lookup

    def _split_by_gs(self, df, gs_lookup):
        """
            Splits a batch DataFrame into rows already covered by the gold
            standard (df_gs) and rows that still need feature computation +
            model inference (df_remaining).
        """
        keys = list(zip(
            df['old_value'].astype(str),
            df['new_value'].astype(str)
        ))
        gs_labels = pd.Series([gs_lookup.get(k) for k in keys], index=df.index)

        is_gs = gs_labels.notna()
        df_gs = df[is_gs].copy()
        df_gs['_gs_label'] = gs_labels[is_gs]

        df_remaining = df[~is_gs].copy()

        return df_gs, df_remaining

    def perform_grid_search(self, classifier, datatype, X_scaled, y_binary, cv, sample_weight=None):
        print(f'Performing grid search for {classifier} on datatype {datatype}', flush=True)
        
        if classifier == 'Random_Forest':
            param_grid = {
                'n_estimators': [50, 100, 150, 200],
                'max_depth': [None, 20, 40],
                'min_samples_leaf': [1, 2, 4, 8],
                'max_features': ['sqrt', 'log2', None],
                'bootstrap': [True],
                'class_weight': ['balanced']
            }
            
            # This already does cross-validation internally
            grid_search = GridSearchCV(RandomForestClassifier(self.random_state), param_grid=param_grid, cv=cv, verbose=2, n_jobs=-1)

        elif classifier == 'Gradient_Boosting':
            param_grid = {
                'n_estimators': [50, 100, 200],
                'max_depth': [3, 5, 7],
                'learning_rate': [0.01, 0.05, 0.1, 0.2],
                'subsample': [0.6, 0.8, 1.0],
            }

            base_estimator = GradientBoostingClassifier(random_state=self.random_state)
            if datatype == 'text':
                base_estimator = MultiOutputClassifier(base_estimator)
                for key in list(param_grid.keys()):
                    param_grid[f'estimator__{key}'] = param_grid.pop(key)
                    print(f"Updated param_grid for multi-output: {param_grid}", flush=True)
            grid_search = GridSearchCV(base_estimator, param_grid=param_grid, cv=cv, verbose=2, n_jobs=-1)

        elif classifier == 'XGBoost': # does not require meta model for multi-label
            # https://www.kaggle.com/code/prashant111/a-guide-on-xgboost-hyperparameters-tuning
            param_grid = {
                'n_estimators': [50, 100, 150, 200],
                'max_depth': [3, 5, 7, 10],
                'learning_rate': [0.01, 0.05, 0.1, 0.2],
                'subsample': [0.6, 0.8, 1.0],
                'colsample_bytree': [0.6, 0.8, 1.0]
            }

            grid_search = GridSearchCV(XGBClassifier(random_state=self.random_state), param_grid=param_grid, cv=cv, verbose=2, n_jobs=-1)

        fit_kwargs = {}
        if sample_weight is not None:
            fit_kwargs['sample_weight'] = sample_weight

        y_for_fit = y_binary.argmax(axis=1) if datatype != 'text' else y_binary

        grid_search.fit(X_scaled, y_for_fit, **fit_kwargs)
        best_params = grid_search.best_params_

        # remove the prefix estimator__ from the result
        best_params = {
            key.replace('estimator__', ''): value 
            for key, value in best_params.items()
        }

        return best_params

    def get_model_instance(self, classifier, best_params, datatype=None):
        """
            Returns model instance for the specified classifier.
            Grid search is performed to find the best parameters
        """
        
        if classifier == 'Random_Forest': # already supports multi-label
            
            model = RandomForestClassifier(
                n_estimators=best_params['n_estimators'],
                max_depth=best_params['max_depth'],
                min_samples_leaf=best_params['min_samples_leaf'],
                max_features=best_params['max_features'],
                bootstrap=best_params['bootstrap'],
                class_weight='balanced', # this handles unbalanced classes
                random_state=self.random_state
            )

        elif classifier == 'Gradient_Boosting':
            base = GradientBoostingClassifier(
                n_estimators=best_params['n_estimators'],
                max_depth=best_params['max_depth'],
                learning_rate=best_params['learning_rate'],
                subsample=best_params['subsample'],
                random_state=self.random_state
            )

            model = MultiOutputClassifier(base) if datatype == 'text' else base

        elif classifier == 'XGBoost': 
            
            # Base classifier
            model = XGBClassifier(
                n_estimators=best_params['n_estimators'], 
                max_depth=best_params['max_depth'],
                learning_rate=best_params['learning_rate'],
                subsample=best_params['subsample'],
                colsample_bytree=best_params['colsample_bytree'],
                random_state=self.random_state 
            )
        
        return model, None

    def perform_kfold_training(self, df, X, y_binary, datatype, label_binarizer, feature_cols, df_index, classifier):
        print(f'Performing k-fold training for {datatype}, {classifier}', flush=True)
        
        if datatype == 'text':
            cv = MultilabelStratifiedKFold(n_splits=self.fold_splits, shuffle=True, random_state=self.random_state)
            split = cv.split(X, y_binary)
        else: # entity is not multi label
            y_labels_1d = y_binary.argmax(axis=1)  # collapse one-hot back to a single class label per row, for stratification only
            cv = StratifiedKFold(n_splits=self.fold_splits, shuffle=True, random_state=self.random_state)
            split = cv.split(X, y_labels_1d)

        results_folds = []
        # aggregate all test and predictions across all folds, given that each instance appears only once in the test set
        # across all folds. Then, I have a prediction for each instance and then I calculate precision, recall, accuracy, f1
        all_y_test = []
        all_y_pred = []

        start_time = time.time()

        for fold, (train_index, test_index) in enumerate(split, 1):
            print('FOLD: ', fold, flush=True)

            X_train, X_test = X.iloc[train_index], X.iloc[test_index]
            y_train, y_test = y_binary[train_index], y_binary[test_index]

            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)

            # Inner CV to find best hyperparameters for the model
            # this is ran only on the training, so the test is not seen
            sample_weight_train = None
            if datatype != 'text':
                y_train_1d = y_train.argmax(axis=1)
                sample_weight_train = compute_sample_weight('balanced', y_train_1d)
                gs_cv_obj = StratifiedKFold(n_splits=3, shuffle=True, random_state=self.random_state)
                gs_cv = list(gs_cv_obj.split(X_train_scaled, y_train_1d))   # materialize with 1D labels
            else:
                gs_cv_obj = MultilabelStratifiedKFold(n_splits=3, shuffle=True, random_state=self.random_state)
                gs_cv = list(gs_cv_obj.split(X_train_scaled, y_train))       # materialize with one-hot y

            best_params = self.perform_grid_search(
                classifier, datatype,
                X_train_scaled, y_train,
                gs_cv,   
                sample_weight=sample_weight_train
            )

            model, _ = self.get_model_instance(classifier, best_params, datatype=datatype)

            metrics_results = {}

            actual_test_index = df_index[test_index]

            fit_kwargs = {}
            if sample_weight_train is not None:
                fit_kwargs['sample_weight'] = sample_weight_train

            y_train_for_fit = y_train_1d if datatype != 'text' else y_train

            clf = model.fit(X_train_scaled, y_train_for_fit, **fit_kwargs)

            y_pred = np.zeros((len(X_test), len(label_binarizer.classes_)))
            X_test_scaled = scaler.transform(X_test)
            y_pred_proba = model.predict_proba(X_test_scaled)
            # predict_proba from docs: ndarray of shape (n_samples, n_classes), or a list of such arrays
            # The class probabilities of the input samples. The order of the classes corresponds to that in the attribute classes_.
            
            if datatype == 'text':
                if isinstance(y_pred_proba, list):
                    # the sklearn classifiers return a list of arrays, one per label
                    # each array has shape (n_samples, 2), where the second column is the positive class probability

                    for label_idx in range(len(label_binarizer.classes_)):
                        probs = y_pred_proba[label_idx][:, 1] # positive class prob for label
                        y_pred[:, label_idx] = (probs >= self.prob_threshold).astype(int)

                    # cases where none of the probs reaches 0.5
                    no_prediction_mask = y_pred.sum(axis=1) == 0
                    if no_prediction_mask.any():
                        # get all probabilities as array (n_samples, n_classes)
                        all_probs = np.column_stack([y_pred_proba[j][:, 1] for j in range(len(label_binarizer.classes_))]) # get positive class porb
                        # for samples with no prediction, set highest prob class to 1
                        max_indices = np.argmax(all_probs[no_prediction_mask], axis=1)
                        y_pred[no_prediction_mask, max_indices] = 1
                else:
                    # XGboost returns an ndarray with shape (n_samples, n_classes)
                    # so it gives you for every sample the probabilities for each class
                    y_pred = (y_pred_proba >= self.prob_threshold).astype(int)    

                    no_prediction_mask = y_pred.sum(axis=1) == 0
                    if no_prediction_mask.any():
                        max_indices = np.argmax(y_pred_proba[no_prediction_mask], axis=1)
                        y_pred[no_prediction_mask, max_indices] = 1
            else: 
                # entity is not multi-label, so we can just take the class with the highest probability
                if isinstance(y_pred_proba, list):
                    all_probs = np.column_stack([p[:, 1] for p in y_pred_proba])
                else:
                    all_probs = y_pred_proba
                max_indices = np.argmax(all_probs, axis=1)
                y_pred[np.arange(len(y_pred)), max_indices] = 1

            if datatype == 'entity':
                debug_rows = df.loc[actual_test_index, ['old_value_label', 'new_value_label']].copy()
            else:
                debug_rows = df.loc[actual_test_index, ['old_value', 'new_value']].copy()
                
            debug_rows['true_labels'] = list(label_binarizer.inverse_transform(y_test))
            debug_rows['pred_labels'] = list(label_binarizer.inverse_transform(y_pred))

            print(f"\n--- Fold {fold} sample predictions ({datatype}) ---", flush=True)
            print(debug_rows.head(5).to_string(), flush=True)

            for i, class_label in enumerate(label_binarizer.classes_):
                #NOTE: this selects all rows for label i
                if not class_label in metrics_results:
                    metrics_results[class_label] = {}

                label_accuracy = accuracy_score(y_test[:, i], y_pred[:, i])
                label_precision = precision_score(y_test[:, i], y_pred[:, i], zero_division=0)
                label_recall = recall_score(y_test[:, i], y_pred[:, i], zero_division=0)
                label_f1 = f1_score(y_test[:, i], y_pred[:, i], zero_division=0)
                
                metrics_results[class_label]['accuracy'] = label_accuracy
                metrics_results[class_label]['precision'] = label_precision
                metrics_results[class_label]['recall'] = label_recall
                metrics_results[class_label]['f1'] = label_f1

            all_y_test.append(y_test)
            all_y_pred.append(y_pred)

            results_folds.append({
                'classifier': classifier.lower(),
                'fold': fold,
                'scaler': scaler,
                'metrics_results': metrics_results,
                'model': clf,
                'features': feature_cols,
                'train_index': train_index, 
                'test_index': actual_test_index,
                'label_binarizer': label_binarizer,
                'best_params': best_params,
                # 'label_distribution': Counter(all_labels),
                'X_test': X_test,
                'y_pred': y_pred,
                'y_test': y_test
            })

        training_time = time.time() - start_time
        if datatype not in self.runtimes:
            self.runtimes[datatype] = dict()
        
        self.runtimes[datatype][classifier] = training_time
        
        labels = label_binarizer.classes_

        micro_averages = dict()
        all_y_test = np.vstack(all_y_test) # concatenate all folds
        all_y_pred = np.vstack(all_y_pred)
        
        for i, class_label in enumerate(labels):
            micro_averages[class_label] = {
                'precision': precision_score(all_y_test[:, i], all_y_pred[:, i], zero_division=0),
                'recall': recall_score(all_y_test[:, i], all_y_pred[:, i], zero_division=0),
                'accuracy': accuracy_score(all_y_test[:, i], all_y_pred[:, i]),
                'f1': f1_score(all_y_test[:, i], all_y_pred[:, i], zero_division=0)
            }

        print(f"Training completed for classifier {classifier}. Time taken: {training_time:.2f} seconds")
        for datatype in micro_averages:
            print(f"Micro averages for {datatype}:")
            print('F1:', micro_averages[datatype]['f1'])
            print('Precision:', micro_averages[datatype]['precision'])
            print('Recall:', micro_averages[datatype]['recall'])
            print('Accuracy:', micro_averages[datatype]['accuracy'])

        return results_folds, micro_averages
    
    def train_classifier(self):
        os.makedirs(FEATURES_DIR, exist_ok=True)
        
        datatypes = ['entity', 'text'] 

        classifiers_rf = dict()
        classifiers_gb = dict()
        classifiers_xgb = dict()
        
        for datatype in datatypes:
            print(f"\n{'='*50}")
            print(f"Training classifier for: {datatype}", flush=True)
            print(f"{'='*50}", flush=True)

            df_gs = pd.read_csv(f'{TRAINING_DATASET_DIR}/gs_{datatype}.csv')

            #############################
            #   Load or create features
            #############################
            if os.path.isfile(f'{FEATURES_DIR}/gs_features_{datatype}.csv'):
                df = pd.read_csv(f'{FEATURES_DIR}/gs_features_{datatype}.csv', index_col=0)
                with open(f'{FEATURES_DIR}/feature_cols_{datatype}.pkl', 'rb') as f:
                    feature_cols = pickle.load(f)
                print('Features already exist, loading.', flush=True)
            else:
                print('Features dont exist, creating.', flush=True)
                start = time.perf_counter()
                # df is already filtered per datatype inside get_features
                df, feature_cols = self.get_features(datatype, df_gs)
                end = time.perf_counter()
                print(f"Time taken to create features for {datatype}: {end - start} seconds", flush=True)

                os.makedirs(FEATURES_DIR, exist_ok=True)
                df.to_csv(f'{FEATURES_DIR}/gs_features_{datatype}.csv', index=True)

            with open(f'{FEATURES_DIR}/feature_cols_{datatype}.pkl', 'wb') as f:
                pickle.dump(feature_cols, f)

            # Fill NAN/Inf with 0
            X = df[feature_cols].astype(float).fillna(0) # features
            X.replace([np.inf, -np.inf], np.nan, inplace=True)
            X.fillna(0, inplace=True)

            label_binarizer = None
            if datatype == 'text':
                # Split label into binary columns
                df['labels_list'] = df['label'].fillna('').str.split(',').apply(lambda x: [l.strip() for l in x])
        
                # all_labels = [label for labels in df['labels_list'] for label in labels]

                # To do multi-label classification we need 1 column per label
                label_binarizer = MultiLabelBinarizer()
                y_binary = label_binarizer.fit_transform(df['labels_list'])
                
            else: # entity is not multi label
                label_binarizer = LabelBinarizer()
                y_binary = label_binarizer.fit_transform(df['label'])

            results_folds_rf, micro_averages_rf = self.perform_kfold_training(df, X, y_binary, datatype, label_binarizer, feature_cols, df.index.values, classifier='Random_Forest')
            results_folds_gb, micro_averages_gb = self.perform_kfold_training(df, X, y_binary, datatype, label_binarizer, feature_cols, df.index.values, classifier='Gradient_Boosting')
            results_folds_xg, micro_averages_xg = self.perform_kfold_training(df, X, y_binary, datatype, label_binarizer, feature_cols, df.index.values, classifier='XGBoost')

            classifiers_rf[datatype] = {
                'results_folds': results_folds_rf,
                'micro_averages': micro_averages_rf
            }

            classifiers_gb[datatype] = {
                'results_folds': results_folds_gb,
                'micro_averages': micro_averages_gb
            }

            classifiers_xgb[datatype] = {
                'results_folds': results_folds_xg,
                'micro_averages': micro_averages_xg
            }

        models_to_save = {
            'random_forest': classifiers_rf,
            'gradient_boosting': classifiers_gb,
            'xgboost': classifiers_xgb
        }

        os.makedirs(TRAINING_INFO_DIR, exist_ok=True)

        for model, dict_ in models_to_save.items():
            if not os.path.isfile(f'{TRAINING_INFO_DIR}/training_info_{model}.pkl'):
                with open(f'{TRAINING_INFO_DIR}/training_info_{model}.pkl', 'wb') as f:
                    pickle.dump(dict_, f)
            else:
                try:
                    with open(f'{TRAINING_INFO_DIR}/training_info_{model}.pkl', 'rb') as f:
                        info = pickle.load(f)
                except Exception as e:
                    print(f"Error loading existing training info for {model}: {e}")
                    raise e
                
                for dt_class in dict_.keys():
                    info[dt_class] = dict_[dt_class]
                
                with open(f'{TRAINING_INFO_DIR}/training_info_{model}.pkl', 'wb') as f:
                    pickle.dump(info, f)

        with open(f'{TRAINING_INFO_DIR}/training_runtimes.pkl', 'wb') as f:
            pickle.dump(self.runtimes, f)

        for dt_class, runtime_models in self.runtimes.items():

            print(f'# ------ {dt_class.upper()} ------ #')
            for model, runtime in runtime_models.items():
                print(f'{model}: {runtime} seconds')
            print('# ------------------------------ #')
        
    # ------------------------------------------------------------------------
    # Methods to classify changes with the trained models
    # ------------------------------------------------------------------------
    def classify_batch(self, training_info_model, df, X, X_index, dt_label):
        """
            We do voting with the models from all folds
            Make all models prdict, average the prob for the classes across all folds, pick the probs that are > 0.5
            If no prob is > 0.5, take the highest one (this inherently means that change will only have one label assigned, 
            for the other cases multiple labels may be assigned)
        """

        # load results_folds, has the trained model
        results_folds = training_info_model[dt_label]['results_folds']

        all_predictions = []

        print(f"Classifying batch of size {len(X)} for datatype {dt_label} using {len(results_folds)} folds", flush=True)
        for i, fold_result in enumerate(results_folds):
            model = fold_result['model']

            # scale features with same scalers used during training
            scaler = fold_result['scaler']
            X_scaled = scaler.transform(X)
            
            # Get probability predictions for each class
            # For multi-label, this returns shape (n_samples, n_classes)
            pred_proba = model.predict_proba(X_scaled)
            
            # predict_proba for multi-label returns list of arrays (one per class)
            # Each element is (n_samples, 2) for [prob_class_0, prob_class_1]
            # in my case it would be 1 array per label, so for refinement: [[prob_no_refinement_cahnge_1, prob_refinement_change_1], [prob_no_refinement_change_2, prob_refinement_change_2]]
            # want (n_samples, n_classes) with prob of class being 1
            
            if isinstance(pred_proba, list):  # Multi-label case
                # positive class (index 1) for each label
                # each p is an array of lists, corresponding to a specific label
                # when we do p[:, 1] we are getting the prob of class 1 for all examples, for that label
                # with np.column_stack, we stack them on a column, so we get:
                # e.g. pred_proba = array([[0.99799539, 0.00200461], [0.99799539, 0.00200461],[0.00441102, 0.99558898]]), array([[0.99322399, 0.00677601],[0.99606133, 0.00393867],[0.01199732, 0.98800268]])
                # p = array([[0.99799539, 0.00200461], [0.99799539, 0.00200461],[0.00441102, 0.99558898]])
                # p[:, 1] = [ 0.00200461
                #             0.00200461
                #             0.99558898 ]
                # for the next label, it will be another column
                pred_proba_positive = np.column_stack([p[:, 1] for p in pred_proba]) 
            else:  # Single-label case
                pred_proba_positive = pred_proba
            
            # has one array for each fold
            all_predictions.append(pred_proba_positive)

        print(f"Completed predictions for all folds", flush=True)

        # Stack all predictions: shape (n_folds, n_samples, n_classes)
        all_predictions = np.array(all_predictions)

        # Average across folds: shape (n_samples, n_classes)
        avg_prediction = np.mean(all_predictions, axis=0)

        label_binarizer = results_folds[0]['label_binarizer'] # it's the same for all folds

        if dt_label == 'text':
            # Apply probability threshold for each instance
            final_labels = (avg_prediction >= self.prob_threshold).astype(int)
            
            for i in range(len(final_labels)):
                if not final_labels[i].any():                    # no label -> fallback to argmax
                    final_labels[i, np.argmax(avg_prediction[i])] = 1
                    continue
        else:
            final_labels = np.zeros_like(avg_prediction, dtype=int)
            max_indices = np.argmax(avg_prediction, axis=1)
            final_labels[np.arange(len(final_labels)), max_indices] = 1

        # get actual label names
        final_labels_transformed = label_binarizer.inverse_transform(final_labels)

        # create list of labels
        def format_predicted_label(labels):
            if isinstance(labels, str):
                return labels if labels else '(none)'
            return ', '.join(labels) if labels else '(none)'

        pred_df = pd.DataFrame({
            'predicted_labels': [format_predicted_label(labels) for labels in final_labels_transformed]
        }, index=X_index)

        # join labels list to original data
        results_df = df.join(pred_df)

        return results_df
    
    
    def classify_changes(self, dt_label, table_suffix, db_config_path, batch_size=1000000):
        """
            Classify changes for a single datatype/label in smaller batches.
            The DB only stores the raw old/new values (+ labels/descriptions
            for entity), so features are computed per batch via get_features
            instead of being read back already-computed.
        """
        print(f'Starting classification for {dt_label} with batch size {batch_size}', flush=True)

        key_cols = list(BASE_KEY_TYPES.keys())
        key_cols_str = ', '.join(key_cols)

        raw_cols = ENTITY_UPDATES_COLS if dt_label == 'entity' else TEXT_UPDATES_COLS
        value_cols = sorted(set(raw_cols) - set(key_cols))
        value_cols_str = ', '.join(value_cols)

        table_name = dt_label
        label_column = 'label'

        # only text changes go through gold-standard rows here - entity ones
        # are already filtered out upstream by entity_rb_classification,
        # which sets `label` for any row found in the gold standard
        gs_lookup = self._load_gs_lookup(dt_label) if dt_label == 'text' else None

        if db_config_path:
            sql_runner = SQLRunner(db_config_path)
            conn = sql_runner.get_connection()
            cursor = conn.cursor()

            key_cols_temp = ', '.join([f'{col} {col_type}' for col, col_type in BASE_KEY_TYPES.items()])
            
            gs_column = ''
            if dt_label == 'text':
                # entity already had rb classification, so we don't need to add label and gs columns for it
                cursor.execute(f"ALTER TABLE updates_{table_name}{table_suffix} DROP COLUMN IF EXISTS {label_column}, ADD COLUMN IF NOT EXISTS {label_column} TEXT")
                cursor.execute(f"ALTER TABLE updates_{table_name}{table_suffix} DROP COLUMN IF EXISTS gs, ADD COLUMN IF NOT EXISTS gs BOOLEAN DEFAULT FALSE")
                gs_column = ', gs BOOLEAN'

            cursor.execute(f"CREATE TEMP TABLE temp_predictions_{dt_label} ({key_cols_temp}, predicted_labels TEXT {gs_column})")
            conn.commit()

            num_batches = 0

            filt_rb = ''
            if dt_label == 'entity':
                # entity already had rb classification
                filt_rb += 'AND rb = FALSE'

            # load best model for the datatype
            with open(f'{TRAINING_INFO_DIR}/best_model_training_info_{dt_label}.pkl', 'rb') as f:
                training_info_model = pickle.load(f)

            while True:

                time_0 = time.time()
                query = f"""
                    SELECT {key_cols_str}, {value_cols_str}
                    FROM updates_{table_name}{table_suffix}
                    WHERE 
                    (label = '' OR label IS NULL) AND (gs IS NULL OR gs = FALSE) {filt_rb}
                    LIMIT {batch_size}
                """

                df = sql_runner.query_to_df(query)
                conn.commit() # close read transaction to release locks

                time_1 = time.time()
                print(f'Finished loading batch {num_batches+1} from DB, took {time_1 - time_0:.2f} seconds')

                if len(df) == 0:
                    break

                df['gs'] = False

                if gs_lookup is not None:
                    df_gs, df_remaining = self._split_by_gs(df, gs_lookup)
                    if len(df_gs) > 0:
                        print(f'Found {len(df_gs)} rows in gold standard, label already set', flush=True)
                else:
                    df_gs, df_remaining = df.iloc[0:0].copy(), df

                results_parts = []

                if len(df_gs) > 0:
                    results_parts.append(
                        df_gs[key_cols].assign(predicted_labels=df_gs['_gs_label'].values)
                    )
                    df_gs['gs'] = True

                if len(df_remaining) > 0:
                    print(f'Found {len(df_remaining)} rows not in gold standard, computing features', flush=True)

                    time_0 = time.time()
                    df_remaining, feature_cols = self.get_features(dt_label, df_remaining)
                    time_1 = time.time()
                    print(f'Finished computing features for batch {num_batches+1}, took {time_1 - time_0:.2f} seconds')

                    X = df_remaining[feature_cols].astype(float).fillna(0)
                    X.replace([np.inf, -np.inf], np.nan, inplace=True)
                    X.fillna(0, inplace=True)

                    time_0  = time.time()
                    results = self.classify_batch(training_info_model, df_remaining, X, df_remaining.index, dt_label)
                    time_1 = time.time()
                    print(f'Finished classifying batch {num_batches+1}, took {time_1 - time_0:.2f} seconds')

                    cols = key_cols + ['predicted_labels']
                    if dt_label == 'text':
                        cols += ['gs']

                    results_parts.append(results[cols])

                results_filtered = pd.concat(results_parts, ignore_index=True)

                buffer = io.StringIO()
                results_filtered.to_csv(buffer, index=False, header=False, sep=';', quoting=csv.QUOTE_NONE, escapechar='\\')
                buffer.seek(0)

                start_time = time.time()
                cursor.copy_expert(f"COPY temp_predictions_{dt_label} FROM STDIN (FORMAT CSV, DELIMITER ';' , QUOTE '\"', ESCAPE '\\')", buffer)
                elapsed_time = time.time() - start_time
                print(f'Finished loading to temp table in {elapsed_time:.2f} seconds')

                start_time = time.time()
                # Update labels
                filt_upt_gs = ''
                if dt_label == 'text':
                    # entity already had rb classification which already checks for rows in gold_standard so gs was already set
                    #  therefore we only update gs for text rows
                    filt_upt_gs = ', gs = tp.gs'
                cursor.execute(f"""
                    UPDATE updates_{table_name}{table_suffix} f
                    SET {label_column} = tp.predicted_labels {filt_upt_gs}
                    FROM temp_predictions_{dt_label} tp
                    WHERE 
                        {' AND '.join([f'f.{key_col} = tp.{key_col}' for key_col in key_cols])}
                """)
                elapsed_time = time.time() - start_time
                final_time, unit = get_time_unit(elapsed_time)
                print(f'Finished updating table in {final_time} {unit}')

                cursor.execute(f"TRUNCATE TABLE temp_predictions_{dt_label}")

                conn.commit()

                num_batches += 1
            
            print(f'Classified {num_batches} batches from DB for {dt_label}')

        else:
            print('No DB config provided, cannot classify in batches from DB')
            return
            
    # ------------------------------------------------------------------------------------------------
    # Methods to calculate evaluation metrics for the model + best model selection
    # ------------------------------------------------------------------------------------------------
    
    @staticmethod
    def create_data_structure_for_visualization():

        """
        training_info_{model}.pkl structure:
        {
            "datatype": { # for reverted_edit & property_replacement, datatype is the name of the label
                'results_folds': [results per fold],
                'micro_averages': {}
            ...
        }

        results per fold:
        {
            'classifier': string, #gb, xgboost, random_forest
            'fold': int,
            'metrics_results': {
                'label': { 
                    'precision': float,
                    'recall': float,
                    'accuracy': float,
                    'f1': float
                },
                ....
            },
            'model': clf,
            'base_model': model,
            'features': feature_cols,
            ....
        }

        Final structure to save:
        "model": {
            "datatype":{
                "label": {
                    "precision": float,
                    "recall": float,
                    "accuracy": float,
                    "f1": float
                }
            }
        }
        """

        # Create data structure
        results = {}
        for model in ['gradient_boosting', 'random_forest', 'xgboost']:
            print(f'Processing model: {model}')
            
            with open(f'{TRAINING_INFO_DIR}/training_info_{model}.pkl', 'rb') as f:
                training_info_model = pickle.load(f)
            
            results[model] = {}
            
            # go over each fold's results for a single datatype
            for datatype, training_info in training_info_model.items():
                
                micro_averages = training_info['micro_averages']

                results[model][datatype] = {}

                for label, metric_values in micro_averages.items(): # metric values across all folds

                    results[model][datatype][label] = {
                        'precision': metric_values['precision'],
                        'recall': metric_values['recall'],
                        'accuracy': metric_values['accuracy'],
                        'f1': metric_values['f1']
                    }

        # re-order data structure for visualization

        """
        Structure for visualization:
        "datatype": {
            "label": {
                "model": {
                    "precision": float,
                    "recall": float,
                    "accuracy": float,
                    "f1": float
                }
            }
        }
        """

        results_dt_label_model_micro = {}
        for model in results:
            for datatype in results[model]:
                if datatype not in results_dt_label_model_micro:
                    results_dt_label_model_micro[datatype] = {}
                
                for label in results[model][datatype]:
                    if label not in results_dt_label_model_micro[datatype]:
                        results_dt_label_model_micro[datatype][label] = {}
                    
                    results_dt_label_model_micro[datatype][label][model] = results[model][datatype][label]
                
        return results_dt_label_model_micro
    
    @staticmethod
    def metric_visualization(results_dt_label_model):

        metrics = ['precision', 'recall', 'accuracy', 'f1']

        # Count subplots
        total_plots = sum(len(results_dt_label_model[dt]) for dt in results_dt_label_model)
        ncols = 3
        nrows = (total_plots + ncols - 1) // ncols

        fig, axes = plt.subplots(nrows, ncols, figsize=(18, 5*nrows))
        if isinstance(axes, np.ndarray):
            axes = axes.flatten()
        else:
            axes = [axes]

        plot_idx = 0
        for datatype in sorted(results_dt_label_model.keys()):
            for label in sorted(results_dt_label_model[datatype].keys()):
                ax = axes[plot_idx]
                
                x = np.arange(len(ML_MODELS))
                width = 0.2
                
                for i, metric in enumerate(metrics):
                    values = [results_dt_label_model[datatype][label][model][metric] for model in ML_MODELS] # metric (accuracy/precision/recall/f1) values for this label and datatype
                    
                    offset = (i - 1) * width
                    bars = ax.bar(x + offset, values, width, label=metric.capitalize(), alpha=0.8)
                    
                    for bar in bars:
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width()/2., height,
                            f'{height:.2f}',
                            ha='center', va='bottom', fontsize=8)
                
                ax.set_ylabel('Score')
                ax.set_title(f'{datatype.upper()}\n{label}', fontweight='bold', fontsize=12)
                ax.set_xticks(x)
                ax.set_xticklabels(ML_MODELS_LABELS, rotation=45, ha='right', fontsize=9)
                ax.legend(loc='upper left', fontsize=9)
                ax.grid(axis='y', alpha=0.3)
                ax.set_ylim([0, 1.05])
                
                plot_idx += 1

        for idx in range(plot_idx, len(axes)):
            axes[idx].set_visible(False)

        plt.tight_layout()
        os.makedirs(TRAINING_INFO_DIR, exist_ok=True)
        plt.savefig(f'{TRAINING_INFO_DIR}/classifier_metrics_all.png', dpi=300, bbox_inches='tight')
        plt.show()

        print(f'Saved evaluation metric plots to {TRAINING_INFO_DIR}/classifier_metrics_all.png')

    @staticmethod
    def select_best_classifier(results_dt_label_model):
        """
        Selects the best classifier per datatype, based on the number of labels
        (within that datatype) where the model has the highest F1.
        """

        os.makedirs(TRAINING_INFO_DIR, exist_ok=True)
        overall_best_per_datatype = {}

        for datatype in results_dt_label_model:
            score_per_model = {}  # reset per datatype
            df_data = {'datatype': [], 'label': [], 'best_model': [], 'best_f1': []}  # reset per datatype

            for label in results_dt_label_model[datatype]:
                best_f1 = -1
                best_models = []

                for model in results_dt_label_model[datatype][label]:
                    if model not in score_per_model:
                        score_per_model[model] = 0

                    f1 = results_dt_label_model[datatype][label][model]['f1']

                    print(f'Model: {model}', f'F1: {f1:.5f}', 'Datatype:', datatype, 'Label:', label)

                    if f1 > best_f1:
                        best_f1 = f1
                        best_models = [model]
                    elif f1 == best_f1:
                        best_models.append(model)

                df_data['datatype'].append(datatype)
                df_data['label'].append(label)
                df_data['best_model'].append(', '.join(best_models))
                df_data['best_f1'].append(best_f1)

                for model in best_models:
                    score_per_model[model] += 1

            # --- pick the model(s) that win the most labels for this datatype ---
            max_score = max(score_per_model.values())
            winning_models = [m for m, s in score_per_model.items() if s == max_score]
            if len(winning_models) > 1:
                print(f'Warning: multiple models tied for best for datatype {datatype}: {winning_models} (won {max_score} labels)')
                # pick the one that has highest F1 on avg across all labels for this datatype
                avg_f1_per_model = {m: 0 for m in winning_models}
                for label in results_dt_label_model[datatype]:
                    for model in winning_models:
                        avg_f1_per_model[model] += results_dt_label_model[datatype][label][model]['f1']
                avg_f1_per_model = {m: f1 / len(results_dt_label_model[datatype]) for m, f1 in avg_f1_per_model.items()}
                overall_best_per_datatype[datatype] = [max(avg_f1_per_model, key=avg_f1_per_model.get)][0]
                print(f'Picked {overall_best_per_datatype[datatype]} as best for datatype {datatype} based on highest average F1 across all labels')
            else:
                overall_best_per_datatype[datatype] = winning_models[0]

            df = pd.DataFrame(df_data)
            df.to_csv(f'{TRAINING_INFO_DIR}/best_model_per_f1_all_tasks_{datatype}.csv', index=False)
            print(f'Saved per-label winners for {datatype} to CSV')
            print(f'Best model(s) for {datatype}: {winning_models} (won {max_score} labels)')

            with open(f'{TRAINING_INFO_DIR}/training_info_{overall_best_per_datatype[datatype]}.pkl', 'rb') as f:
                print(f'Saving training info for {datatype} from {TRAINING_INFO_DIR}/training_info_{overall_best_per_datatype[datatype]}.pkl ')
                training_info_model = pickle.load(f)

            with open(f'{TRAINING_INFO_DIR}/best_model_training_info_{datatype}.pkl', 'wb') as f:
                pickle.dump(training_info_model, f)
        
    def evaluate_cross_validation(self):
        results_dt_label_model = MLClassifier.create_data_structure_for_visualization()

        MLClassifier.metric_visualization(results_dt_label_model)

        MLClassifier.select_best_classifier(results_dt_label_model)
    
        return results_dt_label_model

