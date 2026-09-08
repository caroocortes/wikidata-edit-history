ML_MODELS = ['gradient_boosting', 'random_forest','xgboost']
ML_MODELS_LABELS = ['Gradient Boosting', 'Random Forest', 'XGBoost']

# ===============================
#  Paths 
# ===============================
TRAINING_INFO_DIR = 'classifiers/ml/training_info' # stores trained models
FEATURES_DIR = 'classifiers/ml/features'
TRAINING_DATASET_DIR = 'classifiers/ml/training_dataset'
CONFIG_DIR = 'classifiers/ml/config'
LOG_DIR = 'classifiers/ml/logs'
SCRIPT_DIR = 'analysis/scripts'
SQL_SCRIPT_DIR = 'analysis/sql'
RESULTS_DIR = 'analysis/results'
LOGS_DIR = 'analysis/logs'

YAML_SETUP_PATH = 'classifier_setup.yml'

BASE_KEY_TYPES = {
    'revision_id': 'BIGINT',
    'property_id': 'INT',
    'value_id': 'TEXT'
}


BASIC_CHANGE_LABELS = ['textual_change','refinement', 'unrefinement', 'property_value_update']

SOFT_INSERTIONS = 'soft_insertions' # normal/deprecated -> preferred 
SOFT_DELETIONS = 'soft_deletions' # rank deprecation (normal/prefered -> deprecated) + adding end time qualifier

CLASSES_PER_DATATYPE = {
    'text': ['textual_change', 'refinement', 'unrefinement', 'property_value_update'],
    'quantity': ['refinement', 'unrefinement', 'property_value_update', 're_formatting'],
    'time': ['refinement', 'unrefinement', 'property_value_update', 're_formatting'],
    'globecoordinate_latitude': ['refinement', 'unrefinement', 'property_value_update', 're_formatting'],
    'globecoordinate_longitude': ['refinement', 'unrefinement', 'property_value_update', 're_formatting'],
    'entity': ['refinement', 'unrefinement', 'property_value_update'] 
}
    
WD_STRING_TYPES = ['string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation']
WD_ENTITY_TYPES = ['wikibase-item', 'wikibase-entityid', 'wikibase-property', 'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema']
WD_BASIC_TYPES = ['globecoordinate_latitude', 'globecoordinate_longitude', 'quantity', 'time', 'monolingualtext', 'unknown-values']

ENTITY_SIMPLE_FEATURES_TYPES = {
    'token_overlap': 'FLOAT',
    'old_in_new': 'INT',
    'new_in_old': 'INT',
    # 'complete_replacement': 'INT',
    # 'word_alignment_ratio': 'FLOAT',
    # 'word_insertions': 'INT',
    # 'word_deletions': 'INT',
    # 'word_substitutions': 'INT',
    # 'has_significant_prefix': 'INT',
    # 'has_significant_suffix': 'INT'
}

ENTITY_EMBEDDING_FEATURE_COLS = {
    'label_cosine_similarity': 'FLOAT', 
    'description_cosine_similarity': 'FLOAT',

    'old_to_new_contradiction': 'FLOAT',
    'old_to_new_entailment': 'FLOAT', 
    'old_to_new_neutral': 'FLOAT',
    'new_to_old_contradiction': 'FLOAT', 
    'new_to_old_entailment': 'FLOAT', 
    'new_to_old_neutral': 'FLOAT',

    'old_to_new_desc_contradiction': 'FLOAT', 
    'old_to_new_desc_entailment': 'FLOAT', 
    'old_to_new_desc_neutral': 'FLOAT',
    'new_to_old_desc_contradiction': 'FLOAT', 
    'new_to_old_desc_entailment': 'FLOAT', 
    'new_to_old_desc_neutral': 'FLOAT'
}

BASE_KEY_TYPES = {
    'revision_id': 'BIGINT',
    'property_id': 'INT',
    'value_id': 'TEXT'
}

TEXT_SIMPLE_FEATURE_COLS = [
    
    'token_overlap',
    # 'complete_replacement',

    'old_in_new',
    'new_in_old',
    'word_alignment_ratio',
    'word_insertions',
    'word_deletions',
    'word_substitutions',
    'has_significant_prefix',
    'has_significant_suffix',

    'word_count_old',
    'word_count_new',
    'special_char_count_diff',
    'whitespace_count_diff',
    'accent_char_count_diff',
    'case_swap_count',
    'char_insertions',
    'char_deletions',
    'char_substitutions',
    'adjacent_char_swap',
    'raw_edit_distance_ratio',
    'residual_edit_distance_ratio',
    'stopword_diff_ratio',
    'plural_pair_ratio',
    'other_word_diff_count',
    'other_word_ratio'
]

TEXT_EMBEDDING_FEATURE_COLS = [
    'value_cosine_similarity',

    'old_to_new_contradiction',
    'old_to_new_entailment', 
    'old_to_new_neutral',
    'new_to_old_contradiction', 
    'new_to_old_entailment', 
    'new_to_old_neutral',
]