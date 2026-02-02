from dotenv import load_dotenv
from pathlib import Path
import os

dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path)

WIKIDATA_SERVICE_URL = "https://dumps.wikimedia.org/wikidatawiki/20250601/"

# --------------------------------------------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------------------------------------------
DOWNLOAD_LINKS_FILE_PATH = 'data/xml_download_links.txt'
CLAIMED_FILES_PATH = "logs/claimed_files.txt"
LOCK_FILE_PATH = "logs/file_claim.lock"
PROCESSED_FILES_PATH = 'logs/processed_files.txt'
PARSER_LOG_FILES_PATH = 'logs/parser_log_files.json'
ERROR_REVISION_TEXT_PATH = "logs/error_revision_text.txt"
REVISION_NO_CLAIMS_TEXT_PATH = "logs/revision_no_claims.txt"
PROPERTY_LABELS_PATH = f'data/property_labels_with_deleted.csv'
ENTITY_LABEL_ALIAS_PATH = f'data/labels_aliases.csv'
SUBCLASS_OF_PATH = f'data/p279_entity_types.csv'
INSTANCE_OF_PATH = f'data/p31_entity_types.csv'

TRANSITIVE_CLOSURE_PICKLE_FILE_PATH = 'data/transitive_closures/transitive_closure_cache.pkl'
TRANSITIVE_CLOSURE_STATS_PICKLE_FILE_PATH = 'data/transitive_closures/transitive_closure_stats.pkl'

DATA_PATH = 'data'

# --------------------------------------------------------------------------------------------------------------
# PATH TO SUBCLASSES OF ASTRONOMICAL OBJECTS AND SCHOLARLY ARTICLES
# It is used to identify entities of these types
# --------------------------------------------------------------------------------------------------------------
ASTRONOMICAL_OBJECT_TYPES_PATH = f'data/subclassof_astronomical_object.csv'
SCHOLARLY_ARTICLE_TYPES_PATH = 'data/subclassof_scholarly_article.csv'

# --------------------------------------------------------------------------------------------------------------
# LOG PATHS
# --------------------------------------------------------------------------------------------------------------
SCHOLARLY_ARTICLE_STATS_FILE_PATH = 'logs/stats/scholarly_article_stats.csv'
ASTRONOMICAL_OBJECT_STATS_FILE_PATH = 'logs/stats/astronomical_object_stats.csv'
LESS20_STATS_FILE_PATH = 'logs/stats/less20_stats.csv'
STATS_FILE_PATH = 'logs/stats/stats.csv'

# --------------------------------------------------------------------------------------------------------------
# CHANGE TYPES
# --------------------------------------------------------------------------------------------------------------

CREATE_PROPERTY = "CREATE_PROPERTY"
CREATE_PROPERTY_VALUE = "CREATE_PROPERTY_VALUE"
CREATE_ENTITY = "CREATE_ENTITY"
UPDATE_PROPERTY_VALUE = "UPDATE_PROPERTY_VALUE"
UPDATE_PROPERTY_DATATYPE_METADATA = "UPDATE_PROPERTY_DATATYPE_METADATA"
DELETE_PROPERTY = "DELETE_PROPERTY"
DELETE_PROPERTY_VALUE = "DELETE_PROPERTY_VALUE"
UPDATE_RANK = "UPDATE_RANK"
CREATE_QUALIFIER = "CREATE_QUALIFIER"
DELETE_QUALIFIER = "DELETE_QUALIFIER"
CREATE_QUALIFIER_VALUE = "CREATE_QUALIFIER_VALUE"
DELETE_QUALIFIER_VALUE = "DELETE_QUALIFIER_VALUE"
CREATE_REFERENCE = "CREATE_REFERENCE"
DELETE_REFERENCE = "DELETE_REFERENCE"
DELETE_REFERENCE_VALUE = "DELETE_REFERENCE_VALUE"
CREATE_REFERENCE_VALUE = "CREATE_REFERENCE_VALUE"

# --------------------------------------------------------------------------------------------------------------
# CSV PATHS FOR TRANSITIVE CLOSURES
# --------------------------------------------------------------------------------------------------------------
CSV_PATHS = {
    'subclass_transitive': 'data/transitive_closures/subclass_of_transitive.csv',
    'part_of_transitive': 'data/transitive_closures/part_of_transitive.csv',
    'has_part_transitive': 'data/transitive_closures/has_parts_transitive.csv',
    'located_in_transitive': 'data/transitive_closures/located_in_transitive.csv',
}

# ------------------------------------------------------------------------------------------------------------------------------
# Label and description aren't considered "properties" with their own P-id's so we create our own
# ------------------------------------------------------------------------------------------------------------------------------
LABEL_PROP_ID = -1
DESCRIPTION_PROP_ID = -2
REVISION_THRESHOLD = 10
RV_KEYWORDS = ['revert', 'rv', 'undid', 'restore', 'rvv', 'vandal', 'undo']

# ------------------------------------------------------------------------------------------------------------------------------
# Queue size for file processing
# ------------------------------------------------------------------------------------------------------------------------------
QUEUE_SIZE = 10000
BATCH_SIZE = 5000

# ------------------------------------------------------------------------------------------------------------------------------
# Wikidata's special values
# ------------------------------------------------------------------------------------------------------------------------------
NO_VALUE = 'novalue'
SOME_VALUE = 'somevalue'


# ------------------------------------------------------------------------------------------------------------------------------
# Wikidata's XML namespace
# ------------------------------------------------------------------------------------------------------------------------------
NS = "http://www.mediawiki.org/xml/export-0.11/"

# ------------------------------------------------------------------------------------------------------------------------------
# Wikidata's datatypes
# ------------------------------------------------------------------------------------------------------------------------------
WD_STRING_TYPES = ['monolingualtext', 'string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation', 'unknown-values']
WD_ENTITY_TYPES = ['wikibase-item', 'wikibase-entityid', 'wikibase-property', 'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema']


# ------------------------------------------------------------------------------------------------------------------------------
# TABLE COLUMNS
# ------------------------------------------------------------------------------------------------------------------------------
REVISION_COLS = ['prev_revision_id', 'revision_id', 'entity_id', 'timestamp', 'week', 
                 'year_month', 'year', 'user_id', 'username', 'user_type', 'comment', 
                 'file_path', 'redirect', 'q_id_redirect', 'entity_label']
REVISION_PK = ['revision_id']

VALUE_CHANGE_COLS = ['revision_id', 'property_id', 'property_label', 'value_id', 'old_value', 
                     'new_value', 'old_datatype', 'new_datatype', 'change_target', 
                     'action', 'target', 'old_hash', 'new_hash', 'timestamp', 'week', 'year_month', 
                     'year', 'label', 'entity_id', 'is_reverted', 'reversion', 'entity_label']
VALUE_CHANGE_PK = ['revision_id', 'property_id', 'value_id', 'change_target']

QUALIFIER_CHANGE_COLS = ['revision_id', 'property_id', 'property_label', 'value_id', 'qual_property_id', 'qual_property_label', 
                         'value_hash', 'old_value', 'new_value', 'old_datatype', 'new_datatype', 'change_target', 
                         'action', 'target', 'timestamp', 'week', 'year_month', 'year', 'entity_id', 'label', 'entity_label']

QUALIFIER_CHANGE_PK = ['revision_id', 'property_id', 'value_id', 'qual_property_id', 'value_hash', 'change_target']

REFERENCE_CHANGE_COLS = ['revision_id', 'property_id', 'property_label', 'value_id', 'ref_property_id', 'ref_property_label', 
                         'ref_hash', 'value_hash', 'old_value', 'new_value', 'old_datatype', 'new_datatype', 'change_target', 
                         'action', 'target', 'timestamp', 'week', 'year_month', 'year', 'entity_id', 'label', 'entity_label']

REFERENCE_CHANGE_PK = ['revision_id', 'property_id', 'value_id', 'ref_property_id', 'value_hash', 'ref_hash', 'change_target']

DATATYPE_METADATA_CHANGE_COLS = ['revision_id', 'property_id', 'property_label', 'value_id', 'old_value', 'new_value', 'old_datatype', 
                                 'new_datatype', 'change_target', 'action', 'target', 'old_hash', 'new_hash', 
                                 'timestamp', 'week', 'year_month', 'year', 'entity_id', 'label', 'entity_label']
DATATYPE_METADATA_CHANGE_PK = ['revision_id', 'property_id', 'value_id', 'change_target']

# ------------------------------------------------------------------------------------------------------------------------------
# FEATURE COLUMNS
# ------------------------------------------------------------------------------------------------------------------------------

# REVERTED_EDIT_FEATURE_COLS = [
#     'revision_id', 
#     'property_id', 
#     'value_id', 
#     'change_target', 
#     'new_datatype', 
#     'old_datatype', 
#     'action',

#     # features
#     'user_type_encoded', 
#     'day_of_week_encoded', 
#     'hour_of_day', 
#     'is_weekend', 
#     'action_encoded',
#     'is_reverted_within_day', 
#     'num_changes_same_user_last_24h', 
#     'rv_keyword_in_comment_next_10',
#     'hash_reversion_next_10', 
#     'time_to_prev_change_seconds', 
#     'time_to_next_change_seconds',

#     #label
#     'label'
# ]

ENTITY_FEATURE_COLS = [
    'revision_id',
    'property_id',
    'property_label',
    'value_id',
    'change_target',
    'new_datatype',
    'old_datatype', 
    'action',
    'old_value',
    'new_value', 

    'length_diff_abs',
    'token_count_old', 
    'token_count_new', 
    'token_overlap', 
    'old_in_new',
    'new_in_old', 
    'levenshtein_distance',
    'edit_distance_ratio',
    'complete_replacement', 
    'structure_similarity',

    'old_value_subclass_new_value', 
    'new_value_subclass_old_value',

    'old_value_located_in_new_value',
    'new_value_located_in_old_value',
    'old_value_has_parts_new_value',
    'new_value_has_parts_old_value',

    'old_value_part_of_new_value',
    'new_value_part_of_old_value',

    # 'new_value_is_metaclass_for_old_value',
    # 'old_value_is_metaclass_for_new_value' ,

    'old_value_label',
    'new_value_label', 
    'old_value_description', 
    'new_value_description',

    'entity_label',
    'entity_description',
    'entity_types_31',
    'entity_types_279',
    
    'label_cosine_similarity', 
    'description_cosine_similarity', 
    'full_cosine_similarity',

    'label'
]
ENTITY_FEATURE_PK = ['revision_id', 'property_id', 'value_id', 'change_target']


ENTITY_ONLY_FEATURES_COLS_TYPES = {
    'length_diff_abs': 'INT',
    'token_count_old': 'INT', 
    'token_count_new': 'INT', 
    'token_overlap': 'FLOAT', 
    'old_in_new': 'INT',
    'new_in_old': 'INT', 
    'levenshtein_distance': 'INT',
    'edit_distance_ratio': 'FLOAT',
    'complete_replacement': 'INT', 
    'structure_similarity': 'FLOAT',

    'old_value_subclass_new_value': 'INT', 
    'new_value_subclass_old_value': 'INT',

    'old_value_located_in_new_value': 'INT',
    'new_value_located_in_old_value': 'INT',
    'old_value_has_parts_new_value': 'INT',
    'new_value_has_parts_old_value': 'INT',

    'old_value_part_of_new_value': 'INT',
    'new_value_part_of_old_value': 'INT',
    
    'label_cosine_similarity': 'FLOAT', 
    'description_cosine_similarity': 'FLOAT', 
    'full_cosine_similarity': 'FLOAT'
}

BASE_KEY_TYPES = {
    'revision_id': 'BIGINT',
    'property_id': 'INT',
    'value_id': 'TEXT',
    'change_target': 'TEXT'
}

PROP_REP_KEY_TYPES = {
    'pair_id': 'BIGINT'
}

TIME_FEATURE_COLS = [
    'revision_id',
    'property_id',
    'property_label',
    'value_id',
    'change_target',
    'new_datatype',
    'old_datatype',
    'action',
    'old_value',
    'new_value',

    # for time
    'date_diff_days',
    'time_diff_minutes',
    'sign_change', # 0 or 1
    'change_one_to_zero', # YYYY-01-01 -> YYYY-00-00 -> I treated this as formatting
    'change_one_to_value',
    'change_zero_to_one', # YYYY-00-00 -> YYYY-01-01 -> I treated this as refinement?
    'day_added',
    'day_removed',
    'month_added',
    'month_removed',
    'different_year',
    'different_day',
    'different_month',

    'entity_label',
    'entity_description',
    'entity_types_31',
    'entity_types_279',

    'full_cosine_similarity',
    'label'
]
TIME_FEATURE_PK = ['revision_id', 'property_id', 'value_id', 'change_target']

QUANTITY_FEATURE_COLS = [
    'revision_id',
    'property_id',
    'property_label',
    'value_id',
    'change_target',
    'new_datatype',
    'old_datatype',
    'action',
    'old_value',
    'new_value',

    'sign_change',
    'precision_change',
    'precision_added',
    'precision_removed',
    'length_increase',
    'length_decrease',
    'whole_number_change',
    'shared_prefix',
    'shared_prefix_length',

    'entity_label',
    'entity_description',
    'entity_types_31',
    'entity_types_279',

    'full_cosine_similarity',

    'label'
]
QUANTITY_FEATURE_PK = ['revision_id', 'property_id', 'value_id', 'change_target']

GLOBE_FEATURE_COLS = [
    'revision_id',
    'property_id',
    'property_label',
    'value_id',
    'change_target',
    'new_datatype',
    'old_datatype',
    'action',
    'old_value',
    'new_value',

    # for globecoordinate
    'relative_value_diff_latitude',
    'relative_value_diff_longitude',
    'latitude_sign_change', # 0 or 1
    'longitude_sign_change',# 0 or 1
    'latitude_whole_number_change', # 0 or 1
    'longitude_whole_number_change', # 0 or 1
    'coordinate_distance_km',
    'latitude_precision_change', # 0 or 1
    'longitude_precision_change', # 0 or 1
    'latitude_length_increase', # 0 or 1
    'latitude_length_decrease', # 0 or 1
    'longitude_length_increase', # 0 or 1
    'longitude_length_decrease', # 0 or 1
    'longitude_shared_prefix',
    'latitude_shared_prefix',
    'longitude_shared_prefix_length',
    'latitude_shared_prefix_length',
    
    'entity_label', 
    'entity_description',
    'entity_types_31',
    'entity_types_279',

    'full_cosine_similarity',
    'label'
]
GLOBE_FEATURE_PK = ['revision_id', 'property_id', 'value_id', 'change_target']

TEXT_FEATURE_COLS = [
    'revision_id',
    'property_id',
    'property_label',
    'value_id',
    'change_target',
    'new_datatype',
    'old_datatype',
    'action',
    'old_value',
    'new_value',

    'length_diff_abs',
    'token_count_old', 
    'token_count_new',         
    'token_overlap', 
    'old_in_new',
    'new_in_old', 
    'levenshtein_distance',
    'edit_distance_ratio',
    'complete_replacement',
    'structure_similarity',

    'char_insertions',
    'char_deletions',
    'adjacent_char_swap',
    'avg_word_similarity',
    'has_significant_prefix',
    'has_significant_suffix',

    'entity_label', 
    'entity_description',
    'entity_types_31',
    'entity_types_279',

    'full_cosine_similarity',

    'label'
]
TEXT_FEATURE_PK = ['revision_id', 'property_id', 'value_id', 'change_target']

PROPERTY_REPLACEMENT_FEATURE_COLS = [

    'delete_revision_id',
    'delete_property_id',
    'delete_value_id',
    'delete_change_target',

    'create_revision_id',
    'create_property_id',
    'create_value_id',
    'create_change_target',
    
    'time_diff',
    'same_day',
    'same_hour',
    'same_revision',
    'delete_before_create',
    'same_user',
    'property_label_similarity',

    'delete_timestamp',
    'create_timestamp',

    'delete_property_label',
    'create_property_label',

    'delete_user_id',
    'create_user_id',
    
    'label'
]
PROPERTY_REPLACEMENT_PK = ['delete_revision_id', 'delete_property_id', 'delete_value_id', 'delete_change_target', 'create_revision_id', 'create_property_id', 'create_value_id', 'create_change_target']

# ------------------------------------------------------------------------------------------------------------------------------
# STATS COLUMNS
# ------------------------------------------------------------------------------------------------------------------------------
ENTITY_STATS_COLS = [
    'entity_id',
    'entity_label',
    'entity_types_31',
    
    'num_revisions',
    
    'num_value_changes', # this includes all changes to property values (creates, deletes, updates)  !! not rank 
    'num_value_change_creates',
    'num_value_change_deletes',
    'num_value_change_updates',

    'num_rank_changes',
    'num_rank_creates',
    'num_rank_deletes',
    'num_rank_updates',

    'num_qualifier_changes',
    'num_reference_changes',

    'num_datatype_metadata_changes',
    'num_datatype_metadata_creates',
    'num_datatype_metadata_deletes',
    'num_datatype_metadata_updates',
    
    'first_revision_timestamp', 
    'last_revision_timestamp',
    
    'num_bot_edits', 
    'num_anonymous_edits',
    'num_human_edits',
    
    'num_reverted_edits',
    'num_reversions',
    'num_reverted_edits_create',
    'num_reverted_edits_delete',
    'num_reverted_edits_update'
]

ENTITY_STATS_PK = ['entity_id']

ENTITY_PROPERTY_TIME_STATS_COLS = [
    'entity_id',
    'property_id',
    'time_period',
    'num_value_changes',
    'num_value_additions',
    'num_value_deletions',
    'num_value_updates',
    'num_statement_additions',
    'num_statement_deletions',
    'num_soft_insertions',
    'num_soft_deletions',
    'num_rank_changes',
    'num_rank_creates',
    'num_rank_deletes',
    'num_rank_updates',
    'num_reference_additions',
    'num_reference_deletions',
    'num_qualifier_additions',
    'num_qualifier_deletions',
    'num_revisions',
    'num_revisions_bot',
    'num_revisions_human',
    'num_revisions_anonymous',
    'num_unique_editors'
]
ENTITY_PROPERTY_TIME_STATS_PK = ['entity_id', 'property_id', 'time_period']


ACTION_ENCODING = {
    'UPDATE': 2,
    'DELETE': 1,
    'CREATE': 0
}

USER_TYPE_ENCODING = {
    'HUMAN': 2,
    'BOT': 1,
    'ANONYMOUS': 0
}

DAY_OF_WEEK_ENCODING = {
    'Friday': 0, 
    'Monday': 1, 
    'Saturday': 2, 
    'Sunday': 3, 
    'Thursday': 4, 
    'Tuesday': 5, 
    'Wednesday': 6
}
