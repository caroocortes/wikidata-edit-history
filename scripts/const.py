WIKIDATA_SERVICE_URL = "https://dumps.wikimedia.org/wikidatawiki/20250601/"

# Paths
DOWNLOAD_LINKS_FILE_PATH = 'data/xml_download_links.txt'
PROCESSED_FILES_PATH = 'logs/processed_files.txt'
PARSER_LOG_FILES_PATH = 'logs/parser_log_files.json'
ERROR_REVISION_TEXT_PATH = "logs/error_revision_text.txt"
REVISION_NO_CLAIMS_TEXT_PATH = "logs/revision_no_claims.txt"

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

# Label and description aren't considered "properties" with their own P-id's so we create our own
LABEL_PROP_ID = -1
DESCRIPTION_PROP_ID = -2

QUEUE_SIZE = 150

# Wikidata's special values
NO_VALUE = 'novalue'
SOME_VALUE = 'somevalue'

# Wikidata's XML namespace
NS = "http://www.mediawiki.org/xml/export-0.11/"

# Wikidata's datatypes
WD_STRING_TYPES = ['monolingualtext', 'string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation', 'unknown-values']
WD_ENTITY_TYPES = ['wikibase-item', 'wikibase-entityid', 'wikibase-property', 'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema']
