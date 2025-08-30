WIKIDATA_SERVICE_URL = "https://dumps.wikimedia.org/wikidatawiki/20250601/"

DUMP_DIR = "data/wikidata_dumps_20250601/"

DOWNLOAD_LINKS_FILE_PATH = 'data/xml_download_links.txt'

CREATE_PROPERTY = "CREATE_PROPERTY"
CREATE_PROPERTY_VALUE = "CREATE_PROPERTY_VALUE"
CREATE_ENTITY = "CREATE_ENTITY"
UPDATE_PROPERTY_VALUE = "UPDATE_PROPERTY_VALUE"
UPDATE_PROPERTY_DATATYPE_METADATA = "UPDATE_PROPERTY_DATATYPE_METADATA"
DELETE_ENTITY = "DELETE_ENTITY" # this probably never happens, the entity page seizes to exist
DELETE_PROPERTY = "DELETE_PROPERTY"
DELETE_PROPERTY_VALUE = "DELETE_PROPERTY_VALUE"

BATCH_SIZE_CHANGES = 10000
BATCH_SIZE_ENTITIES = 100

THREADS_PAGE_PROCESS = 6

NO_VALUE = 'novalue'
SOME_VALUE = 'unknown'

# Wikidata's XML namespace
NS = '{http://www.mediawiki.org/xml/export-0.11/}'

WD_STRING_TYPES = ['monolingualtext', 'string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation']
WD_ENTITY_TYPES = ['wikibase-item', 'wikibase-property', 'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema']
