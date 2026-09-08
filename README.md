# WiDiff - Change Extraction and Exploration in Wikidata

This tool extracts changes (diff between revisions) of statement values, ranks, qualifiers, and references, from Wikidata's xml dumps and stores them in a relational DB. For a description of the change extraction, refer to our paper [WiDiff: Change Extraction and Exploration in Wikidata]().

Additionally, this tool was extended to classify changes using a defined change type taxonomy, with rule-based and ML classifiers, as described in [Change classification](#change-classification).

This README is structured as follows:
- [Change extraction](#change-extraction): change extraction prerequisites and configuration parameters.
- [Running WiDiff](#running-widiff): explains how to run the extraction pipeline.
- [Databse schema](#database-schema): database schema description and diagram (includes change schema and feature tables).
- [Change classification](#change-classification): 
  - [Change classification framework and change type taxonomy](#change-classification-framework-and-change-type-taxonomy): Describes the change classification framework and change type taxonomy
  - [ML model training](#ml-model-training): describes how to re-train models if needed and provides links to trained models.
  - [LLM baseline](#llm-baseline): describes how to run the LLM baseline
  - [Classification of remaining changes (Text and Entity)](#classification-of-remaining-changes-text-and-entity): describes how to classify remaining changes (those that weren't classified by rule-based classifiers).
- [Descriptive Analysis -- TODO: Needs UPDATING](#descriptive-analysis): instructions on how to re-run the analysis.
- [Downloading extra data](#downloading-extra-data): explaines how to download extra data (e.g., transitive closures).
- [Transitive Closure Cache Creation](#transitive-closure-cache-creation): instructions on how to create the transitive closure cache from the .csv files obtained in [Downloading extra data](#downloading-extra-data).

**To reproduce the comparison of RDF and Relational DB storage go to *wikidata-edit-history/rdf_benchmarking/README.md*.**

## Project structure
```bash
├── config/           # Configuration files
├── auxiliary_data/           # Auxiliary datasets needed during parsing (subclassof_astronomical_objects.csv, subclassof_scholarly_articles.csv, transitive closure cache)
├── download_scripts/       # Script for downloading XML files + list of download links for the dump of 20250601
├── logs/       # Log folder with logs of change extraction
├── classifiers/ # contains rule, ml and llm classifiers
├── parser_scripts/        # Core parsing classes 
│   ├── file_parser.py              # Processes XML files, extracts pages
│   ├── page_parser.py              # Processes a page (all edit history for an entity)
│   ├── utils.py                    # Auxiliary methods 
│   ├── const.py                    # Constants
│   └── db_writer.py # in charge of storing changes in the DB
├── table_schemas/        # stores .sql schema of DB
└── wdtk/           # Files needed to extract extra data from a WD full dump (uses WD Toolkit)
```

## Change Extraction

*Please read the instructions carefully since there are parameters to be set (in set_up.yml) for running the extraction pipeline.*

### Prerequisites

- Python 3.11.9
- Install required libraries listed in `requirements.txt`.
- Update the following files:
    - data/subclassof_astronomical_objects.csv
    - data/subclassof_scholarly_articles.csv
    - data/property_labels.csv
using the queries in `data/sparql_queries.txt` against [Wikidata's query service](https://query.wikidata.org/), or [QLever](https://qlever.dev/wikidata/)
- Download dump files from Wikidata's dump service. The folder `download/` contains a script to download files from the list of files in `download/xml_download_links.txt`. Note that the list provided is from the dump of June 2025 which may not be available anymore, since Wikidata provides the more recent dumps ([Link to Wikidata dumps][https://dumps.wikimedia.org/wikidatawiki/]).
The files to download are the ones called pages-meta-history (See image below).

<img src="diagrams/pages-meta-history.png" alt="drawing" width="600"/>

### Configuration (set_up.yml)

#### `database_config_path`
Path to the database configuration file, which has to be a json file with the following structure:

```
{
    "DB_USER": DB_USER,
    "DB_PASS": DB_PASS,
    "DB_NAME": DB_NAME,
    "DB_PORT": DB_PORT,
    "DB_HOST": DB_HOST
}
```

We provide a template file in *config/db_config.example.json*.

**NOTE: The DB needs to be created beforehand and the adequate credentials (username, user password, database name, hostname and port) need to be set on *the config/db_config.json* file. The schema is created by the pipeline.**

#### `change_extraction_processing`
Controls how the change extraction pipeline runs.

| Parameter | Description |
|---|---|
| *language* | Language code for extracting labels and descriptions (e.g., *en*) |
| *files_in_parallel* | Number of dump files processed in parallel |
| *pages_in_parallel* | Number of pages processed in parallel within a file |
| *files_directory* | Path to the directory containing the Wikidata dump files (xml.bz2) |
| *memory_consumption_monitoring* | If *true*, logs memory usage during processing |
| *page_queue_size* | Maximum number of pages held in the queue of *file_parser.py* |
| *db_batch_size* | Number of revisions inserted per database batch |
| *db_max_queue_size* | Maximum number of elems held in the queue of *db_writer.py* |

**NOTE: Provide the correct path to the directory storing the dump files (xml.bz2 format) in *files_directory***

#### `change_extraction_filters`
Controls which entity types are extracted and processed. Each filter has the following fields:

| Field | Description |
|---|---|
| *extract* | If *true*, changes for this entity type are extracted |
| *feature_extraction* | If *true*, ML features for change classification are computed for this entity type |
| *datatype_metadata_extraction* | If *true*, datatype metadata changes are extracted |

Available filters:

- **scholarly_articles_filter**: Entities classified as scholarly articles (Q13442814)
- **astronomical_objects_filter**: Entities classified as astronomical objects (Q6999)
- **less_filter**: Entities with fewer than *threshold* changes — used to exclude low-activity entities. *threshold* can be set in *set_up.yml*
- **rest`**: All remaining entities not matched by the above filters. This entities are extracted by default.

#### `reverted_edit_tagging`
Controls revert edit tagging during change extraction.

| Parameter | Description |
|---|---|
| time_threshold_seconds | Maximum time window (in seconds) within which an edit can be considered reverted. Default is *2419200* (4 weeks) |

#### `re_interpretation`
If *true*, performs the re-interpretation step, tagging soft deletions, soft insertions, value updates (for updates between values of different datatypes), refinement/unrefinement/re-formatting/textual_change/property_value_update for time, quantity, globecoordinate, and text data types.

#### `update_entity_labels_descriptions`
If *true*, updates entity labels and descriptions for the table *updates_entity{suffix}*

## Running WiDiff

Activate your Python environment with the dependencies from requirements.txt before running the script.

Run the parser with the following command:

```bash
python3 -m extract_changes [options]
```

**Options:**

`-f FILE`: Path to .xml.bz2 file to process (for single file processing).
`-n NUM_FILES`: maximum number of files to process on a run.

Alternatively, use the provided `run_parser.sh` script to process a maximum of `NUM_FILES` files (Activate environment with requirements.txt beforehand):

```bash
chmod +x run_parser.sh
./run_parser.sh <NUM_FILES> &
```

*Note:* `run_parser.sh` runs `extract_changes.py` with the configuration set in `setup.yml` until `NUM_FILES` files have been processed.

### Parallelization
By default, extract_changes.py uses the following parallelization strategy:
- Creates *files_in_parallel* processes (from set_up.yml) that call FileParser (*file_parser.py*)
- Each FileParser creates *pages_in_parallel* processes (from set_up.yml) to call PageParser (*page_parser.py*) which processes a page (all revisions for an entity).
- Each file also gets its dedicated process for storing changes into the DB (*db_writer.py*)

The system must support at least *files_in_parallel* × *pages_in_parallel* + *files_in_parallel* (for the *db_writer*) cores.

Additionally, `file_parser.py` uses `pbzip2`, therefore, appropriate amount of memory needs to be reserved for processing files. 

![architecture diagram](diagrams/widiff_arch.svg)

### Output Files
The pipeline generates three output files:

- `processed_files.txt`: List of processed files (for tracking)
- `parser_output.log`: Logs from file_parser, page_parser and db_writer
- `parser_log_files.json`: Summary with file size in MB, number of entities, number of processed revisions, avg. revisions per entity, time to read file (secs), total time to process file (secs), peak memory in MB (if `memory_consumption_monitoring: true` in set_up.yml)

## Database schema
The main schema is composed of the **change tables** (value, rank, qualifier, reference and data type metadata). Additionally, we also provide **entity_stats** tables which contain statistics (e.g., number of revisions, number or rank changes, etc.) per entity.

In the following we provide a reduced database schema diagram of the change tables.

The full schema for the tables can be found in *sql/change_schema.sql*, *sql/datatype_metadata_schema.sql*. 

![database schema diagram](diagrams/database_schema_diagram.png)

### Datatype groupings
Since Wikidata defines 18 datatypes, some of which can have added "metadata" (e.g., a value of datatype quantity is accompanied by a unit, lower and upper bound), we group Wikidata's datatypes by their "JSON type" (See []()). For example, Wikidata's quantity datatype maps directly to a "JSON type" "quantity", while geo-shape maps to a "JSON type" "string". Therefore, we end up with the following datatypes: string, quantity, time, entity, globecoordinate.
We also include a new "datatype" named "unknown-values" for the values "somevalue" and "novalue".

In the following we show the groupings for "string" and "entity":
STRING_TYPES = ['string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation']

ENTITY_TYPES = ['wikibase-item', 'wikibase-entityid', 'wikibase-property', 'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema']

### Change Tables

**`revision`** — one row per revision, storing metadata about each edit: the editor (user ID, username, type), timestamp, and a reference to the entity that was edited.

| Column | Description |
|---|---|
| prev_revision_id | ID of the previous revision |
| revision_id | ID of the revision |
| entity_id | ID of the entity |
| file_id | XML file name where this revision is stored |
| timestamp | Timestamp of the revision |
| user_id | ID of the user that made the edit |
| username | Username of the user that made the edit |
| user_type | User type. Can be "human" (for registered users), "bot" or "anonymous" |
| comment | Comment on the revision |
| q_id_redirect | Numeric part of the Q-id of the entity where the current entity is redirected to (e.g., if Q1 is redirected to Q123, then q_id_redirect holds the value 123) |

*Primary key:* revision_id

**`value_change`** — stores changes to statement values, including creations, deletions, and updates. Each row records the old and new value, the action performed, and whether the edit was reverted or is itself a reversion. The `change_target` field distinguishes between changes to the main value, a qualifier, the rank, or datatype metadata.

| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| value_id | ID of the property value |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| action | Indicates the action performed: CREATE, UPDATE or DELETE |
| timestamp | Timestamp of the revision |
| label | Label of change classification. Can contain the values: soft_insertion, soft_deletion, statement_insertion, statement_deletion, refinement, unrefinement, re_formatting, textual_change, property_value_update |
| entity_id | ID of the entity |
| is_reverted | 1 if the edit is reverted, 0 otherwise |
| reversion | 1 if the edit does a reversion, 0 otherwise |
| reversion_timestamp | Timestamp of the edit that does the reversion. This column holds a value only if is_reverted = 1, otherwise it's NULL |
| revision_id_reversion | revision_id of the edit that does the reversion. This column holds a value only if is_reverted = 1, otherwise it's NULL |
    
*Primary key:* (revision_id, property_id, value_id)

*Foreign key:* (revision_id) references revision(revision_id).

**`qualifier_change`** — stores additions and deletions of qualifier values. Since qualifiers lack unique identifiers, only CREATE and DELETE actions are tracked (no UPDATE). Values are identified by a hash of their content.

| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| value_id | ID of the property value |
| qual_property_id | ID of the reference property |
| value_hash | Hash computed from the property value. This hash + qual_property_id identify each statement value |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| action | Indicates the action performed: CREATE or DELETE |
| timestamp | Timestamp of the revision |
| label | Label of change classification. Can contain the values: qualifier_insertion, qualifier_deletion, soft_deletion |
| entity_id | ID of the entity |

*Primary key:* (revision_id, property_id, value_id, qual_property_id, value_hash)

*Foreign key:* (revision_id) references revision(revision_id).

*Note:* (revision_id, property_id, value_id) does not necessarily exist in value_change since a revision could involve only qualifier changes

**`reference_change`** — stores additions and deletions of reference values, following the same approach as `qualifier_change`. Each row is additionally identified by a reference hash (`ref_hash`), which identifies the reference group the value belongs to.

| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| value_id | ID of the property value |
| ref_property_id | ID of the reference property |
| ref_hash | Hash computed from all the statement values in the reference. Identifies the reference (a reference is composed of multiple property - values) |
| value_hash | Hash computed from the property value. This hash + ref_property_id identify each statement value inside the reference |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| action | Indicates the action performed: CREATE or DELETE |
| timestamp | Timestamp of the revision |
| label | Label of change classification. Can contain the values: reference_insertion, reference_deletion |
| entity_id | ID of the entity |

*Primary key:* (revision_id, property_id, value_id, ref_hash, ref_property_id, value_hash)

*Foreign key:* (revision_id) references revision(revision_id).

*Note:* (revision_id, property_id, value_id) does not necessarily exist in value_change since a revision could involve only reference changes

**`datatype_metadata_change`** — stores changes to datatype-specific metadata fields (e.g., `upperBound` for quantity values). These are tracked separately from the main value change.

| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| value_id | ID of the property value |
| old_value | Old value of the datatype metadata |
| new_value | New value of the datatype metadata (e.g., if the 'unit' for a quantity value changes from *square metre (Q25343)* to *metre (Q11573)*, then old_value will have *Q25343* and new_value will have *Q11573*) |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| change_target | Name of datatype metadata (e.g. 'upperBound' for a quantity value) |
| action | Indicates the action performed: CREATE or DELETE |
| timestamp | Timestamp of the revision |
| label | Label of change classification. Can contain the values: context_update |
| entity_id | ID of the entity |

*Primary key:* (revision_id, property_id, value_id, change_target)

*Foreign key:* (revision_id) references revision(revision_id).

**`entity_stats`** — one row per entity, aggregating counts of all change types, user types, reverted edits, and processing times. Useful for entity-level analysis without querying the full change tables.

| Column | Description |
|---|---|
| entity_id | ID of the entity |
| entity_label | Entity label. Extracted as the last one. |
| entity_types_31 | List of Q-ids, corresponding to the last P31 values of the entity |
| entity_types_279 | List of Q-ids, corresponding to the last P279 values of the entity |
| num_revisions | Number of revisions |
| num_value_changes | Number of value changes (CREATE, DELETE, UPDATE) | 
| num_value_change_creates | Number of CREATE for property values changes |
| num_value_change_deletes | Number of DELETE for property values changes |
| num_value_change_updates | Number of UPDATE for property values changes |
| num_rank_changes | Number of rank changes (CREATE, DELETE, UPDATE) | 
| num_rank_creates | Number of CREATE for rank changes |
| num_rank_deletes | Number of DELETE for rank changes |
| num_rank_updates | Number of UPDATE for rank changes |
| num_qualifier_changes | Number of qualifier changes (CREATE, DELETE) | 
| num_reference_changes | Number of reference changes (CREATE, DELETE) |
| num_datatype_metadata_changes | Number of datatype metadata changes (CREATE, DELETE, UPDATE) | 
| num_datatype_metadata_creates | Number of CREATE for datatype metadata changes |
| num_datatype_metadata_deletes | Number of DELETE for datatype metadata changes |
| num_datatype_metadata_updates | Number of UPDATE for datatype metadata changes |
| first_revision_timestamp | First revision timestamp |
| last_revision_timestamp | First revision timestamp |
| num_bot_edits | Number of bot edits | 
| num_anonymous_edits | Number of anonymous edits |
| num_human_edits | Number of human (registered user) edits |
| num_reverted_edits | Number of reverted edit changes (CREATE, DELETE, UPDATE) | 
| num_reversions | Number of reversion changes (CREATE, DELETE, UPDATE) | 
| num_reverted_edits_create | Number of CREATE for reverted edit changes |
| num_reverted_edits_delete | Number of DELETE for reverted edit changes |
| num_reverted_edits_update | Number of UPDATE for reverted edit changes |
| file_path | file name where the edit history of the entity is stored |
| total_xml_parse_time_sec | Total time for reading the full page of the entity with all its edit history in seconds |
| total_process_time_sec | Total time for processing the full edit history of the entity in seconds |
| total_revision_diff_time_sec | Total time for calculating the diff between revisions in seconds |
| num_revisions_timed | Number of revisions for which the time for calculating the diff with a consecutive revision was measured |
| total_rev_edit_time_sec | Total time for reverted edit tagging |
| total_feature_creation_sec | Total time for feature creation in secons |
| num_feature_creations_timed | Number of feature creations calls for which the time was measured |
| total_rule_based_classification_sec | Total time for rule_based_classification_sec |

### Update Tables for Change Classification
One update table for the data types text and entity: `updates_text`, `updates_entity`
Each table stores the UPDATE changes that didn't get classified during rule-based classification, therefore, being classified with ML models.

For entity changes, the table contains all entity UPDATEs since classification depends on label and description of the entities and this is not provided by the edit-history files (we only get the QIDs from the snapshots).

**`updates_text`**
| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| old_value | Old value of the property value |
| new_value | New value of the property value |

*Primary key:* (revision_id, property_id, value_id)

*Foreign key:* (revision_id, property_id, value_id) references value_change(revision_id, property_id, value_id)

**`updates_entity`**
| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| old_value | Old value of the property value |
| new_value | New value of the property value |

| old_value_label | Label of the old_value |
| new_value_label | Label of the new_value |
| old_value_description | Description of the old_value |
| new_value_description | Description of the new_value |

*Primary key:* (revision_id, property_id, value_id)

*Foreign key:* (revision_id, property_id, value_id) references value_change(revision_id, property_id, value_id)

**Note:** All table names include a `{suffix}` placeholder, which is replaced at runtime for the different filters of entity types in `set_up.yml`. The values for this suffix can be: `_sa` (scholarly articles), `_ao` (astronomical objects), `_less` (entities with less than *threshold* value changes)

## Run metrics

See *metrics.ipynb* in *wikidata-edit-history/logs* for run metrics of the change extraction.

*run_results.csv* contains Elapsed time and MaxRSS for 100 files batch jobs ran to process the full June 2025 dump. 

*parser_log_files.csv* contains specs for the different filees in the dump.

## Change classification

This section describes the change classification of Wikidata's edit history for a defined taxonomy.

Structure of section:
- [Change Classification Framework and Change Type taxonomy](#change-classification-framework-and-change-type-taxonomy): presents the [change type taxonomy](#change-type-taxonomy) and classification framework
- [Rule-based classification](#rule-based-classification): explains how to run the rule-based classification.
- [Classification of remaining changes (Text and Entity)](#classification-of-remaining-changes-text-and-entity): explains how to run the classification on the remaining changes left unclassified by rule-based classifiers.
  - [ML model training](#ml-model-training): provides links to model, features, and classification results of our ML classifier for reproducibility and explains how to train and run our ML-based classification.
  - [LLM baseline](#llm-baseline): describes how to run the LLM baseline.

### Change Classification Framework and Change Type taxonomy

This section presents our change classification framework and change type taxonmy.

As shown in the picture below, our change classification framework is composed of 3 steps. The first step classifies edit events which are the basic edits a user can perform on Wikidata entities. In particular, this step classifies (1) statement (and associated rank) insertion, update and deletion, (2) qualifier insertion and deletion, and (3) reference insertion and deletion.
In Step 2 we re-interpret some edit events (e.g., the upgrade of a rank can be classified as soft insertion), tag reverted edits (reverted edits within 4 weeks) and value updates between values of different datatypes (e.g., quantity to string) or from "no value" or "some value" to a concrete value.
Moreover, in this step we classify UPDATE edit events between values of the same data type into *refinement*, *unrefinement*, *re-formatting*, *textual change* and *value update*, using rule-based classifiers.

In Step 3, we classify UPDATE edit events between values of type string-string and entity-entity into refinement, unrefinement, textual change, or value update, using an ML classifier.

![classification framework](change_classification_framework.svg)

Next, we present the definitions of the different change types.

#### Change Type Taxonomy

In the following we present the definitions of change types in our taxonomy.

##### Statement Addition
A new statement is added to an entity. *Example:* for the entity Uruguay (Q77) the statement <Uruguay, capital, Montevideo>[↗](https://www.wikidata.org/w/index.php?title=Q77&diff=next&oldid=5443901) was added.

##### Reference/Qualifier Addition
A reference or qualifier is added to an existing statement. *Example:* The qualifier {end time: 2014} was added to <Luis Suárez, member of sports team, Liverpool F.C.>[↗](https://www.wikidata.org/w/index.php?title=Q26517&diff=prev&oldid=318347070), and the reference {imported from Wikimedia project: Italian Wikipedia} was added to <Luis Suárez, mass, 85>[↗](https://www.wikidata.org/w/index.php?title=Q26517&diff=prev&oldid=355943840).

##### Soft Insertion
A statement's rank is changed from *normal* or *deprecated* to *preferred*, indicating that it represents the most current or accurate value among multiple statements for the same property.
*Example:* <Luis Suárez, given name, Luis> rank was promoted to *preferred*[↗](https://www.wikidata.org/w/index.php?title=Q26517&diff=prev&oldid=889792976), when a second statement <Luis Suárez, given name, Alberto> was added for the same property[↗](https://www.wikidata.org/w/index.php?title=Q26517&diff=next&oldid=889792976).

##### Statement Deletion
A statement is permanently removed from an entity.
*Example:* <Frank van Pamelen, image, Lezing Frank van Pamelen over De Vliegende Hollander.webm>[↗](https://www.wikidata.org/w/index.php?title=Q21281434&diff=next&oldid=1328934396) was deleted after the correct statement (using the *video* property) was added in the prior revision[↗](https://www.wikidata.org/w/index.php?title=Q21281434&diff=prev&oldid=1328934396).

##### Reference/Qualifier Deletion
A reference or qualifier is removed from an existing statement. *Example:* The reference {imported from Wikimedia project: Italian Wikipedia} was removed from <Luis Suárez, mass, 85>[↗](https://www.wikidata.org/w/index.php?title=Q26517&diff=prev&oldid=759983195) and replaced by a more precise one. Similarly, the qualifier {end time: 2020} was removed from <Luis Suárez, member of sports team, Futbol Club Barcelona> and replaced by {end time: September 2020}[↗](https://www.wikidata.org/w/index.php?title=Q26517&diff=prev&oldid=1282785821).

##### Soft Deletion
A statement is logically invalidated without being removed, either by setting its rank to *deprecated* or by adding an *end time (P582)* qualifier (in practice, we also consider the properties *earliest end date (P8554)*, *latest end date (P12506)*, and *end period (P3416)*).

**Examples:**
- <X, native label, Twitter> was deprecated [↗](https://www.wikidata.org/w/index.php?title=Q918&diff=next&oldid=1941896530) in favour of <X, native label, X>[↗](https://www.wikidata.org/w/index.php?title=Q918&diff=prev&oldid=1941896530)
- {end time: July 2023} was added to <X, official name, Twitter>[↗](https://www.wikidata.org/w/index.php?title=Q918&diff=prev&oldid=1942019219) to mark the renaming of the social network.

##### Value Update
A property value is replaced with a semantically different value, altering the statement's meaning. For time, quantity, and globecoordinate values, we also consider sign changes (e.g., -1 -> +1(https://www.wikidata.org/w/index.php?title=Q801294&diff=prev&oldid=110422583)) as value updates, since switching the sign alters the meaning of the value. 
**Examples:**
- *Entity:* Agnosticism (Q288928) -> Islam (Q432)[↗](https://www.wikidata.org/w/index.php?title=334871&diff=prev&oldid=1035395644)
- *Text:* "a country in North America" -> "a country in Central America"[↗](https://www.wikidata.org/w/index.php?title=242&diff=prev&oldid=3747808)
- *Quantity:* +1684527 -> +1719070[↗](https://www.wikidata.org/w/index.php?title=254232&diff=prev&oldid=1028093806) or -1 -> +1 [↗](https://www.wikidata.org/w/index.php?title=Q801294&diff=prev&oldid=110422583)
- *Globe coordinate:* {"latitude": -3.09771, "longitude": -226.98051}{"latitude": -2.8114, "longitude": 118.169}[↗](https://www.wikidata.org/w/index.php?title=26727&diff=prev&oldid=135136435).
- *Time:* -5-00-00 -> +1951-09-25[↗](https://www.wikidata.org/w/index.php?title=210447&diff=prev&oldid=1070077246) or +100-00-00 -> -100-00-00[↗](https://www.wikidata.org/w/index.php?title=Q801294&diff=prev&oldid=663123864), +1764-01-01 -> +1764-00-00[↗](https://www.wikidata.org/w/index.php?title=Q801294&diff=prev&oldid=1574340120)

##### Re-formatting
A property value’s representation is modified at the surface-level, without altering its underlying meaning. For numeric values, re-formatting covers changes in numerical precision that do not alter the value (e.g., adding or removing trailing zeros). Furthermore, globecoordinate values can only be entered in Wikidata in decimal-degree format (e.g., 38.585), as the interface does not support other representations, such as degree-minute-second. As a result, re-formatting changes, which would arise from converting between these formats, do not occur. Additionally, we observed that time values were altered
by adding spaces or special characters, without changing the actual value. We classified these changes as re-formatting.
**Examples:**
- *Quantity:* +4.0 -> +4[↗](https://www.wikidata.org/w/index.php?title=Q801294&diff=prev&oldid=109984021) or +98 -> +98.0[↗](https://www.wikidata.org/w/index.php?title=Q801294&diff=prev&oldid=107182680)

##### Textual Change
A property value of type text is modified to correct or introduce language errors, such as spelling, typos, or grammar, without altering sentence structure or the statement's meaning. This also covers surface-level presentation changes, such as spacing, capitalization, hyphenation, punctuation, and other typographical elements. 
**Examples:**
- "country in southeastern Europe" -> "Country in Southeast Europe"[↗](https://www.wikidata.org/w/index.php?title=Q225&diff=prev&oldid=1678150592)
- "American acterss" -> "American actress"[↗](https://www.wikidata.org/w/index.php?title=Q801294&diff=prev&oldid=143695424)
- "German neuroloigst" -> "German neurologist"[↗](https://www.wikidata.org/w/index.php?title=61670\&diff=prev\&oldid=1294776951)
- "country in southeastern Europe" -> "Country in Southeast Europe"[↗](https://www.wikidata.org/w/index.php?title=Q225\&diff=prev\&oldid=1678150592)
- "Province of Lecce" -> "Pprovince of Lecce"[↗](https://www.wikidata.org/w/index.php?title=16197\&diff=prev\&oldid=2026395311)
- "sovereignt" -> "sovereignty"[↗](https://www.wikidata.org/w/index.php?title=42008&diff=prev&oldid=1288335214)
- "A mountain in Beijing" -> "mountain in Beijing"[↗](https://www.wikidata.org/w/index.php?title=111218927&diff=prev&oldid=2306840798)

##### Refinement / Unrefinement
A property value is replaced by a more (refinement) or less (unrefinement) precise value, without changing the statement's meaning. A refinement may add contextual information, rephrase a text to convey the same meaning more clearly, increase numerical precision, or provide a more specific classification. Analogously, an unrefinement may remove contextual information, decrease numerical precision, or generalize to a broader classification. In both cases, the new value remains semantically compatible with the old one.
**Examples:**
- *Entity:* business (Q4830453) <-> automobile manufacturer (Q786820)[↗](https://www.wikidata.org/w/index.php?title=257815&diff=prev&oldid=1316485355)
- *Text:* "city" <-> "city in South Korea"[↗](https://www.wikidata.org/w/index.php?title=42131&diff=prev&oldid=369720776)
- *Quantity:* +222 <-> +222.4[↗](https://www.wikidata.org/w/index.php?title=192789&diff=prev&oldid=986978112)
- *Globe coordinate:* {"latitude": 14, "longitude": 121.917} <-> {"latitude": 14, "longitude": 121.91666666667} [↗](https://www.wikidata.org/w/index.php?title=103807&diff=prev&oldid=89413888)
- *Time:* +1910-02-10 <-> +1910-00-00[↗](https://www.wikidata.org/w/index.php?title=Q3895839&diff=prev&oldid=1431694434)

##### Reverted Edit
A change is considered reverted when a subsequent edit restores a previous value of a property.
*Example:* "44th President of the United States of America" -> "Worst president ever" for Barack Obama (Q76) [↗](https://www.wikidata.org/w/index.php?title=Q76&diff=prev&oldid=7375872) was reverted in a subsequent revision.

---

### Rule-based classification

Rule-based classification is performed during change extraction by setting *re-interpretation: true* in the *set_up.yml* file for change extraction (See [Change Extraction](#change-extraction)).

Rule-based classifiers can be found in *wikidata-edit-history/classifiers/rule/rule_based_classifier.py*.

### Classification of remaining changes (Text and Entity)

We provide an example for the *classifier_setup.yml* in *classifier_setup.example.yml*. Make a copy of this file and configure it accordingly.

**Before running the classification, perform the following steps:**
1. Rename the table *updates_text* to *updates_text_full*
2. Create the following 2 tables:
```
  create table updates_text_latin as
  select *
  from updates_text_full
  where old_value->>0 ~ '[^\u0000-\u036F\u1E00-\u1EFF\u2000-\u206F\u2070-\u218F]' OR
  new_value->>0 ~ '[^\u0000-\u036F\u1E00-\u1EFF\u2000-\u206F\u2070-\u218F]';

  create table updates_text as
  select *
  from updates_text_full
  where not (old_value->>0 ~ '[^\u0000-\u036F\u1E00-\u1EFF\u2000-\u206F\u2070-\u218F]' OR
  new_value->>0 ~ '[^\u0000-\u036F\u1E00-\u1EFF\u2000-\u206F\u2070-\u218F]');
```
3. Run the update of labels and descriptions for entity. For this, run extract_remaining_changes.py with the following parameters in the classifier_setup.yml

```
classification_ml:
  classify: false     <--------
  evaluate: false     <--------
  table_suffix: ''     <--- set this according to the specific table suffix (e.g., '', '_less', '_sa', '_ao')
  train: false         <--------
config:
  classifier_type: ml     <--------
  db_config_path: config/aux_db_config.json
update_entity_labels_descriptions: true    <--------
```

4. Rename *updates_entity* to *updates_entity_full*
5. Create the following 2 tables:
```
  create table updates_entity_latin as
  select *
  from updates_entity_full
  where (old_value_label = '' OR old_value_label IS NULL) OR
            (new_value_label = '' OR new_value_label IS NULL) OR
            old_value_label ~ '[^\u0000-\u036F\u1E00-\u1EFF\u2000-\u206F\u2070-\u218F]' OR
            new_value_label ~ '[^\u0000-\u036F\u1E00-\u1EFF\u2000-\u206F\u2070-\u218F]';

  create table updates_entity as
  select *
  from updates_entity_full
  where not ((old_value_label = '' OR old_value_label IS NULL) OR
            (new_value_label = '' OR new_value_label IS NULL) OR
            old_value_label ~ '[^\u0000-\u036F\u1E00-\u1EFF\u2000-\u206F\u2070-\u218F]' OR
            new_value_label ~ '[^\u0000-\u036F\u1E00-\u1EFF\u2000-\u206F\u2070-\u218F]');
```

1. Download Transitive closure cache from [WiDiff: Wikidata Entities Transitive Closures (October 2025)](https://doi.org/10.5281/zenodo.22203191) and store it in *auxiliary_data/transitive_closures*. If not, create a new one following the steps in [Downloading extra data](#downloading-extra-data) and [Transitive Closure Cache Creation](#transitive-closure-cache-creation)
2. Download the trained ML classifiers from [Wikidata Change Classification models and features]() and put both *training_info* and *features* folder under *classifiers/ml/*.
3. Set the following parameters in *wikidata-edit-history/classifier_setup.yml*:
````
  config:
    db_config_path: path_to_db_with_changes  <-----
    classifier_type: ml   <-----
  classification_ml:
    train: false          <-----
    classify: true        <-----
    evaluate: false       <-----
    table_suffix: ''      <----- change for the corresponding table suffix (See Database schema and change extraction filters)
  update_entity_labels_descriptions: false  <-----
````

**NOTE:** db_config_path is the path to the database that stores the changes. The config.json has the same structure as the one set for change extraction, can use that one as is.

4. Run:

```bash
python3 -m classify_remaining_changes
```

this command classifies entity changes using rule-based first, and then classifies the remaining entity and text changes using the trained ML model.

---

#### ML model training

**NOTE:** The trained ML models and features can be found in [Wikidata Change Classification models and features]().

We provide an example configuration file in *classifiers/ml/config/ml_classifier_config.example.json*.
Configure its path in *classifier_setup.yml* under *config -> ml_config_path* accordingly.

**Configuration**

Training is performed doing 5-fold cross validation with a 3-fold cross validation for Grid Search per outer fold. The number of folds (5) can be changed in *classifiers/ml/config/ml_classifier_config.json*

Additionally, since we want to guarantee that every change has a label assigned and multi-label classifiers return probabilities for each class, we assign all labels to a change where prob >= 0.5, If no probability reaches this threshold, we take the one with the maximum probability. This threshold can be modified in *classifiers/ml/config/ml_classifier_config.json*.

Finally, we use *random_state = 42* so results are reproducible (also set in *classifiers/ml/config/ml_classifier_config.json*).

**Output**

Training outputs *training_info_<model_name>.pkl* files with the following structure:

`````bash
    {
        "datatype": {
            'results_folds': [], # results per fold
            'micro_averages': {
                'datatype': {
                    'precision': float,
                    'recall': float,
                    'accuracy': float,
                    'f1': float
                }
            } 
    }

    # results per fold:
    {
        'classifier': string, # kn, xgboost, random_forest, gradient_boosting
        'fold': int, # 0-4
        'scaler': sklearn_scaler_for_fold,
        'metrics_results': { # macro average
            'label': { 
                'precision': float,
                'recall': float,
                'accuracy': float,
                'f1': float
            }
        },
        'model': clf, # if the base_model doesn't support MultiOutput classification, we send it through MultiOutputClassifier. If not base_model == model
        'base_model': model, # base model (GradientBoosting, RandomForest, XGBoost, KNN)
        'features': feature_cols, 
        'label_binarizer': label_binarizer,
        'best_params': best_params_from_grid_search
    }
`````

**Re-training**

1. To re-train models, set the following parameters in *wikidata-edit-history/classifier_setup.yml*:
  ```
    config:
      classifier_type: ml
    classification_ml:
      train: true
      classify: false
      evaluate: true
  ```

**Inter annotator agreement**
The shared samples labeled by 2 other extra annotators and compute of IAA can be found in *wikidata-edit-history/classifiers/ml/training_dataset/shared_overlap*. The notebook *inter_annotator_agreement* calculates the IAA.

---

#### LLM baseline
1. Configure LLM in *classifiers/llm/config/llm_classifier_config.json*. To use Qwen 3.5 (FP8 quantized), run the script *classifiers/llm/qwen_server.sh* in the background and set the corresponding `base_url` in the configuration file (*classifiers/llm/config/llm_classifier_config.json*). 
2. Set `classifier_type: llm` and `llm_config_path` under *config* in  *wikidata-edit-history/classifier_setup.yml*. 
3. Run `python3 -m classifiy_remaining_changes`. This classifies changes on the labeled dataset (*classifiers/ml/training_dataset*). To modify the changes to label, set the `[classification_llm][path_to_entity_changes]` and `[classification_llm][path_to_text_changes]` in  *wikidata-edit-history/classifier_setup.yml* to other files.

*Note:* We used 2 40GB VRAM GPUs and that's why `--tensor-parallel-size 2` is used in *classifiers/llm/qwen_server.sh*. If running on a single GPU, then this should be removed

---

## Analysis
All analysis scripts can be found in *wikidata-edit-history/analysis/sql*.

- To compute change distributions use the scripts: *qualifier_change_stats.sql*, *reference_change_stats.sql*, *rank_change_stats.sql*, *dist_change_types.sql* 
- Using Change Types section: *using_change_types.sql*
- Plot can be replicated by running *plots.py*.

## Downloading extra data

All files needed for this step are in the folder `/wdtk` of this repository.

### Prerequisites

- Java 17+ (tested with OpenJDK 17.0.14)
- Maven 3.9+ (tested with Apache Maven 3.9.12)

### Overview

We use the [Wikidata Toolkit](https://github.com/Wikidata-Toolkit/Wikidata-Toolkit) to extract additional data from a Wikidata JSON dump.

We provide the extracted data in [WiDiff: Wikidata Entities Transitive Closures (October 2025)](https://doi.org/10.5281/zenodo.22203191). To extract new data, follow the steps below.

Three extraction classes are provided:

| Class | Description |
|---|---|
| `ExtractTransitiveClosure` | Extracts transitive closures for `subclass of`, `has part(s)`, `part of`, and `located in` |

### Output Files

**`ExtractTransitiveClosure`** (up to 10 hops, columns: `entity_id`, `entity_id_numeric`, `transitive_closure_qids`, `transitive_closure_numeric_ids`)
- `subclass_of_transitive.csv` — transitive closure of `subclass of (P279)`
- `part_of_transitive.csv` — transitive closure of `part of (P361)`
- `has_parts_transitive.csv` — transitive closure of `has part(s) (P527)`
- `located_in_transitive.csv` — transitive closure of `located in (P131)`

### Setup and Execution
**1. Clone the Wikidata Toolkit**
```bash
git clone https://github.com/Wikidata-Toolkit/Wikidata-Toolkit
```

**2. Download a Wikidata dump**

Download a `latest-all.json.bz2` dump and place it in:
```
Wikidata-Toolkit/dumpfiles/wikidatawiki/json-YYYYMMdd/
```
where `YYYYMMdd` is the dump date. The toolkit expects this exact folder structure and a `.json.bz2` format.

Example: *Wikidata-Toolkit/dumpfiles/wikidatawiki/json-20252018/wikidata-20251018-all.json.bz2*

**3. Add the extraction files**

Copy `ExtractTransitiveClosure.java`, and `config.properties` from `wdtk/` into:
```
Wikidata-Toolkit/wdtk-examples/src/main/java/org/wikidata/wdtk/examples/
```

**4. Replace the `pom.xml` files**

Copy the provided `pom.xml` files into the Wikidata Toolkit directory, replacing the existing ones:

- `wdtk/pom.xml` → `Wikidata-Toolkit/pom.xml`
- `wdtk/wdtk-examples-pom.xml` → `Wikidata-Toolkit/wdtk-examples/pom.xml`

**Note:** Before running a different extraction class, update the `<mainClass>` field in `Wikidata-Toolkit/wdtk-examples/pom.xml`:
```xml
<mainClass>org.wikidata.wdtk.examples.CLASS_NAME</mainClass>
```

**5. Enable offline mode** *(skip if you want the toolkit to download the dump itself)*

In `Wikidata-Toolkit/wdtk-examples/src/main/java/org/wikidata/wdtk/ExampleHelpers.java`, change:
```java
public static final boolean OFFLINE_MODE = false;
```
to:
```java
public static final boolean OFFLINE_MODE = true;
```

**6. Configure `config.properties`**

Copy `config.properties` file to `Wikidata-Toolkit` root.

| Parameter | Description |
|---|---|
| `dump_path` | Path to the `.json.bz2` dump file |
| `output_dir` | Path to the directory where output files will be stored |
| `language` | Language code for labels, descriptions, and aliases (e.g., `en`) |


**7. Configure and run the bash script**

Set the following parameters in `extract_extra_data.bash`:

| Parameter | Description |
|---|---|
| `WORK_DIR` | Path to the cloned Wikidata Toolkit directory |
| `JAR_FILE` | Path to the built JAR (default: `$WORK_DIR/wdtk-examples/target/wdtk-examples-0.17.1-SNAPSHOT.jar`) |
| `MAX_HEAP` | Maximum JVM heap size (e.g., `140G`) |
| `INIT_HEAP` | Initial JVM heap size (e.g., `140G`) |
| `GC_THREADS` | Number of parallel GC threads |
| `CONCURRENT_GC_THREADS` | Number of concurrent GC threads |

Then run:
```bash
bash extract_extra_data.bash
```

## Transitive Closure Cache Creation
The transitive closure cache is required for ML-based change classification. It loads the transitive closure CSV files produced by `ExtractTransitiveClosure.java` into memory and serializes them as a pickle file for fast access during feature computation.

**Set the following parameters in `set_up.yml` under `transitive_closure_cache`:**
- *subclass_transitive_path:* path to the .csv file with the transitive closures for subclass of
- *part_of_transitive_path:* path to the .csv file with the transitive closures for part of
- *has_part_transitive_path:* path to the .csv file with the transitive closures for has parts
- *located_in_transitive_path:* path to the .csv file with the transitive closures for located in
- *transitive_closure_pickle_file_path:* file path to the transitive closure cache pickle file
- *transitive_closure_stats_pickle_file_path:* file path to transitive closure cache stats pickle file (size, time of construction)

To create the cache, run:

```bash
from classifiers.rule.transitive_closure_cache import TransitiveClosureCache
cache = TransitiveClosureCache()
```

**Note:** Cache creation is slow and memory-intensive (the full cache can reach several GB). It only needs to be run once — subsequent runs load directly from the pickle file.
