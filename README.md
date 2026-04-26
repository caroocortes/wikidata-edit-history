# WiDiff - Change Extraction and Exploration in Wikidata

This tool extracts changes (diff between revisions) of statement values, ranks, qualifiers, and references, from WD's xml dumps and stores them in a relational DB.

## Project structure
```bash
├── config/           # Configuration files
├── data/           # Auxiliary datasets needed during parsing (property_labels.csv, subclassof_astronomical_objects.csv, subclassof_scholarly_articles.csv)
├── download/       # Script for downloading XML files + list of download links for the dump of 20250601
├── logs/       # Log folder with logs of change extraction
├── scripts/        # Core parsing classes 
│   ├── file_parser.py              # Processes XML files, extracts pages
│   ├── page_parser.py              # Processes a page (all edit history for an entity)
│   ├── utils.py                    # Auxiliary methods 
│   ├── const.py                    # Constants
|   ├── feature_creation.py         # Creates features for change classification
|   ├── compute_remaining_features.py # Creates features that weren't calculated during change extraction (e.g., embedding-based features)
|   ├── transitive_closure_cache.py # Creates a cache from transitive closures for fast access
│   └── db_writer.py # in charge of storing changes in the DB
├── sql/        # stores .sql schema of DB
└── wdtk/           # Files needed to extract extra data from a WD full dump (uses WD Toolkit)
```

## Change Extraction

### Prerequisites

- Python 3.11.9
- Install required libraries listed in `requirements.txt`.
- Update the following files:
    - data/subclassof_astronomical_objects.csv
    - data/subclassof_scholarly_articles.csv
    - data/property_labels.csv
using the queries in `data/sparql_queries.txt` against [Wikidata's query service](https://query.wikidata.org/), or [QLever](https://qlever.dev/wikidata/)
- Download dump files from Wikidata's dump service. The folder `download/` contains a script to download files from the list of files in `download/xml_download_links.txt`.

### Configuration (`set_up.yml`)

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

**NOTE:** The DB needs to be created beforehand. The schema is created by the pipeline.

---

#### `change_extraction_processing`
Controls how the change extraction pipeline runs.

| Parameter | Description |
|---|---|
| `language` | Language code for extracting labels and descriptions (e.g., `en`) |
| `files_in_parallel` | Number of dump files processed in parallel |
| `pages_in_parallel` | Number of pages processed in parallel within a file |
| `files_directory` | Path to the directory containing the Wikidata dump files (xml.bz2) |
| `memory_consumption_monitoring` | If `true`, logs memory usage during processing |
| `page_queue_size` | Maximum number of pages held in the queue of `file_parser.py` |
| `db_batch_size` | Number of revisions inserted per database batch |
| `db_max_queue_size` | Maximum number of elems held in the queue of `db_writer.py` |

---

#### `change_extraction_filters`
Controls which entity types are extracted and processed. Each filter has the following fields:

| Field | Description |
|---|---|
| `extract` | If `true`, changes for this entity type are extracted |
| `feature_extraction` | If `true`, ML features for change classification are computed for this entity type |
| `datatype_metadata_extraction` | If `true`, datatype metadata changes are extracted |

Available filters:

- **`scholarly_articles_filter`**: Entities classified as scholarly articles (Q13442814)
- **`astronomical_objects_filter`**: Entities classified as astronomical objects (Q6999)
- **`less_filter`**: Entities with fewer than `threshold` changes — used to exclude low-activity entities. `threshold` can be set in `set_up.yml`
- **`rest`**: All remaining entities not matched by the above filters. This entities are extracted by default.

---

#### `reverted_edit_tagging`
Controls revert edit tagging during change extraction.

| Parameter | Description |
|---|---|
| `time_threshold_seconds` | Maximum time window (in seconds) within which an edit can be considered reverted. Default is `2419200` (4 weeks) |

---

#### `re_interpretation`
If `true`, performsn the re-interpretation step, tagging soft deletions, soft insertions, value updates (for updates between values of different datatypes).

---

#### `update_entity_labels_descriptions`
If `true`, updates entity labels and descriptions the table `features_entity{suffix}`

## Running WiDiff

Activate your Python environment with the dependencies from requirements.txt before running the script.

Run the parser with the following command:

```bash
python3 -m main [options]
```

**Options:**

`-f FILE`: Path to .xml.bz2 file to process (for single file processing).
`-n NUM_FILES`: maximum number of files to process on a run.

*Note:* Processing each file takes approximately 1 hour.

Alternatively, use the provided `run_parser.sh` script to process a maximum of `NUM_FILES` files (Activate environment with requirements.txt beforehand):

```bash
chmod +x run_parser.sh
./run_parser.sh <NUM_FILES> &
```

*Note:* `run_parser.sh` runs `main.py` with the configuration set in `setup.yml` until `NUM_FILES` files have been processed.

### Parallelization
By default, main.py uses the following parallelization strategy:
- Creates *files_in_parallel* processes (from set_up.yml) that call FileParser (*file_parser.py*)
- Each FileParser creates *pages_in_parallel* processes (from set_up.yml) to call PageParser (*page_parser.py*) which processes a page (all revisions for an entity).
- Creates a dedicated process for storing changes while they wait for batch insertion into the DB (*db_writer.py*)

The system must support at least *files_in_parallel* × *pages_in_parallel* cores + 1 (for the *db_writer*).

Additionally, `file_parser.py` uses `bz2.open(file_path, 'rb')`, therefore, appropriate amount of memory needs to be reserved for processing files. 

![architecture diagram](arch_diagram/parser_arch.svg)

### Output Files
The pipeline generates three output files:

- `processed_files.txt`: List of processed files (for tracking)
- `parser_output.log`: Logs from file_parser and page_parser
- `parser_log_files.json`: Summary with file size in MB, number of entities, number of processed revisions, avg. revisions per entity, time to read file (secs), total time to process file (secs), peak memory in MB (if `memory_consumption_monitoring: true` in set_up.yml)

## Downloading extra data

All files needed for this step are in the folder `/wdtk` of this repository.

### Prerequisites

- Java 17+ (tested with OpenJDK 17.0.14)
- Maven 3.9+ (tested with Apache Maven 3.9.12)

### Overview

We use the [Wikidata Toolkit](https://github.com/Wikidata-Toolkit/Wikidata-Toolkit) to extract additional data from a Wikidata JSON dump.

We provide the extracted data in [WiDiff: Wikidata Entity Labels, Descriptions and Alias, Types (P31 and P279), and Transitive Closures (June 2025)](https://doi.org/10.5281/zenodo.19771721). To extract new data, follow the steps below.

Three extraction classes are provided:

| Class | Description |
|---|---|
| `ExtractLabelsProperties` | Extracts entity labels, aliases, descriptions, and property labels |
| `ExtractInstanceOfSubclassOf` | Extracts `instance of (P31)` and `subclass of (P279)` for every entity |
| `ExtractTransitiveClosure` | Extracts transitive closures for `subclass of`, `has part(s)`, `part of`, and `located in` |

### Output Files

**`ExtractLabelsProperties`**
- `entity_labels_alias_description.csv` — label, first alias, and description for each entity. Columns: `qid`, `numeric_id`, `label`, `alias`, `description`
- `property_labels.csv` — label for each property. Columns: `property_id`, `numeric_id`, `property_label`

**`ExtractInstanceOfSubclassOf`**
- `p31_entity_types.csv` — extracted from `<Q-id, P31, Q-id>`. Columns: `entity`, `entity_numeric_id`, `entity_type (Q-id)`, `entity_type_numeric_id`
- `p279_entity_types.csv` — extracted from `<Q-id, P279, Q-id>`. Columns: `entity`, `entity_numeric_id`, `entity_type (Q-id)`, `entity_type_numeric_id`

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

Copy `ExtractLabelsProperties.java`, `ExtractTransitiveClosure.java`, `ExtractInstanceOfSubclassOf.java`, and `config.properties` from `wdtk/` into:
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

## Database schema

The database is organized into two groups of tables: **change tables**, which store the extracted changes, and **feature tables**, which store the features computed for change classification.

Given the amount of data on Wikidata, the most tables contain "redundant data" for query performance or to simplify aggregations (e.g., tables with a timestamp column contain columns with the week, year_moth and year of the timestamp for aggregations on different time levels).

### Change Tables

**`revision`** — one row per revision, storing metadata about each edit: the editor (user ID, username, type), timestamp, and a reference to the entity that was edited.

| Column | Description |
|---|---|
| prev_revision_id | ID of the previous revision |
| revision_id | ID of the revision |
| entity_id | ID of the entity |
| entity_label | Entity label. Extracted as the last one. |
| file_path | XML file name where this revision is stored |
| timestamp | Timestamp of the revision |
| week | Week of the timestamp of the revision |
| year_month | Year and Month (YYYY-MM) of the timestamp of the revision |
| year | Year of the timestamp of the revision |
| user_id | ID of the user that made the edit |
| username | Username of the user that made the edit |
| user_type | User type. Can be "human" (for registered users), "bot" or "anonymous" |
| comment | Comment on the revision |
| redirect | If *true* the revision is a redirect |
| q_id_redirect | Numeric part of the Q-id of the entity where the current entity is redirected to (e.g., if Q1 is redirected to Q123, then q_id_redirect holds the value 123) |

*Primary key:* revision_id

**`value_change`** — stores changes to statement values, including creations, deletions, and updates. Each row records the old and new value, the action performed, and whether the edit was reverted or is itself a reversion. The `change_target` field distinguishes between changes to the main value, a qualifier, the rank, or datatype metadata.

| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| change_target | Indicates what is being modified. If '' then old_value and new_value correspond to a property value. If 'rank' then old_value and new_value correspond to a change in the rank of a statement. If 'language' then old_value and new_value correspond to a change in the language of a monolingualtext value. |
| action | Indicates the action performed: CREATE, UPDATE or DELETE |
| target | Indicates what is being changed: PROPERTY, RANK, PROPERTY_DATATYPE_METADATA |
| old_hash | Hash of the old value. This is constructed from the JSON of the value |
| new_hash | Hash of the new value. This is constructed from the JSON of the value |
| timestamp | Timestamp of the revision |
| week | Week of the timestamp of the revision |
| year_month | Year and Month (YYYY-MM) of the timestamp of the revision |
| year | Year of the timestamp of the revision |
| label | Label of change classification. Can contain the values: soft_insertion, soft_deletion, statement_insertion, statement_deletion, soft_insertion, soft_deletion |
| entity_id | ID of the entity |
| is_reverted | 1 if the edit is reverted, 0 otherwise |
| reversion | 1 if the edit does a reversion, 0 otherwise |
| reversion_timestamp | Timestamp of the edit that does the reversion. This column holds a value only if is_reverted = 1, otherwise it's NULL |
| revision_id_reversion | revision_id of the edit that does the reversion. This column holds a value only if is_reverted = 1, otherwise it's NULL |
| entity_label | Entity label. Extracted as the last one. |
    
*Primary key:* (revision_id, property_id, value_id, change_target)
*Foreign key:* (revision_id) references revision(revision_id).

**`qualifier_change`** — stores additions and deletions of qualifier values. Since qualifiers lack unique identifiers, only CREATE and DELETE actions are tracked (no UPDATE). Values are identified by a hash of their content.

| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| qual_property_id | ID of the reference property |
| qual_property_label | Label of the reference property |
| value_hash | Hash computed from the property value. This hash + qual_property_id identify each statement value |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| change_target | Indicates what is being modified. Always '' in this table. |
| action | Indicates the action performed: CREATE or DELETE |
| target | Indicates what is being changed: REFERENCE |
| old_hash | Hash of the old value. This is constructed from the JSON of the value |
| new_hash | Hash of the new value. This is constructed from the JSON of the value |
| timestamp | Timestamp of the revision |
| week | Week of the timestamp of the revision |
| year_month | Year and Month (YYYY-MM) of the timestamp of the revision |
| year | Year of the timestamp of the revision |
| label | Label of change classification. Can contain the values: qualifier_insertion, qualifier_deletion, soft_deletion |
| entity_id | ID of the entity |
| entity_label | Entity label. Extracted as the last one. |

*Primary key:* (revision_id, property_id, value_id, qual_property_id, value_hash, change_target)
*Foreign key:* (revision_id) references revision(revision_id).
*Note:* (revision_id, property_id, value_id, change_target) does not necessarily exist in value_change since a revision could involve only qualifier changes

**`reference_change`** — stores additions and deletions of reference values, following the same approach as `qualifier_change`. Each row is additionally identified by a reference hash (`ref_hash`), which identifies the reference group the value belongs to.

| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| ref_property_id | ID of the reference property |
| ref_property_label | Label of the reference property |
| ref_hash | Hash computed from all the statement values in the reference. Identifies the reference (a reference is composed of multiple property - values) |
| value_hash | Hash computed from the property value. This hash + ref_property_id identify each statement value inside the reference |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| change_target | Indicates what is being modified. Always '' in this table. |
| action | Indicates the action performed: CREATE or DELETE |
| target | Indicates what is being changed: REFERENCE |
| old_hash | Hash of the old value. This is constructed from the JSON of the value |
| new_hash | Hash of the new value. This is constructed from the JSON of the value |
| timestamp | Timestamp of the revision |
| week | Week of the timestamp of the revision |
| year_month | Year and Month (YYYY-MM) of the timestamp of the revision |
| year | Year of the timestamp of the revision |
| label | Label of change classification. Can contain the values: reference_insertion, reference_deletion |
| entity_id | ID of the entity |
| entity_label | Entity label. Extracted as the last one. |

*Primary key:* (revision_id, property_id, value_id, ref_hash, ref_property_id, value_hash, change_target)
*Foreign key:* (revision_id) references revision(revision_id).
*Note:* (revision_id, property_id, value_id, change_target) does not necessarily exist in value_change since a revision could involve only reference changes

**`datatype_metadata_change`** — stores changes to datatype-specific metadata fields (e.g., `upperBound` for quantity values). These are tracked separately from the main value change.

| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| ref_property_id | ID of the reference property |
| ref_property_label | Label of the reference property |
| ref_hash | Hash computed from all the statement values in the reference. Identifies the reference (a reference is composed of multiple property - values) |
| value_hash | Hash computed from the property value. This hash + ref_property_id identify each statement value inside the reference |
| old_value | Old value of the datatype metadata |
| new_value | New value of the datatype metadata (e.g., if the 'unit' for a quantity value changes from *square metre (Q25343)* to *metre (Q11573)*, then old_value will have *Q25343* and new_value will have *Q11573*) |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| change_target | Name of datatype metadata (e.g. 'upperBound' for a quantity value) |
| action | Indicates the action performed: CREATE or DELETE |
| target | Indicates what is being changed: REFERENCE |
| old_hash | Hash of the old value. This is constructed from the JSON of the value |
| new_hash | Hash of the new value. This is constructed from the JSON of the value |
| timestamp | Timestamp of the revision |
| week | Week of the timestamp of the revision |
| year_month | Year and Month (YYYY-MM) of the timestamp of the revision |
| year | Year of the timestamp of the revision |
| label | Label of change classification. Can contain the values: reference_insertion, reference_deletion |
| entity_id | ID of the entity |
| entity_label | Entity label. Extracted as the last one. |

*Primary key:* (revision_id, property_id, value_id, change_target)
*Foreign key:* (revision_id) references revision(revision_id).

**`entity_stats`** — one row per entity, aggregating counts of all change types, user types, reverted edits, and processing times. Useful for entity-level analysis without querying the full change tables.

| Column | Description |
|---|---|
| entity_id | ID of the entity |
| entity_label | Entity label. Extracted as the last one. |
| entity_types_31 | List of Q-ids, corresponding to the last P31 values of the entity |
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

### Feature Tables

One feature table per datatype: `features_text`, `features_quantity`, `features_time`, `features_entity`, `features_globecoordinate`. Each table stores the features computed for change classification, referencing the corresponding row in `value_change`. Features are datatype-specific.

**`features_time`**
| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| change_target | Indicates what is being modified. Always '' in this table. |
| entity_label | Entity label. Extracted as the last one. |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| action | Indicates the action performed: CREATE, UPDATE or DELETE |
| date_diff_days | Difference between old_value and new_value in days |
| sign_change | 1 if there's a sign change, 0 otherwise |
| change_one_to_zero | 1 if there's any of these cases happen: YYYY-01-01 -> YYYY-00-00, YYYY-MM-01 -> YYYY-MM-00, YYYY-01-00 -> YYYY-00-00; 0 otherwise|
| day_added | 1 if the day was added, 0 otherwise |
| day_removed | 1 if the day was removed, 0 otherwise |
| month_added | 1 if the day was added, 0 otherwise |
| month_removed | 1 if the month was removed, 0 otherwise |
| different_year | 1 if there's a year change, 0 otherwise |
| different_month | 1 if there's a month change, 0 otherwise |
| different_day | 1 if there's a day change, 0 otherwise |
| label | Change classification label. Can be: *re_formatting*, *property_value_update*, *refinement* or *unrefinement* |

*Primary key:* (revision_id, property_id, value_id, change_target)
*Foreign key:* (revision_id, property_id, value_id, change_target) references value_change(revision_id, property_id, value_id, change_target)

**`features_quantity`**
| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| change_target | Indicates what is being modified. Always '' in this table. |
| entity_label | Entity label. Extracted as the last one. |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| action | Indicates the action performed: CREATE, UPDATE or DELETE |
| sign_change | 1 if there's a sign change, 0 otherwise |
| precision_change | 1 if there's a precision change, 0 otherwise |
| length_increase | 1 if there's a length increase, 0 otherwise |
| length_decrease | 1 if there's a length decrease, 0 otherwise |
| whole_number_change | 1 if the whole number changes, 0 otherwise |
| old_is_prefix_of_new | 1 if the old_value is a prefix of the new_value, 0 otherwise |
| new_is_prefix_of_old | 1 if the new_value is a prefix of the old_value, 0 otherwise |
| same_float_value | 1 if the new_value and old_value represent the same float value, 0 otherwise |
| label | Change classification label. Can be: *re_formatting*, *property_value_update*, *refinement* or *unrefinement* |

*Primary key:* (revision_id, property_id, value_id, change_target)
*Foreign key:* (revision_id, property_id, value_id, change_target) references value_change(revision_id, property_id, value_id, change_target)

**`features_globecoordinate`**
| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| change_target | Indicates what is being modified. Always '' in this table. |
| entity_label | Entity label. Extracted as the last one. |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| action | Indicates the action performed: CREATE, UPDATE or DELETE |
| latitude_sign_change | 1 if there's a sign change, 0 otherwise |
| longitude_sign_change | 1 if there's a sign change, 0 otherwise |
| latitude_whole_number_change | 1 if the whole number changes, 0 otherwise |
| longitude_whole_number_change | 1 if the whole number changes, 0 otherwise |
| latitude_precision_change | 1 if there's a precision change, 0 otherwise |
| longitude_precision_change | 1 if there's a precision change, 0 otherwise |
| latitude_length_increase | 1 if there's a length increase, 0 otherwise |
| latitude_length_decrease | 1 if there's a length decrease, 0 otherwise |
| longitude_length_increase | 1 if there's a length increase, 0 otherwise |
| longitude_length_decrease | 1 if there's a length decrease, 0 otherwise |
| latitude_old_is_prefix_of_new | 1 if the old_value is a prefix of the new_value, 0 otherwise |
| latitude_new_is_prefix_of_old | 1 if the new_value is a prefix of the old_value, 0 otherwise |
| latitude_same_float_value | 1 if the new_value and old_value represent the same float value, 0 otherwise |
| longitude_old_is_prefix_of_new | 1 if the old_value is a prefix of the new_value, 0 otherwise |
| longitude_new_is_prefix_of_old | 1 if the new_value is a prefix of the old_value, 0 otherwise |
| longitude_same_float_value | 1 if the new_value and old_value represent the same float value, 0 otherwise |
| label_latitude | Change classification label. Can be: *re_formatting*, *property_value_update*, *refinement* or *unrefinement* |
| label_longitude | Change classification label. Can be: *re_formatting*, *property_value_update*, *refinement* or *unrefinement* |

*Primary key:* (revision_id, property_id, value_id, change_target)
*Foreign key:* (revision_id, property_id, value_id, change_target) references value_change(revision_id, property_id, value_id, change_target)

**`features_text`**
| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| change_target | Indicates what is being modified. Always '' in this table. |
| entity_label | Entity label. Extracted as the last one. |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| action | Indicates the action performed: CREATE, UPDATE or DELETE |
| length_diff_abs | Absolute length difference between old_value and new_value |
| token_count_old | Number of words (token) in old_value |
| token_count_new | Number of words (token) in new_value |
| token_overlap | Percentage of word overlap between old_value and new_value |
| old_in_new | 1 if old_value is contained new_value |
| new_in_old | 1 if new_value is contained old_value |
| levenshtein_distance | Levenshtein distance between old_value and new_value |
| edit_distance_ratio |  levenshtein_distance / max(len(old_value), len(new_value)) |
| complete_replacement | 1 if (token_overlap == 0 & old_in_new == 0 & new_in_old == 0), otherwise 0 |
| same_value_without_special_char | 1 if old_value == new_value after removing all special characters ([^a-zA-Z0-9]) |
| special_char_count_diff | Difference between number of special characters in old_value and new_value |
| char_insertions | Number of character insertions |
| char_deletions | Number of character deletions |
| char_substitutions | Number of character substitutions |
| adjacent_char_swap | 1 if there's an adjacent character swap, otherwise 0 |
| has_significant_prefix | 1 if there's a significant prefix share between old_value and new_value (the length of the prefix is >= 3), otherwise 0 |
| has_significant_suffix | 1 if there's a significant suffix share between old_value and new_value (the length of the prefix is >= 3), otherwise 0 |
| value_cosine_similarity | cosine similarity between embeddings of old_value and new_value |
| label | Change classification label. Can be: *re_formatting*, *textual_change*, *property_value_update*, *refinement* or *unrefinement* |

*Primary key:* (revision_id, property_id, value_id, change_target)
*Foreign key:* (revision_id, property_id, value_id, change_target) references value_change(revision_id, property_id, value_id, change_target)

**`features_entity`**
| Column | Description |
|---|---|
| revision_id | ID of the revision |
| property_id | ID of the property |
| property_label | label of the property |
| value_id | ID of the property value |
| change_target | Indicates what is being modified. Always '' in this table. |
| entity_label | Entity label. Extracted as the last one. |
| old_value | Old value of the property value |
| new_value | New value of the property value |
| old_datatype | Old datatype of the property value |
| new_datatype | New datatype of the property value |
| action | Indicates the action performed: CREATE, UPDATE or DELETE |
| token_overlap | Percentage of word overlap between old_value and new_value |
| old_in_new | 1 if old_value is contained new_value |
| new_in_old | 1 if new_value is contained old_value |
| edit_distance_ratio |  levenshtein_distance / max(len(old_value), len(new_value)) |
| complete_replacement | 1 if (token_overlap == 0 & old_in_new == 0 & new_in_old == 0), otherwise 0 |
| is_link_change | 1 if old_value != new_value and old_value_label == new_value_label, otherwise 0 |
| label_cosine_similarity | cosine similarity between embeddings of the labels of old_value and new_value |
| description_cosine_similarity | cosine similarity between embeddings of the descriptions of old_value and new_value |
| old_value_subclass_new_value | 1 if old_value is subclass of new_value, 0 otherwise |
| new_value_subclass_old_value | 1 if new_value is subclass of old_value, 0 otherwise |
| old_value_located_in_new_value | 1 if old_value is located in new_value, 0 otherwise |
| new_value_located_in_old_value | 1 if new_value is located in old_value, 0 otherwise |
| old_value_has_parts_new_value | 1 if old_value is has parts new_value, 0 otherwise |
| new_value_has_parts_old_value | 1 if new_value is has parts old_value, 0 otherwise |
| old_value_part_of_new_value | 1 if old_value is part of new_value, 0 otherwise |
| new_value_part_of_old_value | 1 if new_value is part of old_value, 0 otherwise |
| label | Change classification label. Can be: *re_formatting*, *link_change*, *property_value_update*, *refinement* or *unrefinement* |

*Primary key:* (revision_id, property_id, value_id, change_target)
*Foreign key:* (revision_id, property_id, value_id, change_target) references value_change(revision_id, property_id, value_id, change_target)

**Note:** All table names include a `{suffix}` placeholder, which is replaced at runtime for the different filters of entity types in `set_up.yml`. The values for this suffix can be: `_sa` (scholarly articles), `_ao` (astronomical objects), `_less` (entities with less than *threshold* value changes)

## Transitive Closure Cache Creation

The transitive closure cache is required for ML-based change classification. It loads the transitive closure CSV files produced by `ExtractTransitiveClosure.java` into memory and serializes them as a pickle file for fast access during feature computation.

Set the following parameters in `set_up.yml` under `transitive_closure_cache`:
- *subclass_transitive_path:* path to the .csv file with the transitive closures for subclass of
- *part_of_transitive_path:* path to the .csv file with the transitive closures for part of
- *has_part_transitive_path:* path to the .csv file with the transitive closures for has parts
- *located_in_transitive_path:* path to the .csv file with the transitive closures for located in
- *transitive_closure_pickle_file_path:* file path to the transitive closure cache pickle file
- *transitive_closure_stats_pickle_file_path:* file path to transitive closure cache stats pickle file (size, time of construction)

To create the cache, run:

```bash
from scripts.transitive_closure_cache import TransitiveClosure
cache = TransitiveClosure()
```

**Note:** Cache creation is slow and memory-intensive (the full cache can reach several GB). It only needs to be run once — subsequent runs load directly from the pickle file.

---

## Compute Remaining Features
Some features cannot be computed during change extraction and must be calculated in a separate step. This currently includes embedding-based similarity and transitive closure features, which require a language model or the transitive closure cache and are too expensive to compute inline during parsing.

Therefore, the transitive closures must be extracted before running this step. The cache doesn't need to be created beforehand (if it's not created it will be created, but this takes sometime).

This step uses the *all-MiniLM-L6-v2* sentence transformer model, which is downloaded automatically on first run. A GPU is not required but significantly speeds up computation. If a CUDA-compatible GPU is available it will be used automatically, otherwise the script falls back to CPU (`device = "cuda" if torch.cuda.is_available() else "cpu"`).

For embedding-based features for entity changes, the labels and descriptions of `old_value` and `new_value` must be added. For this, enable *update_entity_labels_descriptions* to true in `set_up.yml` before running this script. Note that this script assumers there exists a table named `entity_labels_alias_description` in the database with the following schema (qid, numeric_id, label, alias, description) (*entity_labels_alias_description.csv* extracted with the Wikidata-Toolkit).

*Note:* The remaining features can be computed for the different tables, call the script with the corresponding table_suffix ('ao', 'sa', 'rest', 'less').

To compute the remaining features, run from root:

```bash
python3 -m scripts.compute_remaining_features --table_suffix rest
```

This script reads from the `features_text` and `features_entity` tables in the database and writes the computed values back. It must be run after change extraction with `feature_extraction: true` and before running the ML classifier.

## Descriptive Analysis

Descriptive analysis scripts are provided in `analysis/scripts.py`. Each analysis can be enabled and configured independently in `setup.yml` under the `analysis` section:

```yaml
analysis:
  distribution_of_revisions_value_changes:
    execute: true   # set to true to run this analysis
    reload_data: false  # set to true to re-run the SQL query and overwrite stored results in analysis/results/
  entity_types_analysis:
    execute: true
    reload_data: false
  property_stats:
    execute: true
    reload_data: false
```

To run the analysis, simply execute from root:

```bash
python3 -m analysis.scripts.general_analysis
```

| Analysis | Description |
|---|---|
| `distribution_of_revisions_value_changes` | Distribution of revisions and value changes across all entities |
| `entity_types_analysis` | Largest entity types, most edited entity types and user type breakdown for the latter |
| `property_stats` | Most edited properties (wrt. number of entities that have a change to that entity) and type of action distribution |

Output figures are saved to `analysis/results/figures/`.

We provide datasets to run this analysis ([WiDiff: Analysis Results from Wikidata Edit History Dump (June 2025)](https://doi.org/10.5281/zenodo.19771569)). Download the *widiff_analysis_results_20250601.zip* and put the .csv files in the folder `analysis/results/`.

**Note:** On first run, set `reload_data: true` to execute the SQL queries and store the results. Subsequent runs can use `reload_data: false` to load from the stored results.