SET CLIENT_ENCODING = 'UTF8';

--- #############################################
--          ASTRONOMICAL OBJECTS
--- #############################################
CREATE TABLE IF NOT EXISTS revision_ao (
    prev_revision_id BIGINT,
    revision_id BIGINT,
    entity_id INT,
    entity_label TEXT,
    file_path TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    user_id TEXT,
    username TEXT,
    user_type TEXT,
    comment TEXT,
    redirect BOOLEAN,
    q_id_redirect TEXT,
    PRIMARY KEY (revision_id)
);

CREATE TABLE IF NOT EXISTS value_change_ao (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    is_reverted INT,
    reversion INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_ao(revision_id)
);

-- CREATE TABLE IF NOT EXISTS value_change_metadata_ao (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,
--     change_metadata TEXT,
--     value DOUBLE PRECISION,
--     PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_ao(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS datatype_metadata_change_ao (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_ao(revision_id)
);

-- CREATE TABLE IF NOT EXISTS datatype_metadata_change_metadata_ao (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,
--     change_metadata TEXT,
--     value DOUBLE PRECISION,
--     PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES datatype_metadata_change_ao(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS qualifier_change_ao (
    revision_id BIGINT,
    property_id INT, -- statement property id
    property_label TEXT,
    value_id TEXT, -- statement value id
    qual_property_id INT, -- qualifier property id
    qual_property_label TEXT,
    value_hash TEXT, -- hash of qualifier value. This hash + qual_property_id identify each qualifier value
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- will be '' or datatype metadata name
    action TEXT, -- Will only be CREATE/DELETE, never UPDATE
    target TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, qual_property_id, value_hash, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_ao(revision_id)
    -- NOTE: revision_id, property_id, value_id does not necessarily exist in value_change since a revision could involve only reference/qualifier changes
);

CREATE TABLE IF NOT EXISTS reference_change_ao (
    revision_id BIGINT,
    property_id INT, -- statement property id
    property_label TEXT,
    value_id TEXT, -- statement value id
    ref_property_id INT, -- reference property id
    ref_property_label TEXT,
    ref_hash TEXT, -- identifies the reference (a reference is composed of multiple property - values)
    value_hash TEXT, -- hash of reference value. This hash + ref_property_id identify each reference value
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- will be '' or datatype metadata name
    action TEXT, -- Will only be CREATE/DELETE, never UPDATE
    target TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, ref_hash, ref_property_id, value_hash, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_ao(revision_id)
    -- NOTE: revision_id, property_id, value_id does not necessarily exist in value_change since a revision could involve only reference/qualifier changes
);


--- #############################################
--          SCHOLARLY ARTICLE
--- #############################################
CREATE TABLE IF NOT EXISTS revision_sa (
    prev_revision_id BIGINT,
    revision_id BIGINT,
    entity_id INT,
    entity_label TEXT,
    file_path TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    user_id TEXT,
    username TEXT,
    user_type TEXT,
    comment TEXT,
    redirect BOOLEAN,
    q_id_redirect TEXT,
    PRIMARY KEY (revision_id)
);

CREATE TABLE IF NOT EXISTS value_change_sa (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    is_reverted INT,
    reversion INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_sa(revision_id)
);

-- CREATE TABLE IF NOT EXISTS value_change_metadata_sa (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,
--     change_metadata TEXT,
--     value DOUBLE PRECISION,
--     PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_sa(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS datatype_metadata_change_sa (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_sa(revision_id)
);

-- CREATE TABLE IF NOT EXISTS datatype_metadata_change_metadata_sa (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,
--     change_metadata TEXT,
--     value DOUBLE PRECISION,
--     PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES datatype_metadata_change_sa(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS qualifier_change_sa (
    revision_id BIGINT,
    property_id INT, -- statement property id
    property_label TEXT,
    value_id TEXT, -- statement value id
    qual_property_id INT, -- qualifier property id
    qual_property_label TEXT,
    value_hash TEXT, -- hash of qualifier value. This hash + qual_property_id identify each qualifier value
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- will be '' or datatype metadata name
    action TEXT, -- Will only be CREATE/DELETE, never UPDATE
    target TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, qual_property_id, value_hash, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_sa(revision_id)
    -- NOTE: revision_id, property_id, value_id does not necessarily exist in value_change since a revision could involve only reference/qualifier changes
);

CREATE TABLE IF NOT EXISTS reference_change_sa (
    revision_id BIGINT,
    property_id INT, -- statement property id
    property_label TEXT,
    value_id TEXT, -- statement value id
    ref_property_id INT, -- reference property id
    ref_property_label TEXT,
    ref_hash TEXT, -- identifies the reference (a reference is composed of multiple property - values)
    value_hash TEXT, -- hash of reference value. This hash + ref_property_id identify each reference value
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- will be '' or datatype metadata name
    action TEXT, -- Will only be CREATE/DELETE, never UPDATE
    target TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, ref_hash, ref_property_id, value_hash, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_sa(revision_id)
    -- NOTE: revision_id, property_id, value_id does not necessarily exist in value_change since a revision could involve only reference/qualifier changes
);

--- #####################################################
--      REST OF ENTITIES with at least 20 revisions?
--- #####################################################
CREATE TABLE IF NOT EXISTS revision (
    prev_revision_id BIGINT,
    revision_id BIGINT,
    entity_id INT,
    entity_label TEXT,
    file_path TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    user_id TEXT,
    username TEXT,
    user_type TEXT,
    comment TEXT,
    redirect BOOLEAN,
    q_id_redirect TEXT,
    PRIMARY KEY (revision_id)
);

CREATE TABLE IF NOT EXISTS value_change (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    is_reverted INT,
    reversion INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision(revision_id)
);

-- CREATE TABLE IF NOT EXISTS value_change_metadata (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,
--     change_metadata TEXT,
--     value DOUBLE PRECISION,
--     PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS datatype_metadata_change (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision(revision_id)
);

-- CREATE TABLE IF NOT EXISTS datatype_metadata_change_metadata (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,
--     change_metadata TEXT,
--     value DOUBLE PRECISION,
--     PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES datatype_metadata_change(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS qualifier_change (
    revision_id BIGINT,
    property_id INT, -- statement property id
    property_label TEXT,
    value_id TEXT, -- statement value id
    qual_property_id INT, -- qualifier property id
    qual_property_label TEXT,
    value_hash TEXT, -- hash of qualifier value. This hash + qual_property_id identify each qualifier value
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- will be '' or datatype metadata name
    action TEXT, -- Will only be CREATE/DELETE, never UPDATE
    target TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, qual_property_id, value_hash, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision(revision_id)
    -- NOTE: revision_id, property_id, value_id does not necessarily exist in value_change since a revision could involve only reference/qualifier changes
);

CREATE TABLE IF NOT EXISTS reference_change (
    revision_id BIGINT,
    property_id INT, -- statement property id
    property_label TEXT,
    value_id TEXT, -- statement value id
    ref_property_id INT, -- reference property id
    ref_property_label TEXT,
    ref_hash TEXT, -- identifies the reference (a reference is composed of multiple property - values)
    value_hash TEXT, -- hash of reference value. This hash + ref_property_id identify each reference value
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- will be '' or datatype metadata name
    action TEXT, -- Will only be CREATE/DELETE, never UPDATE
    target TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, ref_hash, ref_property_id, value_hash, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision(revision_id)
    -- NOTE: revision_id, property_id, value_id does not necessarily exist in value_change since a revision could involve only reference/qualifier changes
);


--- #####################################################
--      REST OF ENTITIES with LESS 20 than revisions
--- #####################################################
CREATE TABLE IF NOT EXISTS revision_less (
    prev_revision_id BIGINT,
    revision_id BIGINT,
    entity_id INT,
    entity_label TEXT,
    file_path TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    user_id TEXT,
    username TEXT,
    user_type TEXT,
    comment TEXT,
    redirect BOOLEAN,
    q_id_redirect TEXT,
    PRIMARY KEY (revision_id)
);

CREATE TABLE IF NOT EXISTS value_change_less (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    is_reverted INT,
    reversion INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_less(revision_id)
);

-- CREATE TABLE IF NOT EXISTS value_change_metadata_less (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,
--     change_metadata TEXT,
--     value DOUBLE PRECISION,
--     PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_less(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS datatype_metadata_change_less (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_less(revision_id)
);

-- CREATE TABLE IF NOT EXISTS datatype_metadata_change_metadata_less (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,
--     change_metadata TEXT,
--     value DOUBLE PRECISION,
--     PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES datatype_metadata_change_less(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS qualifier_change_less (
    revision_id BIGINT,
    property_id INT, -- statement property id
    property_label TEXT,
    value_id TEXT, -- statement value id
    qual_property_id INT, -- qualifier property id
    qual_property_label TEXT,
    value_hash TEXT, -- hash of qualifier value. This hash + qual_property_id identify each qualifier value
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- will be '' or datatype metadata name
    action TEXT, -- Will only be CREATE/DELETE, never UPDATE
    target TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, qual_property_id, value_hash, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_less(revision_id)
    -- NOTE: revision_id, property_id, value_id does not necessarily exist in value_change since a revision could involve only reference/qualifier changes
);

CREATE TABLE IF NOT EXISTS reference_change_less (
    revision_id BIGINT,
    property_id INT, -- statement property id
    property_label TEXT,
    value_id TEXT, -- statement value id
    ref_property_id INT, -- reference property id
    ref_property_label TEXT,
    ref_hash TEXT, -- identifies the reference (a reference is composed of multiple property - values)
    value_hash TEXT, -- hash of reference value. This hash + ref_property_id identify each reference value
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, -- will be '' or datatype metadata name
    action TEXT, -- Will only be CREATE/DELETE, never UPDATE
    target TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    week varchar(255),
    year_month varchar(255),
    year varchar(255),
    label TEXT,
    entity_id INT,
    entity_label TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, ref_hash, ref_property_id, value_hash, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision_less(revision_id)
    -- NOTE: revision_id, property_id, value_id does not necessarily exist in value_change since a revision could involve only reference/qualifier changes
);


--- #####################################################
--      ENTITY STATS TABLES
--- #####################################################
CREATE TABLE IF NOT EXISTS entity_stats_sa (
    entity_id INT PRIMARY KEY,
    entity_label TEXT,
    entity_types_31 TEXT,
    
    num_revisions INT,
    
    num_value_changes INT, -- this includes all changes to property values (creates, deletes, updates) 
    num_value_change_creates INT,
    num_value_change_deletes INT,
    num_value_change_updates INT,

    num_rank_changes INT,
    num_rank_creates INT,
    num_rank_deletes INT,
    num_rank_updates INT,

    num_qualifier_changes INT,
    num_reference_changes INT,

    num_datatype_metadata_changes INT,
    num_datatype_metadata_creates INT,
    num_datatype_metadata_deletes INT,
    num_datatype_metadata_updates INT,
    
    first_revision_timestamp TIMESTAMP WITH TIME ZONE, 
    last_revision_timestamp TIMESTAMP WITH TIME ZONE,
    
    num_bot_edits INT, 
    num_anonymous_edits INT,
    num_human_edits INT,

    num_reverted_edits INT,
    num_reversions INT,
    num_reverted_edits_create INT,
    num_reverted_edits_delete INT,
    num_reverted_edits_update INT
);

CREATE TABLE IF NOT EXISTS entity_stats_ao (
    entity_id INT PRIMARY KEY,
    entity_label TEXT,
    entity_types_31 TEXT,
    
    num_revisions INT,
    
    num_value_changes INT, -- this includes all changes to property values (creates, deletes, updates) 
    num_value_change_creates INT,
    num_value_change_deletes INT,
    num_value_change_updates INT,

    num_rank_changes INT,
    num_rank_creates INT,
    num_rank_deletes INT,
    num_rank_updates INT,

    num_qualifier_changes INT,
    num_reference_changes INT,

    num_datatype_metadata_changes INT,
    num_datatype_metadata_creates INT,
    num_datatype_metadata_deletes INT,
    num_datatype_metadata_updates INT,
    
    first_revision_timestamp TIMESTAMP WITH TIME ZONE, 
    last_revision_timestamp TIMESTAMP WITH TIME ZONE,
    
    num_bot_edits INT, 
    num_anonymous_edits INT,
    num_human_edits INT,

    num_reverted_edits INT,
    num_reversions INT,
    num_reverted_edits_create INT,
    num_reverted_edits_delete INT,
    num_reverted_edits_update INT
);

CREATE TABLE IF NOT EXISTS entity_stats_less (
    entity_id INT PRIMARY KEY,
    entity_label TEXT,
    entity_types_31 TEXT,
    
    num_revisions INT,
    
    num_value_changes INT, -- this includes all changes to property values (creates, deletes, updates) 
    num_value_change_creates INT,
    num_value_change_deletes INT,
    num_value_change_updates INT,

    num_rank_changes INT,
    num_rank_creates INT,
    num_rank_deletes INT,
    num_rank_updates INT,

    num_qualifier_changes INT,
    num_reference_changes INT,

    num_datatype_metadata_changes INT,
    num_datatype_metadata_creates INT,
    num_datatype_metadata_deletes INT,
    num_datatype_metadata_updates INT,
    
    first_revision_timestamp TIMESTAMP WITH TIME ZONE, 
    last_revision_timestamp TIMESTAMP WITH TIME ZONE,
    
    num_bot_edits INT, 
    num_anonymous_edits INT,
    num_human_edits INT,

    num_reverted_edits INT,
    num_reversions INT,
    num_reverted_edits_create INT,
    num_reverted_edits_delete INT,
    num_reverted_edits_update INT
);

CREATE TABLE IF NOT EXISTS entity_stats (
    entity_id INT PRIMARY KEY,
    entity_label TEXT,
    entity_types_31 TEXT,
    
    num_revisions INT,
    
    num_value_changes INT, -- this includes all changes to property values (creates, deletes, updates) 
    num_value_change_creates INT,
    num_value_change_deletes INT,
    num_value_change_updates INT,

    num_rank_changes INT,
    num_rank_creates INT,
    num_rank_deletes INT,
    num_rank_updates INT,

    num_qualifier_changes INT,
    num_reference_changes INT,

    num_datatype_metadata_changes INT,
    num_datatype_metadata_creates INT,
    num_datatype_metadata_deletes INT,
    num_datatype_metadata_updates INT,
    
    first_revision_timestamp TIMESTAMP WITH TIME ZONE, 
    last_revision_timestamp TIMESTAMP WITH TIME ZONE,
    
    num_bot_edits INT, 
    num_anonymous_edits INT,
    num_human_edits INT,

    num_reverted_edits INT,
    num_reversions INT,
    num_reverted_edits_create INT,
    num_reverted_edits_delete INT,
    num_reverted_edits_update INT
);
--- #####################################################
--      FEATURE TABLES
--- #####################################################

-- CREATE TABLE IF NOT EXISTS features_reverted_edit (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,

--     -- For filtering
--     new_datatype TEXT,
--     old_datatype TEXT,
--     action VARCHAR(50),

--     -- for reverted edit
--     user_type_encoded INT,
--     day_of_week_encoded INT,
--     hour_of_day INT,
--     is_weekend INT,
--     action_encoded INT, 
--     is_reverted_within_day INT, 
--     num_changes_same_user_last_24h INT,
--     rv_keyword_in_comment_next_10 INT, 
--     hash_reversion_next_10 INT, 
--     time_to_prev_change_seconds FLOAT, 
--     time_to_next_change_seconds FLOAT,
    
--     label VARCHAR(255),
--     PRIMARY KEY (revision_id, property_id, value_id, change_target),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS features_time (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    old_value TEXT,
    new_value TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- for time
    date_diff_days BIGINT,
    time_diff_minutes BIGINT,
    sign_change INT, -- 0 or 1
    change_one_to_zero INT, -- YYYY-01-01 -> YYYY-00-00 -> I treated this as formatting
    change_one_to_value INT,
    change_zero_to_one INT, -- YYYY-00-00 -> YYYY-01-01 -> I treated this as refinement? # TODO: check this
    day_added INT,
    day_removed INT,
    month_added INT,
    month_removed INT,
    different_year INT, -- 0 or 1
    different_month INT, -- 0 or 1
    different_day INT, -- 0 or 1

    full_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change(revision_id, property_id, value_id, change_target)
);


CREATE TABLE IF NOT EXISTS features_quantity (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    old_value TEXT,
    new_value TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- for quantity
    -- relative_value_diff_abs FLOAT,
    sign_change INT, -- 0 or 1
    precision_change INT, -- 0 or 1
    precision_added INT, -- 0 or 1
    precision_removed INT, -- 0 or 1
    length_increase INT, -- 0 or 1
    length_decrease INT, -- 0 or 1
    whole_number_change INT, -- 0 or 1
    shared_prefix INT,
    shared_prefix_length BIGINT,

    full_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_globecoordinate (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    old_value TEXT,
    new_value TEXT,

    -- for globecoordinate
    relative_value_diff_latitude FLOAT,
    relative_value_diff_longitude FLOAT,
    latitude_sign_change INT, -- 0 or 1
    longitude_sign_change INT,-- 0 or 1
    latitude_whole_number_change INT, -- 0 or 1
    longitude_whole_number_change INT, -- 0 or 1
    coordinate_distance_km FLOAT,
    latitude_precision_change INT, -- 0 or 1
    longitude_precision_change INT, -- 0 or 1
    latitude_length_increase INT, -- 0 or 1
    latitude_length_decrease INT, -- 0 or 1
    longitude_length_increase INT, -- 0 or 1
    longitude_length_decrease INT, -- 0 or 1
    longitude_shared_prefix INT,
    latitude_shared_prefix INT,
    longitude_shared_prefix_length BIGINT,
    latitude_shared_prefix_length BIGINT,

    -- NOTE: might not use it
    full_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_text (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    old_value TEXT,
    new_value TEXT,

    -- only for text
    char_insertions INT,
    char_deletions INT,
    adjacent_char_swap INT,
    avg_word_similarity FLOAT,
    has_significant_prefix INT,
    has_significant_suffix INT,

    -- for text & entity
    length_diff_abs INT,
    token_count_old INT, 
    token_count_new INT,         
    token_overlap FLOAT, 
    old_in_new INT,
    new_in_old INT, 
    levenshtein_distance INT,
    edit_distance_ratio FLOAT,
    complete_replacement INT,
    structure_similarity FLOAT,

    full_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_entity (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT,
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    
    old_value TEXT,
    new_value TEXT,
    old_value_label TEXT,  -- this is the label or the alias if label == ''
    new_value_label TEXT, -- this is the label or the alias if label == ''
    old_value_description TEXT,
    new_value_description TEXT,

    -- for entity
    length_diff_abs INT,
    token_count_old INT, 
    token_count_new INT,         
    token_overlap FLOAT, 
    old_in_new INT,
    new_in_old INT, 
    levenshtein_distance INT,
    edit_distance_ratio FLOAT,
    complete_replacement INT,
    structure_similarity FLOAT,

    -- semantic similarity (embeddings) -- are calculated later
    label_cosine_similarity FLOAT,
    description_cosine_similarity FLOAT,
    full_cosine_similarity FLOAT,

    -- transitive closure based features
    old_value_subclass_new_value INT,
    new_value_subclass_old_value INT,

    old_value_located_in_new_value INT,
    new_value_located_in_old_value INT,

    old_value_has_parts_new_value INT,
    new_value_has_parts_old_value INT,

    old_value_part_of_new_value INT,
    new_value_part_of_old_value INT,

    -- new_value_is_metaclass_for_old_value INT,
    -- old_value_is_metaclass_for_new_value INT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_property_replacement (
    pair_id BIGSERIAL PRIMARY KEY, -- auto incremental id for each pair
    
    -- References to both changes
    delete_revision_id BIGINT,
    delete_property_id INT, 
    delete_value_id TEXT,
    delete_change_target TEXT,

    create_revision_id BIGINT,
    create_property_id INT,
    create_value_id TEXT,
    create_change_target TEXT,
    
    -- Pair-specific features
    time_diff FLOAT,
    same_day INT,
    same_hour INT,
    same_revision INT,
    delete_before_create INT,
    same_user INT,
    property_label_similarity FLOAT,

    -- Columns for test purposes, can remove later
    delete_timestamp TIMESTAMP WITH TIME ZONE,
    create_timestamp TIMESTAMP WITH TIME ZONE,

    delete_property_label VARCHAR(255),
    create_property_label VARCHAR(255),

    delete_user_id VARCHAR(255),
    create_user_id VARCHAR(255),
    
    label VARCHAR(255)
);

--- #####################################################


-- CREATE TABLE IF NOT EXISTS features_reverted_edit_less (
--     revision_id BIGINT,
--     property_id INT,
--     value_id TEXT,
--     change_target TEXT,

--     -- For filtering
--     new_datatype TEXT,
--     old_datatype TEXT,
--     action VARCHAR(50),

--     -- for reverted edit
--     user_type_encoded INT,
--     day_of_week_encoded INT,
--     hour_of_day INT,
--     is_weekend INT,
--     action_encoded INT, 
--     is_reverted_within_day INT, 
--     num_changes_same_user_last_24h INT,
--     rv_keyword_in_comment_next_10 INT, 
--     hash_reversion_next_10 INT, 
--     time_to_prev_change_seconds FLOAT, 
--     time_to_next_change_seconds FLOAT,
    
--     label VARCHAR(255),
--     PRIMARY KEY (revision_id, property_id, value_id, change_target),
--     FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_less(revision_id, property_id, value_id, change_target)
-- );

CREATE TABLE IF NOT EXISTS features_time_less (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    old_value TEXT,
    new_value TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- for time
    date_diff_days BIGINT,
    time_diff_minutes BIGINT,
    sign_change INT, -- 0 or 1
    change_one_to_zero INT, -- YYYY-01-01 -> YYYY-00-00 -> I treated this as formatting
    change_one_to_value INT,
    change_zero_to_one INT, -- YYYY-00-00 -> YYYY-01-01 -> I treated this as refinement? # TODO: check this
    day_added INT,
    day_removed INT,
    month_added INT,
    month_removed INT,
    different_year INT, -- 0 or 1
    different_month INT, -- 0 or 1
    different_day INT, -- 0 or 1

    full_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_less(revision_id, property_id, value_id, change_target)
);


CREATE TABLE IF NOT EXISTS features_quantity_less (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    old_value TEXT,
    new_value TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- for quantity
    -- relative_value_diff_abs FLOAT,
    sign_change INT, -- 0 or 1
    precision_change INT, -- 0 or 1
    precision_added INT, -- 0 or 1
    precision_removed INT, -- 0 or 1
    length_increase INT, -- 0 or 1
    length_decrease INT, -- 0 or 1
    whole_number_change INT, -- 0 or 1
    shared_prefix INT, -- 0 or 1
    shared_prefix_length BIGINT,

    full_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_less(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_globecoordinate_less (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    old_value TEXT,
    new_value TEXT,

    -- for globecoordinate
    relative_value_diff_latitude FLOAT,
    relative_value_diff_longitude FLOAT,
    latitude_sign_change INT, -- 0 or 1
    longitude_sign_change INT,-- 0 or 1
    latitude_whole_number_change INT, -- 0 or 1
    longitude_whole_number_change INT, -- 0 or 1
    coordinate_distance_km FLOAT,
    latitude_precision_change INT, -- 0 or 1
    longitude_precision_change INT, -- 0 or 1
    latitude_length_increase INT, -- 0 or 1
    latitude_length_decrease INT, -- 0 or 1
    longitude_length_increase INT, -- 0 or 1
    longitude_length_decrease INT, -- 0 or 1
    longitude_shared_prefix INT,
    latitude_shared_prefix INT,
    longitude_shared_prefix_length BIGINT,
    latitude_shared_prefix_length BIGINT,

    -- NOTE: might not use it
    full_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_less(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_text_less (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    old_value TEXT,
    new_value TEXT,

    -- only for text
    char_insertions INT,
    char_deletions INT,
    adjacent_char_swap INT,
    avg_word_similarity FLOAT,
    has_significant_prefix INT,
    has_significant_suffix INT,

    -- for text & entity
    length_diff_abs INT,
    token_count_old INT, 
    token_count_new INT,         
    token_overlap FLOAT, 
    old_in_new INT,
    new_in_old INT, 
    levenshtein_distance INT,
    edit_distance_ratio FLOAT,
    complete_replacement INT,
    structure_similarity FLOAT,

    full_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_less(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_entity_less (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT,
    entity_description TEXT,
    entity_types_31 TEXT,
    entity_types_279 TEXT,
    
    old_value TEXT,
    new_value TEXT,
    old_value_label TEXT,  -- this is the label or the alias if label == ''
    new_value_label TEXT, -- this is the label or the alias if label == ''
    old_value_description TEXT,
    new_value_description TEXT,

    -- for entity
    length_diff_abs INT,
    token_count_old INT, 
    token_count_new INT,         
    token_overlap FLOAT, 
    old_in_new INT,
    new_in_old INT, 
    levenshtein_distance INT,
    edit_distance_ratio FLOAT,
    complete_replacement INT,
    structure_similarity FLOAT,

    -- semantic similarity (embeddings) -- are calculated later
    label_cosine_similarity FLOAT,
    description_cosine_similarity FLOAT,
    full_cosine_similarity FLOAT,

    -- transitive closure based features
    old_value_subclass_new_value INT,
    new_value_subclass_old_value INT,

    old_value_located_in_new_value INT,
    new_value_located_in_old_value INT,

    old_value_has_parts_new_value INT,
    new_value_has_parts_old_value INT,

    old_value_part_of_new_value INT,
    new_value_part_of_old_value INT,

    -- new_value_is_metaclass_for_old_value INT,
    -- old_value_is_metaclass_for_new_value INT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change_less(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_property_replacement_less (
    
    -- References to both changes
    delete_revision_id BIGINT,
    delete_property_id INT, 
    delete_value_id TEXT,
    delete_change_target TEXT,

    create_revision_id BIGINT,
    create_property_id INT,
    create_value_id TEXT,
    create_change_target TEXT,
    
    -- Pair-specific features
    time_diff FLOAT,
    same_day INT,
    same_hour INT,
    same_revision INT,
    delete_before_create INT,
    same_user INT,
    property_label_similarity FLOAT,

    -- Columns for test purposes, can remove later
    delete_timestamp TIMESTAMP WITH TIME ZONE,
    create_timestamp TIMESTAMP WITH TIME ZONE,

    delete_property_label VARCHAR(255),
    create_property_label VARCHAR(255),

    delete_user_id VARCHAR(255),
    create_user_id VARCHAR(255),
    
    label VARCHAR(255),

    PRIMARY KEY (delete_revision_id, delete_property_id, delete_value_id, delete_change_target, create_revision_id, create_property_id, create_value_id, create_change_target)
);


-- #####################################################
--      ENTITY PROPERTY TIME TABLES
-- #####################################################

CREATE TABLE IF NOT EXISTS entity_property_time_stats (
    entity_id INTEGER NOT NULL,
    property_id INTEGER NOT NULL,

    -- Time bucket identifier (week / month / year)
    time_period TEXT NOT NULL,

    -- Property value changes
    num_value_changes BIGINT NOT NULL DEFAULT 0,
    num_value_additions BIGINT NOT NULL DEFAULT 0,
    num_value_deletions BIGINT NOT NULL DEFAULT 0,
    num_value_updates BIGINT NOT NULL DEFAULT 0,

    num_statement_additions BIGINT NOT NULL DEFAULT 0,
    num_statement_deletions BIGINT NOT NULL DEFAULT 0,

    num_soft_insertions BIGINT NOT NULL DEFAULT 0,
    num_soft_deletions BIGINT NOT NULL DEFAULT 0,

    -- Rank changes
    num_rank_changes BIGINT NOT NULL DEFAULT 0,
    num_rank_creates BIGINT NOT NULL DEFAULT 0,
    num_rank_deletes BIGINT NOT NULL DEFAULT 0,
    num_rank_updates BIGINT NOT NULL DEFAULT 0,

    -- Reference changes
    num_reference_additions BIGINT NOT NULL DEFAULT 0,
    num_reference_deletions BIGINT NOT NULL DEFAULT 0,

    -- Qualifier changes
    num_qualifier_additions BIGINT NOT NULL DEFAULT 0,
    num_qualifier_deletions BIGINT NOT NULL DEFAULT 0,

    -- Revision statistics
    num_revisions BIGINT NOT NULL DEFAULT 0,
    num_revisions_bot BIGINT NOT NULL DEFAULT 0,
    num_revisions_human BIGINT NOT NULL DEFAULT 0,
    num_revisions_anonymous BIGINT NOT NULL DEFAULT 0,
    num_unique_editors BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (entity_id, property_id, time_period)
);

CREATE TABLE IF NOT EXISTS entity_property_time_stats_sa (
    entity_id INTEGER NOT NULL,
    property_id INTEGER NOT NULL,

    -- Time bucket identifier (week / month / year)
    time_period TEXT NOT NULL,

    -- Property value changes
    num_value_changes BIGINT NOT NULL DEFAULT 0,
    num_value_additions BIGINT NOT NULL DEFAULT 0,
    num_value_deletions BIGINT NOT NULL DEFAULT 0,
    num_value_updates BIGINT NOT NULL DEFAULT 0,

    num_statement_additions BIGINT NOT NULL DEFAULT 0,
    num_statement_deletions BIGINT NOT NULL DEFAULT 0,

    num_soft_insertions BIGINT NOT NULL DEFAULT 0,
    num_soft_deletions BIGINT NOT NULL DEFAULT 0,

    -- Rank changes
    num_rank_changes BIGINT NOT NULL DEFAULT 0,
    num_rank_creates BIGINT NOT NULL DEFAULT 0,
    num_rank_deletes BIGINT NOT NULL DEFAULT 0,
    num_rank_updates BIGINT NOT NULL DEFAULT 0,

    -- Reference changes
    num_reference_additions BIGINT NOT NULL DEFAULT 0,
    num_reference_deletions BIGINT NOT NULL DEFAULT 0,

    -- Qualifier changes
    num_qualifier_additions BIGINT NOT NULL DEFAULT 0,
    num_qualifier_deletions BIGINT NOT NULL DEFAULT 0,

    -- Revision statistics
    num_revisions BIGINT NOT NULL DEFAULT 0,
    num_revisions_bot BIGINT NOT NULL DEFAULT 0,
    num_revisions_human BIGINT NOT NULL DEFAULT 0,
    num_revisions_anonymous BIGINT NOT NULL DEFAULT 0,
    num_unique_editors BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (entity_id, property_id, time_period)
);

CREATE TABLE IF NOT EXISTS entity_property_time_stats_ao (
    entity_id INTEGER NOT NULL,
    property_id INTEGER NOT NULL,

    -- Time bucket identifier (week / month / year)
    time_period TEXT NOT NULL,

    -- Property value changes
    num_value_changes BIGINT NOT NULL DEFAULT 0,
    num_value_additions BIGINT NOT NULL DEFAULT 0,
    num_value_deletions BIGINT NOT NULL DEFAULT 0,
    num_value_updates BIGINT NOT NULL DEFAULT 0,

    num_statement_additions BIGINT NOT NULL DEFAULT 0,
    num_statement_deletions BIGINT NOT NULL DEFAULT 0,

    num_soft_insertions BIGINT NOT NULL DEFAULT 0,
    num_soft_deletions BIGINT NOT NULL DEFAULT 0,

    -- Rank changes
    num_rank_changes BIGINT NOT NULL DEFAULT 0,
    num_rank_creates BIGINT NOT NULL DEFAULT 0,
    num_rank_deletes BIGINT NOT NULL DEFAULT 0,
    num_rank_updates BIGINT NOT NULL DEFAULT 0,

    -- Reference changes
    num_reference_additions BIGINT NOT NULL DEFAULT 0,
    num_reference_deletions BIGINT NOT NULL DEFAULT 0,

    -- Qualifier changes
    num_qualifier_additions BIGINT NOT NULL DEFAULT 0,
    num_qualifier_deletions BIGINT NOT NULL DEFAULT 0,

    -- Revision statistics
    num_revisions BIGINT NOT NULL DEFAULT 0,
    num_revisions_bot BIGINT NOT NULL DEFAULT 0,
    num_revisions_human BIGINT NOT NULL DEFAULT 0,
    num_revisions_anonymous BIGINT NOT NULL DEFAULT 0,
    num_unique_editors BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (entity_id, property_id, time_period)
);

CREATE TABLE IF NOT EXISTS entity_property_time_stats_less (
    entity_id INTEGER NOT NULL,
    property_id INTEGER NOT NULL,

    -- Time bucket identifier (week / month / year)
    time_period TEXT NOT NULL,

    -- Property value changes
    num_value_changes BIGINT NOT NULL DEFAULT 0,
    num_value_additions BIGINT NOT NULL DEFAULT 0,
    num_value_deletions BIGINT NOT NULL DEFAULT 0,
    num_value_updates BIGINT NOT NULL DEFAULT 0,

    num_statement_additions BIGINT NOT NULL DEFAULT 0,
    num_statement_deletions BIGINT NOT NULL DEFAULT 0,

    num_soft_insertions BIGINT NOT NULL DEFAULT 0,
    num_soft_deletions BIGINT NOT NULL DEFAULT 0,

    -- Rank changes
    num_rank_changes BIGINT NOT NULL DEFAULT 0,
    num_rank_creates BIGINT NOT NULL DEFAULT 0,
    num_rank_deletes BIGINT NOT NULL DEFAULT 0,
    num_rank_updates BIGINT NOT NULL DEFAULT 0,

    -- Reference changes
    num_reference_additions BIGINT NOT NULL DEFAULT 0,
    num_reference_deletions BIGINT NOT NULL DEFAULT 0,

    -- Qualifier changes
    num_qualifier_additions BIGINT NOT NULL DEFAULT 0,
    num_qualifier_deletions BIGINT NOT NULL DEFAULT 0,

    -- Revision statistics
    num_revisions BIGINT NOT NULL DEFAULT 0,
    num_revisions_bot BIGINT NOT NULL DEFAULT 0,
    num_revisions_human BIGINT NOT NULL DEFAULT 0,
    num_revisions_anonymous BIGINT NOT NULL DEFAULT 0,
    num_unique_editors BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (entity_id, property_id, time_period)
);