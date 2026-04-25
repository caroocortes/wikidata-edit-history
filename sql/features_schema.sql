--- #####################################################
--      FEATURE TABLES
--- #####################################################

CREATE TABLE IF NOT EXISTS features_time{suffix} (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    change_target TEXT,

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    old_value JSONB,
    new_value JSONB,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- for time
    date_diff_days BIGINT,
    sign_change INT, -- 0 or 1
    change_one_to_zero INT,
    day_added INT,
    day_removed INT,
    month_added INT,
    month_removed INT,
    different_year INT, -- 0 or 1
    different_month INT, -- 0 or 1
    different_day INT, -- 0 or 1

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change{suffix}(revision_id, property_id, value_id, change_target)
);


CREATE TABLE IF NOT EXISTS features_quantity{suffix} (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    change_target TEXT,

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    old_value JSONB,
    new_value JSONB,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- for quantity
    sign_change INT, -- 0 or 1
    precision_change INT, -- 0 or 1
    length_increase INT, -- 0 or 1
    length_decrease INT, -- 0 or 1
    whole_number_change INT, -- 0 or 1
    old_is_prefix_of_new INT,
    new_is_prefix_of_old INT,
    same_float_value INT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change{suffix}(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_globecoordinate{suffix} (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    old_value JSONB,
    new_value JSONB,

    -- for globecoordinate
    latitude_sign_change INT, -- 0 or 1
    longitude_sign_change INT,-- 0 or 1
    latitude_whole_number_change INT, -- 0 or 1
    longitude_whole_number_change INT, -- 0 or 1

    latitude_precision_change INT, -- 0 or 1
    longitude_precision_change INT, -- 0 or 1
    latitude_length_increase INT, -- 0 or 1
    latitude_length_decrease INT, -- 0 or 1
    longitude_length_increase INT, -- 0 or 1
    longitude_length_decrease INT, -- 0 or 1

    latitude_old_is_prefix_of_new INT,
    latitude_new_is_prefix_of_old INT,
    latitude_same_float_value INT,

    longitude_old_is_prefix_of_new INT,
    longitude_new_is_prefix_of_old INT,
    longitude_same_float_value INT,

    label_latitude VARCHAR(255),
    label_longitude VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change{suffix}(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_text{suffix} (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT, -- this is the label or the alias if label == ''
    old_value JSONB,
    new_value JSONB,

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

    -- only for text
    same_value_without_special_char INT,
    special_char_count_diff INT,
    
    char_insertions INT,
    char_deletions INT,
    char_substitutions INT,
    adjacent_char_swap INT,
    has_significant_prefix INT,
    has_significant_suffix INT,

    value_cosine_similarity FLOAT,

    label VARCHAR(255),
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change{suffix}(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS features_entity{suffix} (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    change_target TEXT,

    -- For filtering
    new_datatype TEXT,
    old_datatype TEXT,
    action VARCHAR(50),

    -- For calculating semantic similarity features
    entity_label TEXT,
    
    old_value JSONB,
    new_value JSONB,
    old_value_label TEXT,  -- this is the label or the alias if label == ''
    new_value_label TEXT, -- this is the label or the alias if label == ''
    old_value_description TEXT,
    new_value_description TEXT,

    -- for entity  
    token_overlap FLOAT, 
    old_in_new INT,
    new_in_old INT, 
    edit_distance_ratio FLOAT,
    complete_replacement INT,
    is_link_change INT,

    -- semantic similarity (embeddings) -- are calculated later
    label_cosine_similarity FLOAT,
    description_cosine_similarity FLOAT,

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
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES value_change{suffix}(revision_id, property_id, value_id, change_target)
);

-- CREATE TABLE IF NOT EXISTS features_property_replacement{suffix} (
    
--     -- References to both changes
--     delete_revision_id BIGINT,
--     delete_property_id INT, 
--     delete_value_id TEXT,
--     delete_change_target TEXT,

--     create_revision_id BIGINT,
--     create_property_id INT,
--     create_value_id TEXT,
--     create_change_target TEXT,
    
--     -- Pair-specific features
--     time_diff FLOAT,
--     same_day INT,
--     same_hour INT,
--     same_revision INT,
--     delete_before_create INT,
--     same_user INT,
--     property_label_similarity FLOAT,

--     -- Columns for test purposes, can remove later
--     delete_timestamp TIMESTAMP WITH TIME ZONE,
--     create_timestamp TIMESTAMP WITH TIME ZONE,

--     delete_property_label VARCHAR(255),
--     create_property_label VARCHAR(255),

--     delete_user_id VARCHAR(255),
--     create_user_id VARCHAR(255),
    
--     label VARCHAR(255),

--     PRIMARY KEY (delete_revision_id, delete_property_id, delete_value_id, delete_change_target, create_revision_id, create_property_id, create_value_id, create_change_target)
-- );

