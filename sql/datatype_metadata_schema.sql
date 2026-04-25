CREATE TABLE IF NOT EXISTS datatype_metadata_change{suffix} (
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    old_datatype TEXT,
    new_datatype TEXT,
    change_target TEXT, --name of datatype metadata (e.g. 'upperBound' for quantity)
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
    FOREIGN KEY (revision_id) REFERENCES revision{suffix}(revision_id)
);