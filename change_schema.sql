CREATE TABLE IF NOT EXISTS revision (
    revision_id INT,
    entity_id INT,
    entity_label TEXT,
    file_path TEXT,
    timestamp TIMESTAMP WITH TIME ZONE,
    user_id TEXT,
    username TEXT,
    comment TEXT,
    PRIMARY KEY (revision_id)
);

CREATE TABLE IF NOT EXISTS change (
    revision_id INT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    datatype TEXT,
    datatype_metadata TEXT,
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, datatype_metadata),
    FOREIGN KEY (revision_id) REFERENCES revision(revision_id)
);

CREATE TABLE IF NOT EXISTS change_metadata (
    revision_id INT,
    property_id INT,
    value_id TEXT,
    datatype_metadata TEXT,
    change_metadata TEXT,
    value DOUBLE PRECISION,
    PRIMARY KEY (revision_id, property_id, value_id, datatype_metadata, change_metadata),
    FOREIGN KEY (revision_id, property_id, value_id, datatype_metadata) REFERENCES change(revision_id, property_id, value_id, datatype_metadata)
);

CREATE TABLE IF NOT EXISTS entity_types (
    entity_id INT,
    class_id INT,
    class_label TEXT,
    rank TEXT,
    PRIMARY KEY (entity_id, class_id)
);