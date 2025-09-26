CREATE TABLE IF NOT EXISTS revision (
    prev_revision_id BIGINT,
    revision_id BIGINT,
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
    revision_id BIGINT,
    property_id INT,
    property_label TEXT,
    value_id TEXT,
    old_value JSONB,
    new_value JSONB,
    datatype TEXT,
    change_target TEXT, -- can be '' (value), p-id of qualifier, 'rank', name of datatype metadata (e.g. 'upperBound' for quantity)
    action TEXT,
    target TEXT,
    old_hash TEXT,
    new_hash TEXT,
    PRIMARY KEY (revision_id, property_id, value_id, change_target),
    FOREIGN KEY (revision_id) REFERENCES revision(revision_id)
);

CREATE TABLE IF NOT EXISTS change_metadata (
    revision_id BIGINT,
    property_id INT,
    value_id TEXT,
    change_target TEXT,
    change_metadata TEXT,
    value DOUBLE PRECISION,
    PRIMARY KEY (revision_id, property_id, value_id, change_target, change_metadata),
    FOREIGN KEY (revision_id, property_id, value_id, change_target) REFERENCES change(revision_id, property_id, value_id, change_target)
);

CREATE TABLE IF NOT EXISTS class (
    class_id INT,
    class_label TEXT,
    rank TEXT,
    PRIMARY KEY (class_id)
);

CREATE TABLE IF NOT EXISTS entity_type (
    entity_id INT,
    class_id INT,
    PRIMARY KEY (entity_id, class_id),
    FOREIGN KEY (class_id) REFERENCES class(class_id)
);
    