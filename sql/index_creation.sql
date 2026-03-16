create index property_id_features_text_idx on features_text (property_id);
create index property_id_features_globe_idx on features_globecoordinate (property_id);
create index property_id_features_entity_idx on features_entity (property_id);
create index property_id_features_quantity_idx on features_quantity (property_id);
create index property_id_features_time_idx on features_time (property_id);

create index entity_id_revision_idx on revision (entity_id);
create index redirect_revision_idx on revision (redirect);
create index file_path_revision_idx on revision (file_path);
CREATE INDEX entity_label_revision_idx on revision (entity_label);

create index entity_id_value_change_idx on value_change (entity_id);
create index property_id_value_change_idx on value_change (property_id);
create index change_target_value_change_idx on value_change (change_target);
create index revision_id_value_change_idx on value_change (revision_id);
create index action_value_change_idx on value_change (action);
create index target_value_change_idx on value_change (target);
create index not_reverted_value_change_idx on value_change (is_reverted, reversion)
where is_reverted = 0 and reversion = 0;

ALTER TABLE p279_entity_types
ALTER COLUMN entity_numeric_id TYPE INT using entity_numeric_id::INT;

ALTER TABLE p31_entity_types
ALTER COLUMN entity_numeric_id TYPE INT using entity_numeric_id::INT;

ALTER TABLE p31_entity_types
ALTER COLUMN entity_type_numeric_id TYPE INT using entity_type_numeric_id::INT;

ALTER TABLE p279_entity_types
ALTER COLUMN entity_type_numeric_id TYPE INT using entity_type_numeric_id::INT;

CREATE INDEX p31_entity_types_numeric_id
ON p31_entity_types (entity_numeric_id);

CREATE INDEX p279_entity_types_numeric_id
ON p279_entity_types (entity_numeric_id);

ALTER TABLE entity_labels_alias_description
ALTER COLUMN numeric_id TYPE INT using numeric_id::INT;

CREATE INDEX entity_label_alias_desc_numeric_id
ON entity_labels_alias_description (numeric_id);

