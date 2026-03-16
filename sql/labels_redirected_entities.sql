
INSERT INTO entity_labels_alias_description
select 'Q' || r.entity_id::text, r.entity_id, elad.label, '', ''
FROM revision r join entity_labels_alias_description elad on r.q_id_redirect = elad.numeric_id::text
where r.redirect = TRUE and r.entity_label = ''
ON CONFLICT (numeric_id) DO NOTHING;

CREATE INDEX entity_id_idx_less on revision_less (entity_id);
CREATE INDEX redirect_idx_less on revision_less (redirect);
CREATE INDEX entity_label_idx_less on revision_less (entity_label);

INSERT INTO entity_labels_alias_description
select 'Q' || r.entity_id::text, r.entity_id, elad.label, '', ''
FROM revision_less r join entity_labels_alias_description elad on r.q_id_redirect = elad.numeric_id::text
where r.redirect = TRUE and r.entity_label = ''
ON CONFLICT (numeric_id) DO NOTHING;

CREATE INDEX redirect_idx_sa on revision_sa (redirect);
CREATE INDEX entity_label_idx_sa on revision_sa (entity_label);
CREATE INDEX entity_id_idx_sa on revision_sa (entity_id);

INSERT INTO entity_labels_alias_description
select 'Q' || r.entity_id::text, r.entity_id, elad.label, '', ''
FROM revision_less_sa r join entity_labels_alias_description elad on r.q_id_redirect = elad.numeric_id::text
where r.redirect = TRUE and r.entity_label = ''
ON CONFLICT (numeric_id) DO NOTHING;

CREATE INDEX redirect_idx_ao on revision_ao (redirect);
CREATE INDEX entity_label_idx_ao on revision_ao (entity_label);
CREATE INDEX entity_id_idx_ao on revision_ao (entity_id);

INSERT INTO entity_labels_alias_description
select 'Q' || r.entity_id::text, r.entity_id, elad.label, '', ''
FROM revision_less_ao r join entity_labels_alias_description elad on r.q_id_redirect = elad.numeric_id::text
where r.redirect = TRUE and r.entity_label = ''
ON CONFLICT (numeric_id) DO NOTHING;

-- add is_reverted to feautre tables
alter table features_time
add column is_reverted INT DEFAULT 0;

alter table features_entity
add column is_reverted INT DEFAULT 0;

alter table features_time
add column is_reverted INT DEFAULT 0;

alter table features_globecoordinate
add column is_reverted INT DEFAULT 0;

UPDATE features_time f
SET is_reverted = (
    SELECT v.is_reverted 
    FROM value_change v
    WHERE v.revision_id = f.revision_id
    AND v.property_id = f.property_id
    AND v.value_id = f.value_id
    AND v.change_target = f.change_target
    LIMIT 1
);

UPDATE features_quantity f
SET is_reverted = (
    SELECT v.is_reverted 
    FROM value_change v
    WHERE v.revision_id = f.revision_id
    AND v.property_id = f.property_id
    AND v.value_id = f.value_id
    AND v.change_target = f.change_target
    LIMIT 1
);

UPDATE features_globecoordinate f
SET is_reverted = (
    SELECT v.is_reverted 
    FROM value_change v
    WHERE v.revision_id = f.revision_id
    AND v.property_id = f.property_id
    AND v.value_id = f.value_id
    AND v.change_target = f.change_target
    LIMIT 1
);

UPDATE features_entity f
SET is_reverted = (
    SELECT v.is_reverted 
    FROM value_change v
    WHERE v.revision_id = f.revision_id
    AND v.property_id = f.property_id
    AND v.value_id = f.value_id
    AND v.change_target = f.change_target
    LIMIT 1
);

UPDATE features_text f
SET is_reverted = (
    SELECT v.is_reverted 
    FROM value_change v
    WHERE v.revision_id = f.revision_id
    AND v.property_id = f.property_id
    AND v.value_id = f.value_id
    AND v.change_target = f.change_target
    LIMIT 1
);