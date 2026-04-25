CREATE TABLE entity_type_stats AS
WITH type_split AS (
    SELECT 
		entity_id,
		unnest(string_to_array(entity_types_31, ', ')) AS individual_type,
		num_revisions,
		num_value_changes,
		num_qualifier_changes,
		num_reference_changes,
		num_bot_edits,
		num_anonymous_edits,
		num_human_edits
    FROM entity_stats 
	WHERE 
	entity_id not in (4115189, 13406268, 15397819, 112795079) and -- sandbox entities
	entity_types_31 IS NOT NULL AND entity_types_31 != ''
)
SELECT 
	individual_type,
	count(distinct entity_id),
	sum(num_revisions) as num_revisions, 
	sum(num_value_changes) as num_value_changes, 
	sum(num_qualifier_changes) as num_qualifier_changes, 
	sum(num_reference_changes) as num_reference_changes, 
	sum(num_bot_edits) as num_bot_edits, 
	sum(num_anonymous_edits) as num_anonymous_edits, 
	sum(num_human_edits) as registered_user_edits
FROM type_split
GROUP BY individual_type;

alter table 
entity_type_stats
add column entity_type_label VARCHAR default '';

update entity_type_stats
set entity_type_label = label
from entity_labels_alias_description elad
where elad.qid = individual_type;

select individual_type, entity_type_label, count, num_revisions, num_value_changes, num_qualifier_changes, num_reference_changes, num_bot_edits, num_anonymous_edits, registered_user_edits
from entity_type_stats
order by count desc;
