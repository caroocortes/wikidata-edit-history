create table stats_properties as
select 
	property_id, property_label, 
	count(distinct entity_id) as count_entities, 
	count(*) as count_changes, 
	count(*) filter (where is_reverted = 1) as count_reverted, 
	count(*) filter (where action = 'CREATE') as count_create,
	count(*) filter (where action = 'DELETE') as count_delete,
	count(*) filter (where action = 'UPDATE') as count_update
from value_change
where change_target = ''
group by property_id, property_label