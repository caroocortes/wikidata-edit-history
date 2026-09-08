create table updates_entity_rev as
select ue.*, vc.is_reverted, vc.reversion 
from updates_entity ue join value_change vc on 
	ue.revision_id = vc.revision_id and 
	ue.property_id = vc.property_id and
	ue.value_id = vc.value_id;

create table updates_entity_rev_stats as
select label, 
		count(*) as total_counts,
		count(*) FILTER(where is_reverted = 1) as reverted,
		count(*) FILTER(where is_reverted = 0 and reversion = 0) as non_reverted
from updates_entity_rev
group by label;