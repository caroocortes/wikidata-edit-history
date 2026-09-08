create table updates_text_rev as
select ut.*, vc.is_reverted, vc.reversion 
from updates_text ut join value_change vc on 
	ut.revision_id = vc.revision_id and 
	ut.property_id = vc.property_id and
	ut.value_id = vc.value_id;

select 
	trim(unnest(regexp_split_to_array(label, ','))) AS individual_label,
	count(*) as count_updates,
	count(*) FILTER(where is_reverted = 1) as reverted,
	count(*) FILTER(where is_reverted = 0 and reversion = 0) as non_reverted
from updates_text_rev
group by trim(unnest(regexp_split_to_array(label, ',')));