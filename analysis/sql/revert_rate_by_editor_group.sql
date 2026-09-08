select user_type, sum(total), sum(rev)
from (
	(with unnested as (
		select revision_id, property_id, value_id, trim(unnest(regexp_split_to_array(label, ','))) as new_label, is_reverted
		from updates_text_rev
	)
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from unnested u join revision r on r.revision_id = u.revision_id
	where new_label = 'textual_change'
	group by user_type)
	union all
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1)
	from value_change v join revision r on r.revision_id = v.revision_id
	where label = 'textual_change'
	group by user_type
)
group by user_type;

select user_type, sum(total), sum(rev)
from (
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from updates_entity_rev u join revision r on r.revision_id = u.revision_id
	where label = 'property_value_update'
	group by user_type
	union all
	(with unnested as (
		select revision_id, property_id, value_id, trim(unnest(regexp_split_to_array(label, ','))) as new_label, is_reverted
		from updates_text_rev
	)
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from unnested u join revision r on r.revision_id = u.revision_id
	where new_label = 'property_value_update'
	group by user_type)
	union all
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from value_change v join revision r on r.revision_id = v.revision_id
	where label = 'property_value_update'
	group by user_type
)
group by user_type;

select user_type, count(*), count(*) filter(where is_reverted = 1) as reverted
from value_change v join revision r on r.revision_id = v.revision_id
where label = 're_formatting'
group by user_type;

select user_type, sum(total), sum(rev)
from (
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from updates_entity_rev u join revision r on r.revision_id = u.revision_id
	where label = 'refinement'
	group by user_type
	union all
	(with unnested as (
		select revision_id, property_id, value_id, trim(unnest(regexp_split_to_array(label, ','))) as new_label, is_reverted
		from updates_text_rev
	)
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from unnested u join revision r on r.revision_id = u.revision_id
	where new_label = 'refinement'
	group by user_type)
	union all
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from value_change v join revision r on r.revision_id = v.revision_id
	where label = 'refinement'
	group by user_type
)
group by user_type;

select user_type, sum(total), sum(rev)
from (
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from updates_entity_rev u join revision r on r.revision_id = u.revision_id
	where label = 'unrefinement'
	group by user_type
	union all
	(with unnested as (
		select revision_id, property_id, value_id, trim(unnest(regexp_split_to_array(label, ','))) as new_label, is_reverted
		from updates_text_rev
	)
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from unnested u join revision r on r.revision_id = u.revision_id
	where new_label = 'unrefinement'
	group by user_type)
	union all
	select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as rev
	from value_change v join revision r on r.revision_id = v.revision_id
	where label = 'unrefinement'
	group by user_type
)
group by user_type;

select user_type, count(*), count(*) filter(where is_reverted = 1) as reverted
from value_change v join revision r on r.revision_id = v.revision_id
where action = 'CREATE'
group by user_type;

select user_type, count(*) as total, count(*) filter(where is_reverted = 1) as reverted
from value_change v join revision r on r.revision_id = v.revision_id
where action = 'DELETE'
group by user_type;