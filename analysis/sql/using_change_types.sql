-- DATE REFINEMENTS REVERTED BECAUSE OF LACK OF SOURCE
-- total number of refinements reverted
select
	count(*) as total,
	count(*) filter(where is_reverted = 1) as revs
from value_change  f
where label = 'refinement' and new_datatype = 'time' and old_datatype = 'time';

-- date reifnements that have been reveted and their reversion comment contains "non-WP source(s)"
select
	count(*) as reverted_with_comment 
from value_change f join revision r on r.revision_id = f.revision_id
where f.label = 'refinement' and new_datatype = 'time' and old_datatype = 'time' and revision_id_reversion in (
select revision_id
from revision 
where comment ilike '%non-WP source(s)%'
);


-- OSCILLATIONS FOR REFINEMENTS/UNREFINEMENTS IN ENTITY CHANGES
WITH entity_changes AS (
    SELECT 
        r.entity_id,
        f.property_id,
		f.value_id,
        f.property_label,
        f.old_value_label,
        f.new_value_label,
        f.label,
        r.timestamp,
        -- check if next change on same entity+property+value is the opposite
        LEAD(f.label) OVER (
            PARTITION BY r.entity_id, f.property_id, f.value_id
            ORDER BY r.timestamp asc
        ) AS next_label
    FROM features_entity f
    JOIN revision r ON r.revision_id = f.revision_id
    WHERE f.label IN ('refinement', 'unrefinement')
    AND f.is_reverted = 0
),
oscillations AS (
    SELECT *
    FROM entity_changes
    WHERE (label = 'refinement' AND next_label = 'unrefinement')
    OR (label = 'unrefinement' AND next_label = 'refinement')
)
SELECT 
    entity_id,
    property_id,
	value_id,
    property_label,
    COUNT(*) as oscillation_count,
    -- show the sequence of values
    array_agg(old_value_label || ' → ' || new_value_label 
        ORDER BY timestamp) as change_sequence
FROM oscillations
GROUP BY entity_id, property_id, property_label, value_id
HAVING COUNT(*) >= 2  -- at least 2 oscillations
ORDER BY oscillation_count DESC;

-- TEXT ANALYSIS
-- user type distribution for textual changes
select user_type, sum(total)
from (
	(with unnested as (
		select revision_id, property_id, value_id, trim(unnest(regexp_split_to_array(label, ','))) as new_label, is_reverted
		from updates_text_rev
	)
	select user_type, count(*) as total
	from unnested u join revision r on r.revision_id = u.revision_id
	where new_label = 'textual_change'
	group by user_type)
	union all
	select user_type, count(*) as total
	from value_change v join revision r on r.revision_id = v.revision_id
	where 
		label = 'textual_change' and new_datatype in ('string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation') 
		and old_datatype in ('string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation')
	group by user_type
)
group by user_type;

-- refinement properties
select property_id, sum(total)
from (
	(with unnested as (
		select revision_id, property_id, value_id, trim(unnest(regexp_split_to_array(label, ','))) as new_label
		from updates_text_rev
	)
	select property_id, count(*) as total
	from unnested u 
	where new_label = 'refinement'
	group by property_id)
	union all
	select property_id, count(*) as total
	from value_change v
	where label = 'refinement' and new_datatype in ('string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation') 
		and old_datatype in ('string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation')
	group by property_id
)
group by property_id
order by sum(total) desc;

-- entities with at least 1 UPDATE to their label or description
select count(distinct entity_id)
from value_change
where property_id = -1 or property_id = -2 and action = 'UPDATE';

-- ENTITY VALUE UPDATES WHERE OLD VALUE IS REDIRECTED ENTITY
select count(*)
from updates_entity u, revision r
where label = 'property_value_update' and new_value_label = old_value_label 
and r.q_id_redirect != '' 
and u.old_value->>0 = 'Q' || r.entity_id::text -- old value is an entity that was redirected
and u.new_value->>0 = 'Q' || r.q_id_redirect; -- new value is the new entity being redirected to


--- TIME value updates
select branch, count(*)
from value_change
where label = 'property_value_update' and new_datatype = 'time' and old_datatype = 'time'
group by branch;

SELECT count(*)
from value_change
where label = 'property_value_update' and new_datatype = 'time' and old_datatype = 'time';


--- QUANTITY
select branch, count(*)
from value_change
where label = 'unrefinement' and new_datatype = 'quantity' and old_datatype = 'quantity'
group by branch;


-- GLOBECOORDINATE
select branch, count(*)
from value_change
where label = 'unrefinement' and new_datatype = 'quantity' and old_datatype = 'quantity'
group by branch;

select count(*), label
from value_change
where new_datatype = 'globecoordinate' and old_datatype = 'globecoordinate' and label ilike '%property_value_update%'
group by label;