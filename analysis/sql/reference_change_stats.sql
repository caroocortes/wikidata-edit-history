-- reference insertion & deletion
-- reference_change stores changes to reference values, so to count the insertion/deletion of the whole reference I need to count
-- per (revision_id, property_id, value_id, ref_hash, ref_property_id)
create index idx_reference_change_reference_id on reference_change(revision_id, property_id, value_id, ref_property_id, ref_hash);
create table reference_change_stats as
with refs_ids as (
    select distinct on (revision_id, property_id, value_id, ref_property_id, ref_hash) 
            revision_id, property_id, value_id, ref_property_id, ref_hash, action
    from reference_change
    order by revision_id, property_id, value_id, ref_property_id, ref_hash
)
select 
    'reference' as table_name, 
    count(*) FILTER(WHERE action = 'CREATE') as count_insertion,
    count(*) FILTER(WHERE action = 'DELETE') as count_deletion
from refs_ids;