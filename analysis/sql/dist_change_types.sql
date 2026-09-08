-- statement changes
create index idx_value_change_action on value_change(action);
create index idx_value_change_action_label on value_change(action, label);
create index idx_value_change_action_label_datatype on value_change(action, label, new_datatype, old_datatype);
create index idx_value_change_action_datatype on value_change(action, new_datatype, old_datatype);
create table value_change_stats as
select 
    count(*) FILTER(WHERE action = 'CREATE') as count_insertion,
    count(*) FILTER(WHERE action = 'CREATE' and is_reverted = 1) as count_create_reverted,
    count(*) FILTER(WHERE action = 'CREATE' and is_reverted = 0 and reversion = 0) as count_create_non_reverted,

    count(*) FILTER(WHERE action = 'DELETE') as count_deletion,
    count(*) FILTER(WHERE action = 'DELETE' and is_reverted = 1) as count_delete_reverted,
    count(*) FILTER(WHERE action = 'DELETE' and is_reverted = 0 and reversion = 0) as count_delete_non_reverted,

    count(*) FILTER(WHERE action = 'UPDATE' and label != '') as count_updates,
    count(*) FILTER(WHERE action = 'UPDATE' and label != '' and is_reverted = 1) as count_updates_reverted,
    count(*) FILTER(WHERE action = 'UPDATE' and label != '' and is_reverted = 0 and reversion = 0) as count_updates_non_reverted,

    count(*) FILTER(WHERE action = 'UPDATE' and label = 'property_value_update') as count_rb_property_value_update,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 'property_value_update' and is_reverted = 1) as count_rb_property_value_update_reverted,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 'property_value_update' and is_reverted = 0 and reversion = 0) as count_rb_property_value_update_non_reverted,

    count(*) FILTER(WHERE action = 'UPDATE' and label = 're_formatting') as count_rb_re_formatting,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 're_formatting' and is_reverted = 1) as count_rb_re_formatting_reverted,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 're_formatting' and is_reverted = 0 and reversion = 0) as count_rb_re_formatting_non_reverted,

    count(*) FILTER(WHERE action = 'UPDATE' and label = 'textual_change') as count_rb_textual_change,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 'textual_change' and is_reverted = 1) as count_rb_textual_change_reverted,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 'textual_change' and is_reverted = 0 and reversion = 0) as count_rb_textual_change_non_reverted,

    count(*) FILTER(WHERE action = 'UPDATE' and label = 'refinement') as count_rb_refinement,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 'refinement' and is_reverted = 1) as count_rb_refinement_reverted,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 'refinement' and is_reverted = 0 and reversion = 0) as count_rb_refinement_non_reverted,

    count(*) FILTER(WHERE action = 'UPDATE' and label = 'unrefinement') as count_rb_unrefinement,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 'unrefinement' and is_reverted = 1) as count_rb_unrefinement_reverted,
    count(*) FILTER(WHERE action = 'UPDATE' and label = 'unrefinement' and is_reverted = 0 and reversion = 0) as count_rb_unrefinement_non_reverted
from value_change;

create table updates_same_datatype_stats as
select 
    case 
        -- when new_datatype in ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 'wikibase-lexeme', 'wikibase-sense', 'wikibase-form', 'entity-schema') then 'entity'
        when new_datatype in ('string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation') then 'text'
        else new_datatype
    end as new_datatype,
    label,
    count(*) as count_updates,
    count(*) FILTER(WHERE is_reverted = 1) as count_reverted,
    count(*) FILTER(WHERE is_reverted = 0 and reversion = 0) as count_non_reverted
from value_change
where 
-- same data type value updates
action = 'UPDATE' and (new_datatype = old_datatype) and label != ''
-- updates for entity are all on its own table...
and vc.new_datatype not in ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 'wikibase-lexeme', 
                                        'wikibase-sense', 'wikibase-form', 'entity-schema')
group by new_datatype, label
;

-- ML and rule-based classified entity changes
create table updates_entity_stats as
select 
    'updates_entity' as table_name,
    count(*) as count_updates,

    -- classified with rule-based
    count(*) FILTER(WHERE rb) as count_rb_updates,
    count(*) FILTER(WHERE rb and label = 'refinement') as count_rb_refinement,
    count(*) FILTER(WHERE rb and label = 'refinement' and is_reverted = 1) as count_rb_refinement_reverted,
    count(*) FILTER(WHERE rb and label = 'refinement' and is_reverted = 0 and reversion = 0) as count_rb_refinement_non_reverted,

    count(*) FILTER(WHERE rb and label = 'unrefinement') as count_rb_unrefinement,
    count(*) FILTER(WHERE rb and label = 'unrefinement' and is_reverted = 1) as count_rb_unrefinement_reverted,
    count(*) FILTER(WHERE rb and label = 'unrefinement' and is_reverted = 0 and reversion = 0) as count_rb_unrefinement_non_reverted,

    count(*) FILTER(WHERE rb and label = 'property_value_update') as count_rb_property_value_update,
    count(*) FILTER(WHERE rb and label = 'property_value_update' and is_reverted = 1) as count_rb_property_value_update_reverted,
    count(*) FILTER(WHERE rb and label = 'property_value_update' and is_reverted = 0 and reversion = 0) as count_rb_property_value_update_non_reverted,

    -- classified by being on labeled dataset
    count(*) FILTER(WHERE gs and label = 'refinement') as count_gs_refinement,
    count(*) FILTER(WHERE gs and label = 'refinement' and is_reverted = 1) as count_gs_refinement_reverted,
    count(*) FILTER(WHERE gs and label = 'refinement' and is_reverted = 0 and reversion = 0) as count_gs_refinement_non_reverted,

    count(*) FILTER(WHERE gs and label = 'unrefinement') as count_gs_unrefinement,
    count(*) FILTER(WHERE gs and label = 'unrefinement' and is_reverted = 1) as count_gs_unrefinement_reverted,
    count(*) FILTER(WHERE gs and label = 'unrefinement' and is_reverted = 0 and reversion = 0) as count_gs_unrefinement_non_reverted,

    count(*) FILTER(WHERE gs and label = 'property_value_update') as count_gs_property_value_update,
    count(*) FILTER(WHERE gs and label = 'property_value_update' and is_reverted = 1) as count_gs_property_value_update_reverted,
    count(*) FILTER(WHERE gs and label = 'property_value_update' and is_reverted = 0 and reversion = 0) as count_gs_property_value_update_non_reverted,

    -- classified by machine learning
    count(*) FILTER(WHERE not gs and not rb and label = 'refinement') as count_ml_refinement,
    count(*) FILTER(WHERE not gs and not rb and label = 'refinement' and is_reverted = 1) as count_ml_refinement_reverted,
    count(*) FILTER(WHERE not gs and not rb and label = 'refinement' and is_reverted = 0 and reversion = 0) as count_ml_refinement_non_reverted,

    count(*) FILTER(WHERE not gs and not rb and label = 'unrefinement') as count_ml_unrefinement,
    count(*) FILTER(WHERE not gs and not rb and label = 'unrefinement' and is_reverted = 1) as count_ml_unrefinement_reverted,
    count(*) FILTER(WHERE not gs and not rb and label = 'unrefinement' and is_reverted = 0 and reversion = 0) as count_ml_unrefinement_non,

    count(*) FILTER(WHERE not gs and not rb and label = 'property_value_update') as count_ml_property_value_update,
    count(*) FILTER(WHERE not gs and not rb and label = 'property_value_update' and is_reverted = 1) as count_ml_property_value_update_reverted,
    count(*) FILTER(WHERE not gs and not rb and label = 'property_value_update' and is_reverted = 0 and reversion = 0) as count_ml_property_value_update_non_reverted

from updates_entity ue join value_change vc on vc.revision_id = ue.revision_id and vc.property_id = ue.property_id and vc.value_id = ue.value_id
where 
    vc.action = 'UPDATE' 
    and vc.new_datatype in ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 'wikibase-lexeme', 
                                        'wikibase-sense', 'wikibase-form', 'entity-schema')
    and vc.old_datatype in ('wikibase-item', 'wikibase-entityid', 'wikibase-property', 'wikibase-lexeme', 
                                        'wikibase-sense', 'wikibase-form', 'entity-schema')
;

-- ML classified text changes
create table updates_text_stats as
select 
    'updates_text' as table_name,
    count(*) as count_ml_updates,

    -- classified by being on labeled dataset
    count(*) FILTER(WHERE gs and label = 'refinement') as count_gs_refinement,
    count(*) FILTER(WHERE gs and label = 'refinement' and is_reverted = 1) as count_gs_refinement_reverted,
    count(*) FILTER(WHERE gs and label = 'refinement' and is_reverted = 0 and reversion = 0) as count_gs_refinement_non_reverted,

    count(*) FILTER(WHERE gs and label = 'unrefinement') as count_gs_unrefinement,
    count(*) FILTER(WHERE gs and label = 'unrefinement' and is_reverted = 1) as count_gs_unrefinement_reverted,
    count(*) FILTER(WHERE gs and label = 'unrefinement' and is_reverted = 0 and reversion = 0) as count_gs_unrefinement_non_reverted,

    count(*) FILTER(WHERE gs and label = 'property_value_update') as count_gs_property_value_update,
    count(*) FILTER(WHERE gs and label = 'property_value_update' and is_reverted = 1) as count_gs_property_value_update_reverted,
    count(*) FILTER(WHERE gs and label = 'property_value_update' and is_reverted = 0 and reversion = 0) as count_gs_property_value_update_non_reverted,
    
    count(*) FILTER(WHERE gs and label = 'textual_change') as count_gs_textual_change,
    count(*) FILTER(WHERE gs and label = 'textual_change' and is_reverted = 1) as count_gs_textual_change_reverted,
    count(*) FILTER(WHERE gs and label = 'textual_change' and is_reverted = 0 and reversion = 0) as count_gs_textual_change_non_reverted,

    -- classified by machine learning
    count(*) FILTER(WHERE not gs and not rb and label = 'refinement') as count_ml_refinement,
    count(*) FILTER(WHERE not gs and not rb and label = 'refinement' and is_reverted = 1) as count_ml_refinement_reverted,
    count(*) FILTER(WHERE not gs and not rb and label = 'refinement' and is_reverted = 0 and reversion = 0) as count_ml_refinement_non_reverted,

    count(*) FILTER(WHERE not gs and not rb and label = 'unrefinement') as count_ml_unrefinement,
    count(*) FILTER(WHERE not gs and not rb and label = 'unrefinement' and is_reverted = 1) as count_ml_unrefinement_reverted,
    count(*) FILTER(WHERE not gs and not rb and label = 'unrefinement' and is_reverted = 0 and reversion = 0) as count_ml_unrefinement_non_reverted,

    count(*) FILTER(WHERE not gs and not rb and label = 'property_value_update') as count_ml_property_value_update,
    count(*) FILTER(WHERE not gs and not rb and label = 'property_value_update' and is_reverted = 1) as count_ml_property_value_update_reverted,
    count(*) FILTER(WHERE not gs and not rb and label = 'property_value_update' and is_reverted = 0 and reversion = 0) as count_ml_property_value_update_non_reverted,

    count(*) FILTER(WHERE not gs and not rb and label = 'textual_change') as count_ml_textual_change,
    count(*) FILTER(WHERE not gs and not rb and label = 'textual_change' and is_reverted = 1) as count_ml_textual_change_reverted,
    count(*) FILTER(WHERE not gs and not rb and label = 'textual_change' and is_reverted = 0 and reversion = 0) as count_ml_textual_change_non_reverted

from updates_text ut join value_change vc on vc.revision_id = ut.revision_id and vc.property_id = ut.property_id and vc.value_id = ut.value_id
where 
    vc.action = 'UPDATE' 
    and vc.new_datatype in ('string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation')
    and vc.old_datatype in ('string', 'external-id', 'url', 'commonsMedia', 'geo-shape', 'tabular-data', 'math', 'musical-notation')
;