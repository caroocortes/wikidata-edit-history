create table entity_stats_sa as
select 
    count(*) as num_entities,
    SUM(num_value_changes) as sum_num_value_changes,
    SUM(num_value_changes_updates) as sum_num_value_changes_updates,
    SUM(num_value_change_creates) as sum_num_value_change_creates,
    SUM(num_bot_edits) as sum_num_bot_edits,
    SUM(num_revisions) as sum_num_revisions,
    COUNT(*) FILTER(WHERE num_value_changes_updates = 0) as num_entities_with_no_updates,
from entity_stats_sa;

create table entity_stats_ao as
select 
    count(*) as num_entities,
    SUM(num_value_changes) as sum_num_value_changes,
    SUM(num_value_changes_updates) as sum_num_value_changes_updates,
    SUM(num_value_change_creates) as sum_num_value_change_creates,
    SUM(num_bot_edits) as sum_num_bot_edits,
    SUM(num_revisions) as sum_num_revisions,
    COUNT(*) FILTER(WHERE num_value_changes_updates = 0) as num_entities_with_no_updates,
from entity_stats_ao;

create table entity_stats_less as
select 
    count(*) as num_entities
from entity_stats_less;