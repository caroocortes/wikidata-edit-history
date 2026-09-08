create index idx_qualifier_change_action_label on qualifier_change(action, label);
create table qualifier_change_stats as
select 
    'qualifier' as table_name, 
    count(*) FILTER(WHERE action = 'CREATE') as count_insertion,
    count(*) FILTER(WHERE action = 'DELETE') as count_deletion,
    count(*) FILTER(WHERE action = 'CREATE' and label = 'soft_deletion') as count_soft_deletion
from qualifier_change;