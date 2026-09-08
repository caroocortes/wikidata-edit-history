-- rank changes
create index idx_rank_change_action on rank_change(action);
create index idx_rank_change_action_label on rank_change(action, label);
create table rank_change_stats as
select 
    count(*) count_updates,
    count(*) FILTER(WHERE is_reverted = 1) as count_updates_reverted,
    count(*) FILTER(WHERE is_reverted = 0 and reversion = 0) as count_updates_non_reverted

    count(*) FILTER(WHERE label = 'soft_deletion') as count_soft_deletion,
    count(*) FILTER(WHERE label = 'soft_deletion' and is_reverted = 1) as count_soft_deletion_reverted,
    count(*) FILTER(WHERE label = 'soft_deletion' and is_reverted = 0 and reversion = 0) as count_soft_deletion_non_reverted,
    
    count(*) FILTER(WHERE label = 'soft_insertion') as count_soft_insertion,
    count(*) FILTER(WHERE label = 'soft_insertion' and is_reverted = 1) as count_soft_insertion_reverted,
    count(*) FILTER(WHERE label = 'soft_insertion' and is_reverted = 0 and reversion = 0) as count_soft_insertion_non_reverted
from rank_change
WHERE action = 'UPDATE';