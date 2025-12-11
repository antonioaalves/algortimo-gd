select 
EMPLOYEE_ID as employee_id,
SCHEDULE_DT as schedule_day,
SCHED_TYPE as sched_type 
from wfm.core_algorithm_daysoff 
where employee_id in ({colabs_id})
