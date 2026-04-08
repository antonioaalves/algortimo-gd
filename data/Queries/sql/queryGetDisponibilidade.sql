select 
process_id as process_id,
employee_id as employee_id,
schedule_day as schedule_day,
hora_inicio as hora_ini,
hora_fim as hora_fim 
from wfm.core_pro_emp_availability
where process_id = {process_id}
and schedule_day BETWEEN to_date({start_date},'yyyy-mm-dd') AND to_date({end_date},'yyyy-mm-dd')
and employee_id IN ({colabs_id})