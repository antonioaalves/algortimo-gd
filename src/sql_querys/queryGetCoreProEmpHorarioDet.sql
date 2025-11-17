select employee_id, schedule_day, tipo_dia from wfm.core_pro_emp_horario_det
where process_id = {process_id}
and schedule_day BETWEEN to_date({start_date}, 'YYYY-MM-DD')  and to_date({end_date}, 'YYYY-MM-DD')
and tipo_dia in ('F', 'S')