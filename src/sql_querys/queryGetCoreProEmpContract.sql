select employee_id, schedule_day, contract_id, MAXIMUMWORKLOAD, MAXIMUMDAYSPERWEEK, MAXIMUMWORKDAY from wfm.core_pro_emp_contract
where schedule_day BETWEEN to_date({start_date}, 'YYYY-MM-DD')  and to_date({end_date}, 'YYYY-MM-DD')
and process_id = {process_id}
and employee_id in ({colabs_id})