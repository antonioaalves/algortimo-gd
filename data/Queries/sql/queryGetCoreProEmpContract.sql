SELECT 
    cpec.employee_id,
    cpec.schedule_day,
    cpec.contract_id,
    cpec.MAXIMUMWORKLOAD as maximumworkload,
    cpec.MAXIMUMDAYSPERWEEK as maximumdaysperweek,
    cpec.MAXIMUMWORKDAY as maximumworkday,
    ec.MATRICULA as matricula
FROM wfm.core_pro_emp_contract cpec
LEFT JOIN wfm.esc_colaborador ec ON ec.CODIGO = cpec.employee_id
WHERE cpec.schedule_day BETWEEN to_date({start_date}, 'YYYY-MM-DD') AND to_date({end_date}, 'YYYY-MM-DD')
    AND cpec.process_id = {process_id}
    AND cpec.employee_id IN ({colabs_id})