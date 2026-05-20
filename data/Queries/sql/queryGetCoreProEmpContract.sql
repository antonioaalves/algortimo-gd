SELECT
    cpec.employee_id,
    cpec.contract_id,
    cpec.laborunionid        AS labor_union,
    cpec.MINIMUMDAYSPERWEEK  AS min_dia_trab,
    cpec.MAXIMUMDAYSPERWEEK  AS max_dia_trab,
    cpec.MAXIMUMWORKLOAD     AS maximumworkload,
    cpec.MAXIMUMWORKDAY      AS maximumworkday,
    MIN(cpec.schedule_day)   AS begin_date,
    MAX(cpec.schedule_day)   AS end_date,
    ec.MATRICULA             AS matricula,
    ec.NOME                  AS nome,
    ec.data_admissao,
    ec.data_demissao
FROM wfm.core_pro_emp_contract cpec
LEFT JOIN wfm.esc_colaborador ec ON ec.CODIGO = cpec.employee_id
WHERE cpec.schedule_day BETWEEN to_date({start_date}, 'YYYY-MM-DD') AND to_date({end_date}, 'YYYY-MM-DD')
    AND cpec.process_id = {process_id}
    AND cpec.employee_id IN ({colabs_id})
GROUP BY
    cpec.employee_id,
    cpec.contract_id,
    cpec.laborunionid,
    cpec.MINIMUMDAYSPERWEEK,
    cpec.MAXIMUMDAYSPERWEEK,
    cpec.MAXIMUMWORKLOAD,
    cpec.MAXIMUMWORKDAY,
    ec.MATRICULA,
    ec.NOME,
    ec.data_admissao,
    ec.data_demissao
