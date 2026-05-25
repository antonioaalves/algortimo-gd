WITH contract_days AS (
    SELECT
        cpec.employee_id,
        cpec.contract_id,
        cpec.laborunionid,
        cpec.MINIMUMDAYSPERWEEK,
        cpec.MAXIMUMDAYSPERWEEK,
        cpec.MAXIMUMWORKLOAD,
        cpec.MAXIMUMWORKDAY,
        cpec.schedule_day,
        cpec.schedule_day - ROW_NUMBER() OVER (
            PARTITION BY cpec.employee_id, cpec.contract_id
            ORDER BY cpec.schedule_day
        ) AS period_grp
    FROM wfm.core_pro_emp_contract cpec
    WHERE cpec.schedule_day BETWEEN to_date({start_date}, 'YYYY-MM-DD') AND to_date({end_date}, 'YYYY-MM-DD')
        AND cpec.process_id = {process_id}
        AND cpec.employee_id IN ({colabs_id})
)
SELECT
    cd.employee_id,
    cd.contract_id,
    cd.laborunionid        AS labor_union,
    cd.MINIMUMDAYSPERWEEK  AS min_dia_trab,
    cd.MAXIMUMDAYSPERWEEK  AS max_dia_trab,
    cd.MAXIMUMWORKLOAD     AS maximumworkload,
    cd.MAXIMUMWORKDAY      AS maximumworkday,
    MIN(cd.schedule_day)   AS begin_date,
    MAX(cd.schedule_day)   AS end_date,
    ec.MATRICULA           AS matricula,
    ec.NOME                AS nome,
    ec.data_admissao,
    ec.data_demissao
FROM contract_days cd
LEFT JOIN wfm.esc_colaborador ec ON ec.CODIGO = cd.employee_id
GROUP BY
    cd.employee_id,
    cd.contract_id,
    cd.laborunionid,
    cd.MINIMUMDAYSPERWEEK,
    cd.MAXIMUMDAYSPERWEEK,
    cd.MAXIMUMWORKLOAD,
    cd.MAXIMUMWORKDAY,
    cd.period_grp,
    ec.MATRICULA,
    ec.NOME,
    ec.data_admissao,
    ec.data_demissao
