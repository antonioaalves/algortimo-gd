SELECT c.codigo AS worker,
       'Day_' || (TO_NUMBER(TO_CHAR(p.schedule_dt, 'DDD')) + 11) AS day_of_year,
       p.SCHED_TYPE,
       p.SCHED_SUBTYPE,
       p.dt_create,
       p.fk_processo
FROM wfm.int_pre_schedule_algorithm p
LEFT JOIN wfm.esc_colaborador c
    ON c.matricula = p.employee_id
WHERE p.fk_processo = :process_id

ORDER BY c.codigo, p.schedule_dt