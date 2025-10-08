SELECT ec.matricula as EMPLOYEE_ID, 
    ec.codigo as fk_colaborador, 
    sa.SCHEDULE_DAY, 
    sa.TYPE, sa.SUBTYPE 
FROM WFM.CORE_PRE_SCHEDULE_ALGORITHM  sa
inner join wfm.esc_colaborador ec 
on ec.codigo = sa.EMPLOYEE_ID
WHERE EMPLOYEE_ID IN ({colabs})
AND schedule_day BETWEEN to_date({start_date},'yyyy-mm-dd') AND to_date({end_date},'yyyy-mm-dd')
and exclusion_date is null