SELECT ec.matricula as matricula, 
    ec.codigo as employee_id, 
    sa.SCHEDULE_DAY as schedule_day, 
    sa.TYPE as type, 
    sa.SUBTYPE as subtype 
FROM WFM.CORE_PRE_SCHEDULE_ALGORITHM  sa
inner join wfm.esc_colaborador ec 
on ec.codigo = sa.employee_id
WHERE employee_id IN ({colabs})
AND schedule_day BETWEEN to_date({start_date},'yyyy-mm-dd') AND to_date({end_date},'yyyy-mm-dd')
and exclusion_date is null