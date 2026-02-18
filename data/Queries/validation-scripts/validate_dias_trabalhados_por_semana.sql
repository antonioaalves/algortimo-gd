-- Query that tells the user how many weeks did a employee more (or less) than the minimum and maximum days of work per week

select 
    matricula,
    count(*) as semanas_fora_intervalo
from (
    select 
        cav.EMP as matricula,
        to_number(to_char(ipsa.schedule_dt, 'IW')) as week_number,
        count(ipsa.schedule_dt) as dias_trabalhados,
        min(cav.MIN_DIAS_TRABALHADOS) as min_dias_trabalhados,
        max(cav.MAX_DIAS_TRABALHADOS) as max_dias_trabalhados
    from wfm.int_pre_schedule_algorithm ipsa
    inner join wfm.core_algorithm_variables cav on cav.EMP = ipsa.employee_id
    where 1=1 
    and ipsa.fk_processo = to_char(:process_id)
    and ipsa.sched_type not in ('F')
    and to_number(to_char(ipsa.schedule_dt, 'IW')) not in (1, 53)
    group by cav.EMP, to_number(to_char(ipsa.schedule_dt, 'IW'))
)
where dias_trabalhados not between min_dias_trabalhados and max_dias_trabalhados
group by matricula
order by matricula;