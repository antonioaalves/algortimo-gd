-- Check if the employee has more consecutive days of work than the maximum allowed by the algorithm

select * from (
    select 
        fk_processo,
        employee_id,
        min(schedule_dt) as streak_start,
        max(schedule_dt) as streak_end,
        count(*) as consecutive_days,
        max(num_dias_cons) as num_dias_cons,
        case when count(*) > max(num_dias_cons) then 'VIOLATION' else 'OK' end as status
    from (
        select 
            ipsa.fk_processo, 
            ipsa.employee_id, 
            ipsa.schedule_dt,
            coalesce(cap_secao.numbervalue, cap_unidade.numbervalue) as num_dias_cons,
            ipsa.schedule_dt - row_number() over (partition by ipsa.employee_id order by ipsa.schedule_dt) as island
        from wfm.int_pre_schedule_algorithm ipsa
        inner join wfm.esc_processo ep on ipsa.fk_processo = ep.codigo
        inner join wfm.esc_secao es on es.codigo = ep.fk_secao
        left join wfm.core_alg_parameters cap_secao 
            on cap_secao.fk_secao = ep.fk_secao 
            and cap_secao.sys_p_name = 'NUM_DIAS_CONS'
            and cap_secao.fk_secao is not null
        left join wfm.core_alg_parameters cap_unidade 
            on cap_unidade.fk_unidade = es.fk_unidade
            and cap_unidade.sys_p_name = 'NUM_DIAS_CONS'
            and cap_unidade.fk_secao is null
        where ep.codigo = to_char(:process_id)
        and ipsa.sched_type not in ('F')
        and (ipsa.dt_create <= ep.data_alteracao or ipsa.dt_create is null)
    )
    group by fk_processo, employee_id, island
)
where status = 'VIOLATION'
order by employee_id, streak_start;