SELECT DISTINCT 
    cpehd.PROCESS_ID as process_id,
    cpehd.EMPLOYEE_ID as employee_id,
    ec.MATRICULA as matricula,
    cpehd.SCHEDULE_DAY as schedule_day,
    cpehd.TIPO_DIA as tipo_dia,
    cpehd.DESCANSO as descanso,
    cpehd.HORARIO_IND as horario_ind,
    cpehd.HORA_INI_1 as hora_ini_1,
    cpehd.HORA_FIM_1 as hora_fim_1,
    cpehd.HORA_INI_2 as hora_ini_2,
    cpehd.HORA_FIM_2 as hora_fim_2,
    cpehd.FK_HORARIO as fk_horario,
    cpehd.NRO_SEMANA as nro_semana,
    cpehd.DIA_SEMANA as dia_semana,
    cpehd.MINIMUMWORKDAY as minimumworkday,
    cpehd.MAXIMUMWORKDAY as maximumworkday
FROM wfm.CORE_PRO_EMP_HORARIO_DET cpehd
LEFT JOIN wfm.esc_colaborador ec ON ec.CODIGO = cpehd.EMPLOYEE_ID
WHERE cpehd.PROCESS_ID = {process_id}
    AND cpehd.SCHEDULE_DAY BETWEEN to_date({start_date}, 'YYYY-MM-DD') AND to_date({end_date}, 'YYYY-MM-DD')
    AND (
        (ec.CODIGO IN ({colab90ciclo}))  -- First query condition
        OR 
        (cpehd.TIPO_DIA = 'F')            -- Second query condition
    )