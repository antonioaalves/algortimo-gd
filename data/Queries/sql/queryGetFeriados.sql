SELECT 
tt.data_feriado as schedule_day,
tt.tipo_feriado as tipo_feriado,
tt.fk_unidade as fk_unidade,
tt.fk_cidade as fk_cidade,
tt.fk_estado as fk_estado,
tt.fk_pais as fk_pais
FROM TABLE(wfm.S_FERIADO.obtem_dias_feriado_uni (
    I_USER => user,
    I_UNIDADE    => {unit_id},
    I_DATA_INI   => to_date({start_date},'YYYY-MM-DD'),
    I_DATA_FIM   => to_date({end_date},'YYYY-MM-DD')
)) tt
order by schedule_day