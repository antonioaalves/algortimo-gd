select 
fk_processo as process_id,
fk_unidade,
fk_secao,
fk_tipo_posto,
fk_colaborador as employee_id,
prioridade_folgas,
gerar_dados
FROM TABLE(wfm.S_PROCESSO.GET_PROCESS_VALID_EMPLOYEES_V2({process_id}))