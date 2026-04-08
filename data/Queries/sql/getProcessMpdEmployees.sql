select fk_processo as process_id, 
fk_secao, 
fk_tipo_posto, 
fk_colaborador as employee_id, 
semana, 
gera_horario_ind, 
existe_horario_ind 
from table(WFM.S_PROCESSO.GET_PROCESS_MPD_EMPLOYEES({process_id}))
