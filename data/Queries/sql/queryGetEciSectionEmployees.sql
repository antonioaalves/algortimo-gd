select fk_colaborador as employee_id, matricula 
from wfm.d_colaborador_lista_t
where fk_secao = {secao_id}
