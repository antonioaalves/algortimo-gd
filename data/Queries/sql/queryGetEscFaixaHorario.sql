select fk_secao, 
aber_seg, 
fech_seg, 
aber_ter, 
fech_ter, 
aber_qua, 
fech_qua, 
aber_qui, 
fech_qui, 
aber_sex, 
fech_sex, 
aber_sab, 
fech_sab, 
aber_dom, 
fech_dom, 
aber_fer, 
fech_fer,
DATA_INI as data_ini, 
DATA_FIM as data_fim
from wfm.esc_faixa_horario
where 1=1
and fk_secao = {secao_id}
and data_ini <= to_date({start_date}, 'yyyy-mm-dd')
and data_fim >= to_date({end_date}, 'yyyy-mm-dd')