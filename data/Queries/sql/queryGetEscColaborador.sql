select 
codigo as employee_id, 
matricula as matricula 
from wfm.esc_colaborador
where 1=1
AND codigo in ({colabs_id})