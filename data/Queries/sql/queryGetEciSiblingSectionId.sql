select s.codigo as fk_secao, s.nome as nome_secao
from wfm.esc_secao s 
where s.fk_unidade = {unit_id}
and UPPER(s.nome) LIKE '%' || UPPER({sibling_section_name})
