select 
u.codigo as fk_unidade, 
u.nome as nome_unidade, 
s.codigo as fk_secao, 
s.nome as nome_secao, 
p.codigo as fk_tipo_posto, 
p.nome as nome_tipo_posto,
u.fk_pais as fk_pais,
ct.name as nome_pais,
u.fk_estado as fk_estado,
u.fk_cidade as fk_cidade
from wfm.esc_secao s 
inner join wfm.esc_unidade u on u.codigo=s.fk_unidade
inner join wfm.esc_tipo_posto p on s.codigo=p.fk_secao
inner join wfm.core_country ct on u.fk_pais=ct.id
where s.codigo = {secao_id}
-- QUERY DE MAPEAMENTOS (estruturaWFM.txt)