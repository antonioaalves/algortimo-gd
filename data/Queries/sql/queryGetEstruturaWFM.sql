select u.codigo fk_unidade, u.nome unidade, s.codigo fk_secao, s.nome secao, p.codigo fk_tipo_posto, p.nome tipo_posto
from wfm.esc_secao s 
inner join wfm.esc_unidade u on u.codigo=s.fk_unidade
inner join wfm.esc_tipo_posto p on s.codigo=p.fk_secao
-- QUERY DE MAPEAMENTOS (estruturaWFM.txt)