select 
u.codigo as fk_unidade, 
u.nome as unidade, 
o.fk_secao as fk_secao, 
s.nome as secao, 
p.codigo as fk_tipo_posto, 
p.nome as tipo_posto,
o.data as schedule_day, 
o.horario as hora_ini,
O.CALCULADOS as pessoas_estimado, 
O.OBRIGATORIOS as pessoas_min, 
O.QTDE_PDVS as pessoas_final
from wfm.esc_tmp_pdv_ideal o
inner join wfm.esc_tipo_posto p on p.codigo=o.fk_tipo_posto
inner join wfm.esc_secao s on s.codigo=p.fk_secao
inner join wfm.esc_unidade u on u.codigo=s.fk_unidade
where o.data between to_date({start_date},'yyyy-mm-dd') and to_date({end_date},'yyyy-mm-dd') and p.codigo={posto_id}
order by schedule_day asc, hora_ini asc