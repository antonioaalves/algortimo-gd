select u.codigo fk_unidade, u.nome unidade, o.fk_secao, s.nome secao, p.codigo fk_tipo_posto, p.nome tipo_posto,
o.data, o.horario HORA_INI,
O.CALCULADOS PESSOAS_ESTIMADO, O.OBRIGATORIOS PESSOAS_MIN, O.QTDE_PDVS PESSOAS_FINAL
from wfm.esc_tmp_pdv_ideal o
inner join wfm.esc_tipo_posto p on p.codigo=o.fk_tipo_posto
inner join wfm.esc_secao s on s.codigo=p.fk_secao
inner join wfm.esc_unidade u on u.codigo=s.fk_unidade
where o.data between to_date({start_date},'yyyy-mm-dd') and to_date({end_date},'yyyy-mm-dd') and p.codigo={posto_id}