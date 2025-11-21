select ea.CODIGO as codigo, 
FK_COLABORADOR as employee_id,
MATRICULA as matricula,
DATA_INI as data_ini,
DATA_FIM as data_fim,
TIPO_AUSENCIA as tipo_ausencia,
FK_MOTIVO_AUSENCIA as fk_motivo_ausencia
from  wfm.esc_ausencia ea
inner join wfm.esc_colaborador ec  on ec.codigo = ea.fk_colaborador 
where  1=1
-- and {condition} 
and MATRICULA IN ({colabs_id})
and data_ini BETWEEN to_date({start_date},'yyyy-mm-dd') AND to_date({end_date},'yyyy-mm-dd')
and data_fim BETWEEN to_date({start_date},'yyyy-mm-dd') AND to_date({end_date},'yyyy-mm-dd')
and ea.data_exclusao is null
AND TIPO_AUSENCIA not in ('AP')
order by data_ini
--QUERY PARA EXCEÇÕES DE QUANTIDADE (excecoesQuantidade.txt)