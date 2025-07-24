select ea.CODIGO, FK_COLABORADOR, MATRICULA, DATA_INI, DATA_FIM, TIPO_AUSENCIA, FK_MOTIVO_AUSENCIA
from  wfm.esc_ausencia ea
inner join wfm.esc_colaborador ec  on ec.codigo = ea.fk_colaborador 
where  1=1
-- and {condition} 
and MATRICULA IN ({colabs_id})
and ea.data_exclusao is null
--QUERY PARA EXCEÇÕES DE QUANTIDADE (excecoesQuantidade.txt)