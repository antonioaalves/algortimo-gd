select fk_secao, aber_seg, fech_seg, aber_ter, fech_ter, aber_qua, fech_qua, aber_qui, fech_qui, aber_sex, fech_sex, aber_sab, fech_sab, aber_dom, fech_dom, aber_fer, fech_fer,
DATA_INI, DATA_FIM
from wfm.esc_faixa_horario
where ativo='S' and fk_secao is not null

-- QUERY ESC_FAIXA_HORARIO SECAO (escFaixaHorario.txt)