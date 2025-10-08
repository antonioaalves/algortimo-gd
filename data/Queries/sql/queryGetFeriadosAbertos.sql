SELECT
fk_unidade, fk_pais, fk_estado, fk_cidade, TO_CHAR(data,'YYYY-MM-DD') AS database, descricao, tipo, feriado_fixo
FROM wfm.esc_feriado 
WHERE  tipo IN (2)

