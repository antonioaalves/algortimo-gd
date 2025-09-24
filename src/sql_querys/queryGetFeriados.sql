SELECT data, tipo 
FROM wfm.esc_feriado 
WHERE fk_unidade = {unit_id}
AND tipo IN (2, 3)
AND (data BETWEEN to_date({start_date},'yyyy-mm-dd') AND to_date({end_date},'yyyy-mm-dd') 
OR data < to_date('2000-12-31','yyyy-mm-dd'))
order by data