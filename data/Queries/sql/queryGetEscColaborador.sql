select codigo as fk_colaborador, matricula from wfm.esc_colaborador
where 1=1
AND codigo in ({colabs_id})