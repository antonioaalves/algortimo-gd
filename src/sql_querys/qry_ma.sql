SELECT ec.codigo,loja, secao, puesto, convenio, cav.nome, emp, min_dias_trabalhados, max_dias_trabalhados, tipo_de_turno, seq_turno, t_total, l_total, DYF_MAX_T, dofhc, Lq,Q, fds_cal_2d, fds_cal_3d, d_cal_xx, semana_1,OUT, ciclo as CICLO, ec.data_admissao, ec.data_demissao, fk_tipo_posto, h_tm_in, h_tm_out, h_tt_in, h_tt_out, h_seg_in, h_seg_out, h_ter_in, h_ter_out, h_qua_in, h_qua_out, h_qui_in, h_qui_out, h_sex_in, h_sex_out, h_sab_in, h_sab_out, h_dom_in, h_dom_out, h_fer_in, h_fer_out, limite_superior_manha, limite_inferior_tarde 
FROM wfm.core_algorithm_variables cav
inner join WFM.ESC_COLABORADOR ec
on ec.matricula = cav.emp
WHERE ec.CODIGO IN ({colabs_id})