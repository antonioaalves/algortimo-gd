SELECT DISTINCT cpehd.PROCESS_ID,
		cpehd.EMPLOYEE_ID,
		ec.MATRICULA,
		cpehd.SCHEDULE_DAY,
		TIPO_DIA,
		DESCANSO,
		/*--HORA_INICIO
		--HORA_FIM*/
		HORARIO_IND,
		HORA_INI_1,
		HORA_FIM_1,
		HORA_INI_2,
		HORA_FIM_2,
		FK_HORARIO,
		NRO_SEMANA,
		DIA_SEMANA,
		MINIMUMWORKDAY,
		MAXIMUMWORKDAY
FROM wfm.CORE_PRO_EMP_HORARIO_DET  cpehd
/*--join wfm.core_pro_emp_horario_det cpehd on cpehd.PROCESS_ID = cpea.PROCESS_ID
--and cpehd.EMPLOYEE_ID = cpea.EMPLOYEE_ID 
--and cpehd.SCHEDULE_DAY = cpea.SCHEDULE_DAY  */
JOIN wfm.esc_colaborador ec ON ec.CODIGO = cpehd.EMPLOYEE_ID
WHERE cpehd.PROCESS_ID = {process_id}
AND ec.CODIGO in ({colab90ciclo})
AND cpehd.SCHEDULE_DAY BETWEEN to_date({start_date}, 'YYYY-MM-DD')  and to_date({end_date}, 'YYYY-MM-DD')



