SELECT
    cpem.PROCESS_ID,
    cpem.EMPLOYEE_ID,
    cpem.SCHEDULE_DAY,
    cpem.RULE_HEAD_ID,
    cpem.RULE_CODE,
    cpem.RULE_FIELD_CODE,
    cpem.VALUE_OPT1,
    cpem.VALUE_OPT2
FROM wfm.core_pro_emp_mov cpem
WHERE cpem.PROCESS_ID = {process_id}
    AND cpem.EMPLOYEE_ID IN ({colabs_id})
