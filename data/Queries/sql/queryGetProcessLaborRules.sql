SELECT
    cplr.PROCESS_ID,
    cplr.LABOR_UNION_ID,
    cplr.CONTRACT_ID,
    cplr.BEGIN_DATE,
    cplr.END_DATE,
    cplr.RULE_CODE,
    cplr.RULE_ID,
    cplr.RULE_HEAD_ID,
    cplr.PRIORITY,
    cplr.ORDER_SEQ,
    cplr.FIELD_CODE,
    cplr.RULE_FIELD_ID,
    cplr.FIELD_TYPE,
    cplr.VALUE
FROM wfm.core_process_labor_rules cplr
WHERE cplr.PROCESS_ID = {process_id}
