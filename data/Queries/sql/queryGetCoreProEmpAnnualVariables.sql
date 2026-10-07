SELECT
    cpeav.process_id,
    cpeav.employee_id,
    cpeav.year,
    cpeav.begin_date,
    cpeav.end_date,
    cpeav.rule_field_code,
    cpeav.value,
    cpeav.apply_ind
FROM wfm.core_pro_emp_annual_variables cpeav
WHERE cpeav.process_id = {process_id}
    AND cpeav.employee_id IN ({colabs_id})
    AND cpeav.rule_field_code IN (
        'NUM_DAYS_OFF_SUNDAY_YEAR',
        'NUM_DAYS_OFF_WEEKEND_YEAR',
        'NUM_DAYS_OFF_SAT_YEAR',
        'NUM_DAYS_OFF_SAT_OR_SUN_YEAR'
    )
ORDER BY cpeav.employee_id, cpeav.begin_date, cpeav.year, cpeav.rule_field_code
