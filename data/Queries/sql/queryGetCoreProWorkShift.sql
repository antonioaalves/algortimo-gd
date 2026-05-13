SELECT
    cpws.process_id,
    cpws.scope,
    cpws.scope_id,
    cpws.shift,
    cpws.begin_date,
    cpws.end_date,
    cpws.start_time,
    cpws.end_time
FROM wfm.core_pro_work_shift cpws
WHERE cpws.process_id = {process_id}
    AND cpws.scope IN ('S', 'C')
    AND cpws.deleted = 0
ORDER BY cpws.scope, cpws.scope_id, cpws.shift, cpws.begin_date
