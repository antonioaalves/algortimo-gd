select CLOSEDDAY as schedule_day, 
DAYTYPE as tipo_dia, 
FIXEDDAY as fixedday
from  wfm.core_closed_days
where  FK_UNIDADE = {unit_id}