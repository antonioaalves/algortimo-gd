select CLOSEDDAY as DATA, DAYTYPE, FIXEDDAY
from  wfm.core_closed_days
where  FK_UNIDADE = {unit_id}