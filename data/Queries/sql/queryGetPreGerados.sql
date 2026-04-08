select FK_EMP, SCHEDULE_DT, SCHED_SUBTYPE, IND
from  wfm.core_algorithm_pregerados
where  1=1 
AND SCHEDULE_DT BETWEEN to_date({start_date},'yyyy-mm-dd') and to_date(,'yyyy-mm-dd')