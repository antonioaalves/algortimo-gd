select FK_UNIDADE ,
  FK_SECAO ,
  FK_GRUPO ,
  FK_TIPO_POSTO ,
  SYS_P_NAME ,
  CHARVALUE ,
  NUMBERVALUE ,
  DATEVALUE 
from wfm.CORE_ALG_PARAMETERS
where fk_unidade = {unit_id} or fk_unidade is null