select FK_UNIDADE as fk_unidade,
  FK_SECAO as fk_secao,
  FK_GRUPO as fk_grupo,
  FK_TIPO_POSTO as fk_tipo_posto,
  SYS_P_NAME as sys_p_name,
  CHARVALUE as charvalue,
  NUMBERVALUE as numbervalue,
  DATEVALUE as datevalue 
from wfm.CORE_ALG_PARAMETERS
where fk_unidade = {unit_id} or fk_unidade is null