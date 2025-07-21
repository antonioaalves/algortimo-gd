declare   
  i_user VARCHAR2(242) := :i_user; 
  i_fk_process NUMBER := :i_fk_process; 
  i_type_error VARCHAR2(242) := :i_type_error;  
  i_process_type VARCHAR2(242) := :i_process_type;  
  i_error_code VARCHAR2(242) := :i_error_code;  
  i_description VARCHAR2(242) := :i_description;   
  i_employee_id NUMBER := :i_employee_id;   
  i_schedule_day DATE := to_date(:i_schedule_day, 'yyyy-mm-dd');    
begin  
  wfm.S_PROCESSO.SET_PROCESS_ERRORS(
      i_user, i_fk_process, i_type_error, i_process_type, i_error_code, 
      i_description, i_employee_id, i_schedule_day
  );  
  COMMIT;  
end;