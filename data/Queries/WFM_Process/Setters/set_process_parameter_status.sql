declare 

i_user VARCHAR2(242) := :i_user_input;
i_process_id NUMBER := :i_process_id_input;
i_new_status VARCHAR2(242) := :i_new_status_input; 
i NUMBER;

begin

i:=S_PROCESSO.SET_PROCESS_PARAMETER_STATUS(i_user,i_process_id,i_new_status);

COMMIT;

end;