# -*- coding: utf-8 -*-
"""
Created on Tue Apr  8 09:42:07 2025

@author: jason.vogensen
"""

import os
import signal
import sys
import time
import datetime

# Import batch process components
from base_data_project.log_config import setup_logger, get_logger
from base_data_project.utils import create_components
from batch_process import run_batch_process
from src.config import CONFIG, PROJECT_NAME

# Initialize logger
setup_logger(
    project_name=PROJECT_NAME,
    log_level=CONFIG.get('log_level', 'INFO'),
    log_dir=CONFIG.get('log_dir', 'logs'),
    console_output=CONFIG.get('console_output', True)
)
logger = get_logger(PROJECT_NAME)

# SET INITIAL PATH -----------------------------------
import platform
import warnings
sis = platform.system()
path_ficheiros_global = os.getcwd() + "/"
os.chdir(path_ficheiros_global)
print("-------------------------------")
print(path_ficheiros_global)
print("-------------------------------")

# Add correct path to the functions
from src.orquestrador_functions.WFM_Process.Getters import get_process_by_status, get_process_by_id, get_total_process_by_status
from src.orquestrador_functions.WFM_Process.Setters import set_process_status, set_process_errors, set_process_param_status
from src.orquestrador_functions.Data_Handlers.GetGlobalData import get_all_params, get_gran_equi
from src.orquestrador_functions.Logs.message_loader import get_messages, set_messages
from src.orquestrador_functions.Classes.AlgorithmPrepClasses.ConnectionHandler import ConnectionHandler

# Setup database connection
connection_object = ConnectionHandler(path=path_ficheiros_global)
connection_object.connect_to_database()
connection = connection_object.get_connection()
connection = connection_object.ensure_connection()
print(f"connection: {connection}")
# GET LOG MESSAGES
df_msg = get_messages(path_ficheiros_global, lang='ES')
#print(f"df_msg: {df_msg}")
data_hoje = datetime.date.today()
# Suppress all warnings
warnings.filterwarnings("ignore")
#print(f"data_hoje: {data_hoje}")
# Redirect stdout and stderr to null device
#sys.stdout = open(os.devnull, 'w')
#sys.stderr = open(os.devnull, 'w')
# GET ARG BY API -----------------------------------
# Equivalent to commandArgs in R
if len(sys.argv) > 2:
    api_proc_id = sys.argv[1]
    api_user = sys.argv[2]
else:
    # Default values for development
    api_proc_id = 999
    api_user = 'WFM'
    
    ## PROCESS CODE
PROC_COD = 'AlgoritmoHorariosPython_Pai'

# VER QUANTAS THREADS
# Get parameters with defaults
all_params = get_all_params(path_ficheiros_global, connection = connection)
print(f"all_params: {all_params}")
GLOBAL_MAX_CONCURRENT_PROCESSES = next((item['NUMBERVALUE'] for item in all_params if item['SYS_P_NAME'] == 'GLOBAL_MAX_CONCURRENT_PROCESSES'), None)
if GLOBAL_MAX_CONCURRENT_PROCESSES is not None: 
    max(GLOBAL_MAX_CONCURRENT_PROCESSES,3)
else:
    GLOBAL_MAX_CONCURRENT_PROCESSES = 3
    
LOCAL_MAX_CONCURRENT_PROCESSES = next((item['NUMBERVALUE'] for item in all_params if item['SYS_P_NAME'] == 'LOCAL_MAX_CONCURRENT_PROCESSES'), None)
if LOCAL_MAX_CONCURRENT_PROCESSES is not None: 
    max(LOCAL_MAX_CONCURRENT_PROCESSES,1)
else:
    LOCAL_MAX_CONCURRENT_PROCESSES = 1
    
MAX_RETRIES = next((item['NUMBERVALUE'] for item in all_params if item['SYS_P_NAME'] == 'MAX_RETRIES'), None)
if MAX_RETRIES is not None: 
    max(MAX_RETRIES,1)
else:
    MAX_RETRIES = 1
    
RETRY_WAIT_TIME = next((item['NUMBERVALUE'] for item in all_params if item['SYS_P_NAME'] == 'RETRY_WAIT_TIME'), None)
if RETRY_WAIT_TIME is not None: 
    max(RETRY_WAIT_TIME,0)
else:
    RETRY_WAIT_TIME = 0


# GET PROCESSES -----------------------------------
sec_to_proc = get_process_by_status(path_ficheiros_global, 'WFM', 'MPD', '2', 'N', connection)
print(f"Nr of process {len(sec_to_proc)}")

local_processes = 0
retries = 0
if not sec_to_proc.empty:
    while retries < MAX_RETRIES:
        num_new_processes = len(sec_to_proc)
        num_processing_processes = get_total_process_by_status(path_ficheiros_global,connection)['TOTAL_P'].iloc[0]
        
        # MAX THREADS REACHED, MUST WAIT
        if num_processing_processes >= GLOBAL_MAX_CONCURRENT_PROCESSES:
            set_process_errors(
                connection=connection,
                pathOS=path_ficheiros_global, 
                user=api_user,
                fk_process=api_proc_id, 
                type_error='I', 
                process_type=PROC_COD, 
                error_code=None, 
                description=set_messages(df_msg, 'iniProc', {'1': api_user, '2': retries}),
                employee_id=None, 
                schedule_day=None
            )
            
            retries += 1
            time.sleep(RETRY_WAIT_TIME)
        else:
            # CALLS THE PROC CHILDS
            available_global_slots = GLOBAL_MAX_CONCURRENT_PROCESSES - num_processing_processes
            available_local_slots = LOCAL_MAX_CONCURRENT_PROCESSES - local_processes
            processes_to_start = min(available_global_slots, available_local_slots, num_new_processes)
            print(f"processes_to_start: {processes_to_start}")
            if processes_to_start > 0:
                for i in range(processes_to_start):
                    try:
                        wfm_proc_id = int(sec_to_proc.iloc[i]['CODIGO'])
                        wfm_user = str(sec_to_proc.iloc[i]['USER_CRIACAO'])
                        data_ini = sec_to_proc.iloc[i]['DATA_INI'].strftime('%Y-%m-%d') 
                        data_fim = sec_to_proc.iloc[i]['DATA_FIM'].strftime('%Y-%m-%d')
                        wfm_proc_colab = sec_to_proc.iloc[i]['FK_COLABORADOR']
                        res = set_process_status(connection,path_ficheiros_global, wfm_user, wfm_proc_id, status='P')
                        print(f"res: {res}")
                        if(wfm_proc_colab is not None):
                            wfm_proc_colab = int(wfm_proc_colab)
                        if res == 1:
                            set_process_errors(
                                connection,
                                pathOS=path_ficheiros_global, 
                                user=api_user,
                                fk_process=wfm_proc_id, 
                                type_error='I', 
                                process_type=PROC_COD, 
                                error_code=None, 
                                description=set_messages(df_msg, 'callSubproc', {'1': api_user, '2': i+1}),
                                employee_id=None, 
                                schedule_day=None
                            )
                            print(f"running the process")
                            
                            # Commit any pending transactions to prevent database lock conflicts
                            if connection:
                                connection.commit()
                            
                            # Call batch process function directly instead of subprocess
                            print(f"Starting direct function call for process {wfm_proc_id}")
                            
                            try:
                                # Create components for batch process
                                data_manager, process_manager = create_components(
                                    use_db=True,  # Always use database 
                                    no_tracking=False,
                                    config=CONFIG,
                                    project_name=PROJECT_NAME
                                )
                                
                                # Build external call dict with same parameters
                                external_call_dict = {
                                    'current_process_id': wfm_proc_id,
                                    'api_proc_id': api_proc_id,
                                    'wfm_proc_id': wfm_proc_id,
                                    'wfm_user': wfm_user,
                                    'start_date': str(data_ini),
                                    'end_date': str(data_fim),
                                }
                                
                                # Add wfm-proc-colab if it exists
                                if wfm_proc_colab is not None:
                                    external_call_dict['wfm_proc_colab'] = str(wfm_proc_colab)
                                
                                # Call the function directly
                                with data_manager:
                                    success = run_batch_process(
                                        data_manager=data_manager,
                                        process_manager=process_manager,
                                        algorithm="example_algorithm",
                                        external_call_dict=external_call_dict
                                    )
                                
                                if success:
                                    print(f"Direct function call completed successfully for process {wfm_proc_id}")
                                    local_processes += 1
                                else:
                                    print(f"Direct function call failed for process {wfm_proc_id}")
                                    
                            except Exception as e:
                                print(f"Direct function call failed: {e}")
                                logger.error(f"Error calling batch process directly: {e}", exc_info=True)

                        else:
                            print(f"erro ao chamar processo {wfm_proc_id}")
                            set_process_errors(
                                connection,
                                pathOS=path_ficheiros_global, 
                                user=api_user,
                                fk_process=wfm_proc_id, 
                                type_error='E', 
                                process_type=PROC_COD, 
                                error_code=None, 
                                description=set_messages(df_msg, 'errCallSubProc', {'1': api_user, '2': "Error calling process"}),
                                employee_id=None, 
                                schedule_day=None
                            )
                        
                        # Brief pause before starting next process
                        time.sleep(1)
                        
                    except Exception as e:
                        set_process_errors(
                            connection,
                            pathOS=path_ficheiros_global, 
                            user=api_user,
                            fk_process=wfm_proc_id if 'wfm_proc_id' in locals() else None, 
                            type_error='E', 
                            process_type=PROC_COD, 
                            error_code=None,
                            description=set_messages(df_msg, 'errCallSubProc', {'1': api_user, '2': str(e)}),
                            employee_id=None, 
                            schedule_day=None
                        )
                        
                        print(f'Error calling child proc {i+1}: {str(e)}')
            
            break
            
    if retries >= MAX_RETRIES:
        set_process_errors(
            connection,
            pathOS=path_ficheiros_global, 
            user=api_user,
            fk_process=api_proc_id if 'api_proc_id' in locals() else None, 
            type_error='I', 
            process_type=PROC_COD, 
            error_code=None, 
            description=set_messages(df_msg, 'errMaxThreads', {'1': api_user}),
            employee_id=None, 
            schedule_day=None
        )
        
# Database connection remains active for parent process
print("Parent process completed successfully - subprocess is independent")
sys.exit(0)