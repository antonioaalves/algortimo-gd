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
import subprocess
# Set locale for time
# Python doesn't have an exact equivalent to R's Sys.setlocale("LC_TIME", "English")
# but we can use locale module if needed
# import locale
# locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')  # Adjust as needed for your OS

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
    
# GET LOG MESSAGES
df_msg = get_messages(path_ficheiros_global, lang='ES')

data_hoje = datetime.date.today()
# Suppress all warnings
warnings.filterwarnings("ignore")

# Redirect stdout and stderr to null device
sys.stdout = open(os.devnull, 'w')
sys.stderr = open(os.devnull, 'w')
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
sec_to_proc = get_process_by_status(path_ficheiros_global, 'WFM', 'ESC', 'PATH02', 'N', connection)
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
                path_os=path_ficheiros_global, 
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
            
            if processes_to_start > 0:
                for i in range(processes_to_start):
                    try:
                        wfm_proc_id = int(sec_to_proc.iloc[i]['CODIGO'])
                        wfm_user = str(sec_to_proc.iloc[i]['USER_CRIACAO'])
                        data_ini = sec_to_proc.iloc[i]['DATA_INI'].strftime('%d-%m-%Y') 
                        data_fim = sec_to_proc.iloc[i]['DATA_FIM'].strftime('%d-%m-%Y')
                        wfm_proc_colab = sec_to_proc.iloc[i]['FK_COLABORADOR']
                        res = set_process_status(connection,path_ficheiros_global, wfm_user, wfm_proc_id, status='P')
                        
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

                            # In Python we use subprocess instead of system
                            subprocess.Popen([
                                "python", "main.py", 
                                str(api_proc_id), 
                                str(wfm_proc_id),
                                wfm_user,
                                data_ini, data_fim,
                                str(wfm_proc_colab), 
                                str(i+1),
                                'product',
                                'ESC'
                            ])
                            os.kill(os.getpid(), signal.SIGTERM)

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
                        
                        # WAIT 5 SECONDS BEFORE CALLING ANOTHER CHILD
                        time.sleep(5)
                        
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
                        
                        print(f'0. Error calling child proc {i+1}: {str(e)}')
            
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
        
connection = connection_object.disconnect_database()
sys.exit()