# -*- coding: utf-8 -*-
"""
MULTITHREADING EXAMPLE - Complete implementation for parallel execution

This file shows a complete multithreaded version of the orchestrator.
It can be used as a reference or replacement for orquestrador.py
"""

import os
import sys
import time
import datetime
import pandas as pd
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Import batch process components
from base_data_project.log_config import setup_logger, get_logger
from base_data_project.utils import create_components
from batch_process import run_batch_process
from src.configuration_manager.instance import get_config

# Import orchestrator functions
from src.orquestrador_functions.WFM_Process.Getters import get_process_by_status, get_process_by_id, get_total_process_by_status
from src.orquestrador_functions.WFM_Process.Setters import set_process_status, set_process_param_status
from src.helpers import set_process_errors
from src.orquestrador_functions.Data_Handlers.GetGlobalData import get_all_params, get_gran_equi
from src.orquestrador_functions.Logs.message_loader import get_messages, set_messages
from src.orquestrador_functions.Classes.AlgorithmPrepClasses.ConnectionHandler import ConnectionHandler

# ============================================================================
# KEY CONCEPT 1: Thread-Safe Counter
# ============================================================================
# When multiple threads update a shared variable (like local_processes),
# we need a lock to prevent race conditions.

class ThreadSafeCounter:
    """Thread-safe counter for tracking running processes."""
    def __init__(self):
        self._value = 0
        self._lock = Lock()
    
    def increment(self):
        """Safely increment the counter."""
        with self._lock:
            self._value += 1
            return self._value
    
    def decrement(self):
        """Safely decrement the counter."""
        with self._lock:
            self._value -= 1
            return self._value
    
    def get(self):
        """Get current value."""
        with self._lock:
            return self._value


# ============================================================================
# KEY CONCEPT 2: Connection Helper (Open/Close on Demand)
# ============================================================================
# OPTIMIZATION: Instead of keeping connections open for entire thread lifetime,
# we open/close connections only when needed. This reduces concurrent connections.
#
# Connection usage pattern:
# 1. Setup phase: Open -> set status/log -> Close (quick, ~1-2 queries)
# 2. Batch process: Open -> keep open during execution -> Close (long-running)
# 3. Final status: Open -> update status/log -> Close (quick, ~1-2 queries)
#
# This means connections are only open during actual database operations,
# not during the entire thread lifetime (which could be minutes).

def get_thread_connection():
    """Create a new database connection for a thread. Caller must close it."""
    connection_object = ConnectionHandler()
    connection_object.connect_to_database()
    connection = connection_object.get_connection()
    connection = connection_object.ensure_connection()
    return connection

def with_connection(func):
    """Context manager helper: opens connection, executes function, closes connection.
    
    Usage:
        result = with_connection(lambda conn: do_something(conn))
    """
    connection = None
    try:
        connection = get_thread_connection()
        return func(connection)
    finally:
        if connection:
            try:
                connection.close()
            except:
                pass

# ============================================================================
# KEY CONCEPT 3: Worker Function (Runs in Each Thread)
# ============================================================================
# Each thread executes this function independently. It needs:
# - Opens/closes connections only when needed (not kept open)
# - All the process parameters
# - Error handling that doesn't crash other threads

def run_single_process_worker(
    process_data,           # Dict with wfm_proc_id, wfm_user, data_ini, etc.
    api_proc_id,           # Parent process ID
    api_user,              # Parent user
    path_ficheiros_global, # File paths
    df_msg,                # Messages dataframe
    PROC_COD,              # Process code
    config_manager,        # Configuration
    local_counter,         # Thread-safe counter
    thread_index           # Index for logging (i+1)
):
    """
    Worker function that runs in a separate thread.
    
    Each thread:
    1. Creates its own database connection
    2. Sets process status to 'P' (Processing)
    3. Runs the batch process
    4. Updates status to 'G' (Success) or 'I' (Error)
    5. Decrements the local counter when done
    """
    wfm_proc_id = process_data['wfm_proc_id']
    wfm_user = process_data['wfm_user']
    data_ini = process_data['data_ini']
    data_fim = process_data['data_fim']
    wfm_proc_colab = process_data['wfm_proc_colab']
    
    # Get logger for this thread
    logger = get_logger(config_manager.system.project_name)
    
    logger.info(f"[Thread {thread_index}] Starting process {wfm_proc_id}")
    
    try:
        # OPTIMIZATION: Open connection only for setup queries, then close
        def setup_process(conn):
            # Set status to Processing
            res = set_process_status(
                conn, 
                path_ficheiros_global, 
                wfm_user, 
                wfm_proc_id, 
                status='P'
            )
            
            if res == 1:
                # Log start
                set_process_errors(
                    conn,
                    pathOS=path_ficheiros_global,
                    user=api_user,
                    fk_process=wfm_proc_id,
                    type_error='I',
                    process_type=PROC_COD,
                    error_code=None,
                    description=set_messages(df_msg, 'callSubproc', {'1': wfm_proc_id, '2': thread_index}),
                    employee_id=None,
                    schedule_day=None
                )
                conn.commit()
                return True
            return False
        
        # Execute setup with connection (opens, executes, closes)
        setup_success = with_connection(setup_process)
        
        if not setup_success:
            logger.error(f"[Thread {thread_index}] Failed to set status for process {wfm_proc_id}")
            # Try to log error (open connection, log, close)
            def log_error(conn):
                set_process_errors(
                    conn,
                    pathOS=path_ficheiros_global,
                    user=api_user,
                    fk_process=wfm_proc_id,
                    type_error='E',
                    process_type=PROC_COD,
                    error_code=None,
                    description=set_messages(df_msg, 'errCallSubProc', {'1': wfm_proc_id, '2': thread_index, '3': ''}),
                    employee_id=None,
                    schedule_day=None
                )
            with_connection(log_error)
            return
        
        # Create components for this thread's batch process
        # Note: data_manager creates its own SQLAlchemy connection internally
        data_manager, process_manager = create_components(
            use_db=True,
            no_tracking=False,
            config=config_manager,
            project_name=config_manager.system.project_name
        )
        
        # Build external call dict
        external_call_dict = {
            'current_process_id': wfm_proc_id,
            'api_proc_id': api_proc_id,
            'wfm_proc_id': wfm_proc_id,
            'wfm_user': wfm_user,
            'start_date': str(data_ini),
            'end_date': str(data_fim),
            'wfm_proc_colab': str(wfm_proc_colab) if wfm_proc_colab is not None else ''
        }
        
        # OPTIMIZATION: Open connection for batch process, keep open during execution, close after
        # The connection is needed throughout batch process for error logging
        # So we open it, use it, then close immediately after batch completes
        batch_connection = None
        try:
            batch_connection = get_thread_connection()
            # Run batch process (this is the time-consuming part)
            # Multiple threads can run this simultaneously
            with data_manager:
                success = run_batch_process(
                    data_manager=data_manager,
                    process_manager=process_manager,
                    algorithm="example_algorithm",
                    external_call_dict=external_call_dict,
                    external_raw_connection=batch_connection  # Keep open during batch process
                )
        finally:
            # Close batch connection immediately after batch process completes
            if batch_connection:
                try:
                    batch_connection.close()
                except:
                    pass
        
        # OPTIMIZATION: Open connection only for final status updates, then close
        def update_final_status(conn):
            if success:
                logger.info(f"[Thread {thread_index}] Process {wfm_proc_id} completed successfully")
                set_process_param_status(
                    conn,
                    pathOS=path_ficheiros_global,
                    user=api_user,
                    process_id=wfm_proc_id,
                    new_status='G'
                )
                set_process_errors(
                    conn,
                    pathOS=path_ficheiros_global,
                    user=api_user,
                    fk_process=wfm_proc_id,
                    type_error='I',
                    process_type=PROC_COD,
                    error_code=None,
                    description=set_messages(df_msg, 'endSubproc', {'1': wfm_proc_id, '2': ''}),
                    employee_id=None,
                    schedule_day=None
                )
            else:
                logger.info(f"[Thread {thread_index}] Process {wfm_proc_id} failed")
                set_process_param_status(
                    conn,
                    pathOS=path_ficheiros_global,
                    user=api_user,
                    process_id=wfm_proc_id,
                    new_status='I'
                )
        
        # Execute final status update with connection (opens, executes, closes)
        with_connection(update_final_status)
            
    except Exception as e:
        logger.error(f"[Thread {thread_index}] Error in process {wfm_proc_id}: {e}", exc_info=True)
        # Try to log error (open connection, log, close)
        try:
            def log_exception(conn):
                set_process_errors(
                    conn,
                    pathOS=path_ficheiros_global,
                    user=api_user,
                    fk_process=wfm_proc_id,
                    type_error='E',
                    process_type=PROC_COD,
                    error_code=None,
                    description=set_messages(df_msg, 'errCallSubProc', {'1': wfm_proc_id, '2': thread_index, '3': str(e)}),
                    employee_id=None,
                    schedule_day=None
                )
            with_connection(log_exception)
        except:
            pass  # Don't let error logging crash the thread
    
    finally:
        # Decrement counter when thread finishes
        local_counter.decrement()
        logger.info(f"[Thread {thread_index}] Process {wfm_proc_id} thread finished. Active threads: {local_counter.get()}")


# ============================================================================
# MAIN EXECUTION - Complete multithreaded orchestrator
# ============================================================================

# Get shared configuration manager instance
config_manager = get_config()

# Initialize logger
setup_logger(
    project_name=config_manager.system.project_name,
    log_level=config_manager.system.get_log_level(),
    log_dir=config_manager.system.logging_config.get('log_dir', 'logs'),
    console_output=True
)
logger = get_logger(config_manager.system.project_name)

# SET INITIAL PATH
import platform
sis = platform.system()
path_ficheiros_global = os.getcwd() + "/"
os.chdir(path_ficheiros_global)
logger.info("-------------------------------")
logger.info(path_ficheiros_global)
logger.info("-------------------------------")

# Suppress all warnings
warnings.filterwarnings("ignore")

# Setup database connection (uses config_manager credentials - no path required)
connection_object = ConnectionHandler()
connection_object.connect_to_database()
connection = connection_object.get_connection()
connection = connection_object.ensure_connection()
logger.info(f"connection: {connection}")

# GET LOG MESSAGES
df_msg = get_messages(path_ficheiros_global, lang='ES')
data_hoje = datetime.date.today()

# GET ARG BY API
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
all_params = get_all_params(path_ficheiros_global, connection=connection)
logger.info(f"all_params: {all_params}")

GLOBAL_MAX_CONCURRENT_PROCESSES = next((item['NUMBERVALUE'] for item in all_params if item['SYS_P_NAME'] == 'GLOBAL_MAX_CONCURRENT_PROCESSES'), None)
if GLOBAL_MAX_CONCURRENT_PROCESSES is not None: 
    GLOBAL_MAX_CONCURRENT_PROCESSES = max(GLOBAL_MAX_CONCURRENT_PROCESSES, 3)
else:
    GLOBAL_MAX_CONCURRENT_PROCESSES = 3
    
LOCAL_MAX_CONCURRENT_PROCESSES = next((item['NUMBERVALUE'] for item in all_params if item['SYS_P_NAME'] == 'LOCAL_MAX_CONCURRENT_PROCESSES'), None)
if LOCAL_MAX_CONCURRENT_PROCESSES is not None: 
    LOCAL_MAX_CONCURRENT_PROCESSES = max(LOCAL_MAX_CONCURRENT_PROCESSES, 1)
else:
    LOCAL_MAX_CONCURRENT_PROCESSES = 1
    
MAX_RETRIES = next((item['NUMBERVALUE'] for item in all_params if item['SYS_P_NAME'] == 'MAX_RETRIES'), None)
if MAX_RETRIES is not None: 
    MAX_RETRIES = max(MAX_RETRIES, 1)
else:
    MAX_RETRIES = 1
    
RETRY_WAIT_TIME = next((item['NUMBERVALUE'] for item in all_params if item['SYS_P_NAME'] == 'RETRY_WAIT_TIME'), None)
if RETRY_WAIT_TIME is not None: 
    RETRY_WAIT_TIME = max(RETRY_WAIT_TIME, 0)
else:
    RETRY_WAIT_TIME = 0

# GET PROCESSES
sec_to_proc = get_process_by_status(path_ficheiros_global, 'WFM', 'MPD', '2', 'N', connection)
logger.info(f"Nr of process {len(sec_to_proc)}")

# Thread-safe counter instead of simple variable
local_counter = ThreadSafeCounter()
retries = 0

if not sec_to_proc.empty:
    while retries < MAX_RETRIES:
        num_new_processes = len(sec_to_proc)
        num_processing_processes = get_total_process_by_status(path_ficheiros_global, connection)['TOTAL_P'].iloc[0]
        
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
                description=set_messages(df_msg, 'iniProc', {'1': '', '2': retries}),
                employee_id=None, 
                schedule_day=None
            )
            
            retries += 1
            time.sleep(RETRY_WAIT_TIME)
        else:
            # CALLS THE PROC CHILDS
            available_global_slots = GLOBAL_MAX_CONCURRENT_PROCESSES - num_processing_processes
            available_local_slots = LOCAL_MAX_CONCURRENT_PROCESSES - local_counter.get()
            processes_to_start = min(available_global_slots, available_local_slots, num_new_processes)
            logger.info(f"processes_to_start: {processes_to_start}")
            
            if processes_to_start > 0:
                # Prepare process data for each thread
                process_list = []
                for i in range(processes_to_start):
                    try:
                        # Retrieve variables from sec_to_proc
                        wfm_proc_id = int(sec_to_proc.iloc[i]['CODIGO'])
                        wfm_user = str(sec_to_proc.iloc[i]['USER_CRIACAO'])
                        data_ini = sec_to_proc.iloc[i]['DATA_INI'].strftime('%Y-%m-%d') 
                        data_fim = sec_to_proc.iloc[i]['DATA_FIM'].strftime('%Y-%m-%d')
                        
                        # Handle FK_COLABORADOR - if NaN from database (NULL), set to None
                        wfm_proc_colab_raw = sec_to_proc.iloc[i]['FK_COLABORADOR']
                        if pd.isna(wfm_proc_colab_raw):
                            wfm_proc_colab = None
                        else:
                            wfm_proc_colab = int(wfm_proc_colab_raw)
                        
                        process_data = {
                            'wfm_proc_id': wfm_proc_id,
                            'wfm_user': wfm_user,
                            'data_ini': data_ini,
                            'data_fim': data_fim,
                            'wfm_proc_colab': wfm_proc_colab
                        }
                        process_list.append((process_data, i + 1))  # Store with index for logging
                    except Exception as e:
                        logger.error(f"Error preparing process data for index {i}: {e}", exc_info=True)
                
                # KEY: Use ThreadPoolExecutor to run processes in parallel
                # max_workers limits concurrent threads (respects LOCAL_MAX_CONCURRENT_PROCESSES)
                logger.info(f"Starting {len(process_list)} processes in parallel (max {LOCAL_MAX_CONCURRENT_PROCESSES} concurrent)")
                
                with ThreadPoolExecutor(max_workers=LOCAL_MAX_CONCURRENT_PROCESSES) as executor:
                    # Submit all processes to thread pool
                    # They will start executing immediately (up to max_workers limit)
                    futures = []
                    for process_data, thread_index in process_list:
                        # Increment counter before starting thread
                        local_counter.increment()
                        
                        # Submit to thread pool (non-blocking)
                        future = executor.submit(
                            run_single_process_worker,
                            process_data,
                            api_proc_id,
                            api_user,
                            path_ficheiros_global,
                            df_msg,
                            PROC_COD,
                            config_manager,
                            local_counter,
                            thread_index
                        )
                        futures.append(future)
                    
                    # Wait for all threads to complete
                    # as_completed() yields futures as they finish (not necessarily in order)
                    for future in as_completed(futures):
                        try:
                            # This will raise any exception that occurred in the thread
                            future.result()  # Blocks until this specific thread completes
                        except Exception as e:
                            logger.error(f"Thread raised exception: {e}", exc_info=True)
                
                # All threads have completed
                logger.info(f"All {len(process_list)} processes finished")
                break
            
    if retries >= MAX_RETRIES:
        set_process_errors(
            connection,
            pathOS=path_ficheiros_global, 
            user=api_user,
            fk_process=api_proc_id, 
            type_error='I', 
            process_type=PROC_COD, 
            error_code=None, 
            description=set_messages(df_msg, 'errMaxThreads', {'1': '', '2': f"retries >= MAX_RETRIES: {retries >= MAX_RETRIES}"}),
            employee_id=None, 
            schedule_day=None
        )

# Database connection remains active for parent process
logger.info("Parent process completed successfully - all threads finished")
sys.exit(0)


# ============================================================================
# HOW IT WORKS: Visual Timeline
# ============================================================================
"""
SEQUENTIAL (Current):
Time →
Thread 1: [========Process 1========] [========Process 2========] [========Process 3========]
Total time: Process1 + Process2 + Process3

PARALLEL (Multithreaded):
Time →
Thread 1: [========Process 1========]
Thread 2: [========Process 2========]
Thread 3: [========Process 3========]
Total time: max(Process1, Process2, Process3)  ← Much faster!

If Process1 takes 10 min, Process2 takes 5 min, Process3 takes 8 min:
- Sequential: 10 + 5 + 8 = 23 minutes
- Parallel: max(10, 5, 8) = 10 minutes (saves 13 minutes!)
"""


# ============================================================================
# CRITICAL CONSIDERATIONS
# ============================================================================
"""
1. DATABASE CONNECTIONS:
   - Oracle connections (cx_Oracle) are NOT thread-safe
   - Each thread MUST have its own connection
   - Don't share the main 'connection' object across threads
   - Close connections in finally blocks

2. THREAD SAFETY:
   - Use locks for shared variables (local_processes counter)
   - Database operations are generally safe (Oracle handles concurrent queries)
   - But connection objects themselves cannot be shared

3. ERROR HANDLING:
   - Errors in one thread should not crash others
   - Each thread should handle its own exceptions
   - Use try/except/finally in worker function

4. RESOURCE LIMITS:
   - LOCAL_MAX_CONCURRENT_PROCESSES limits threads in this orchestrator
   - GLOBAL_MAX_CONCURRENT_PROCESSES limits total across all orchestrators
   - ThreadPoolExecutor max_workers enforces LOCAL limit

5. WAITING FOR COMPLETION:
   - ThreadPoolExecutor context manager waits for all threads
   - as_completed() lets you process results as they finish
   - Don't exit main loop until all threads complete
"""

