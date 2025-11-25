"""
Calendar matrix helper functions for the DescansosDataModel.
Converted from R functions for processing employee schedules, holidays, and absences.
"""

import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional, Tuple

# Local stuff
from src.configuration_manager.instance import get_config as get_config_manager
from src.orquestrador_functions.Classes.Connection.connect import ensure_connection
from base_data_project.log_config import get_logger
from base_data_project.data_manager.managers.managers import BaseDataManager, DBDataManager
from src.orquestrador_functions.Classes.Connection.connect import ensure_connection

# Set up logger
logger = get_logger(get_config_manager().system.project_name)

def log_process_event(message_key:str, messages_df: pd.DataFrame, data_manager: BaseDataManager, external_call_data: dict, values_replace_dict: dict, level: str = 'INFO'):
    """
    Log a process event with a message key and a message dataframe.
    """
    message = pd.DataFrame(messages_df[messages_df['VAR'] == message_key])
    if message.empty:
        logger.error(f"Message key {message_key} not found in messages_df")
        return
    message_str = message['ES'].values[0]
    message_str = replace_placeholders(message_str, values_replace_dict)
    logger.info(f"DEBUG: message_str: {message_str}")
    data_manager.set_process_errors(message_key=message_key, rendered_message=message_str, values_replace_dict=external_call_data, error_type=level)

def set_process_errors(connection, pathOS, user, fk_process, type_error, process_type, error_code, description, employee_id, schedule_day):
    """
    Inserts process error details into the database.

    Args:
        connection: Active database connection.
        pathOS (str): Base path for configurations and query files.
        user (str): Username for the operation.
        fk_process (int): Foreign key for the process.
        type_error (str): Type of error.
        process_type (str): Type of process.
        error_code (int): Error code.
        description (str): Description of the error.
        employee_id (int): ID of the employee.
        schedule_day (str): Scheduled day for the error (formatted as 'yyyy-mm-dd').
    Returns:
        int: 1 if successful, 0 otherwise.
    """
    try:
        logger.info(f"DEBUG set_process_errors CALLED - connection: {connection}, user: {user}, fk_process: {fk_process}, description: {description}")
        
        # Only call ensure_connection for direct cx_Oracle connections
        # SQLAlchemy connections manage their own lifecycle
        if hasattr(connection, 'ping') and callable(getattr(connection, 'ping')):
            connection_path = os.path.join(pathOS, "src", "orquestrador_functions", "Classes", "Connection")
            connection = ensure_connection(connection, connection_path)
            logger.info(f"DEBUG: ensured cx_Oracle connection")
        else:
            logger.info(f"DEBUG: using SQLAlchemy connection as-is")
        
        query_file_path = os.path.join(pathOS, 'Data', 'Queries', 'WFM_Process', 'Setters', 'set_process_errors.sql')
        logger.info(f"DEBUG: query_file_path: {query_file_path}")
        
        # Load the query from file
        with open(query_file_path, 'r') as f:
            query = f.read().strip().replace("\n", " ")
        
        logger.info(f"DEBUG: loaded query: {query[:100]}...")
        
        # Execute the query - handle both cx_Oracle and SQLAlchemy connections
        if connection is None:
            logger.error("ERROR: No database connection available")
            return 0
            
        if hasattr(connection, 'cursor') and callable(getattr(connection, 'cursor')):
            # Direct cx_Oracle connection
            cursor_context = connection.cursor()
            logger.info(f"DEBUG: using direct cx_Oracle cursor")
        elif hasattr(connection, 'connection') and hasattr(connection.connection, 'cursor'):
            # SQLAlchemy wrapped connection - get the raw connection
            cursor_context = connection.connection.cursor()
            logger.info(f"DEBUG: using SQLAlchemy wrapped cursor")
        else:
            logger.error(f"ERROR: Unknown connection type: {type(connection)}")
            return 0
        
        with cursor_context as cursor:
            params = {
               'i_user': user,
               'i_fk_process': fk_process,
               'i_type_error': type_error,
               'i_process_type': process_type,
               'i_error_code': error_code if error_code is not None else None,
               'i_description': description,
               'i_employee_id': employee_id if employee_id is not None else None,
               'i_schedule_day': schedule_day if schedule_day is not None else None
           }
            logger.info(f"DEBUG: executing with params: {params}")
            cursor.execute(query, params)
            logger.info(f"DEBUG: query executed successfully")
            
            # Handle commit for both connection types
            if hasattr(connection, 'commit') and callable(getattr(connection, 'commit')):
                # Direct cx_Oracle connection
                connection.commit()
                logger.info(f"DEBUG: committed cx_Oracle connection")
            elif hasattr(connection, 'connection') and hasattr(connection.connection, 'commit'):
                # SQLAlchemy wrapped connection
                connection.connection.commit()
                logger.info(f"DEBUG: committed SQLAlchemy connection")
        
        logger.info(f"DEBUG: set_process_errors returning 1 (success)")
        return 1
        
    except Exception as e:
        logger.error(f"Error in set_process_errors: {e}", exc_info=True)
        return 0

def replace_placeholders(template, values_dict):
    """
    Replaces placeholders in the template string with corresponding values from the values dictionary.
    
    Parameters:
        template (str): The template string with placeholders.
        values (dict): A dictionary with keys corresponding to placeholder names and values as replacements.
    
    Returns:
        str: The template string with placeholders replaced by values.
    """
    for name, value in values_dict.items():
        placeholder = f"{{{name}}}"
        template = template.replace(placeholder, str(value))
    return template

def get_oracle_url_cx():
    """Create Oracle connection URL for cx_Oracle driver"""
    config_manager = get_config_manager()
    if config_manager.oracle_config is None:
        raise ValueError("Oracle configuration not available - use_db is False")
    
    return config_manager.oracle_config.get_connection_url()

def insert_feriados(df_feriados: pd.DataFrame, reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R insert_feriados function to Python.
    Insert holidays into the schedule matrix.
    
    Args:
        df_feriados: DataFrame with holiday data (columns: data, tipo)
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with holidays inserted
    """
    try:
        # Create new row for TIPO_DIA
        new_row = ['-'] * reshaped_final_3.shape[1]
        new_row[0] = "TIPO_DIA"
        
        # Split matrix into upper and lower parts
        upper_bind = reshaped_final_3.iloc[[0]].copy()
        lower_bind = reshaped_final_3.iloc[1:].copy()
        
        # Create new row DataFrame
        new_row_df = pd.DataFrame([new_row], columns=reshaped_final_3.columns)
        
        # Combine parts
        reshaped_final_3 = pd.concat([upper_bind, new_row_df, lower_bind], ignore_index=True)
        
        # Process each holiday
        for _, holiday_row in df_feriados.iterrows():
            temp = str(holiday_row['data'])
            data = temp[:10]  # Extract date (YYYY-MM-DD format)
            val = holiday_row['tipo']
            
            # Find column indices for this date
            col_indices = []
            for col_idx, col_data in reshaped_final_3.items():
                if data in col_data.values:
                    col_indices.append(col_idx)
            
            if len(col_indices) >= 2:
                if val == 2:
                    # Open holiday - mark both shifts as F
                    reshaped_final_3.iloc[1, col_indices[0]:col_indices[1]+1] = "F"
                elif val == 3:
                    # Closed holiday - mark day type and all employees as F
                    reshaped_final_3.iloc[1, col_indices[0]:col_indices[1]+1] = "F"
                    reshaped_final_3.iloc[3:, col_indices[0]] = "F"
                    reshaped_final_3.iloc[3:, col_indices[1]] = "F"
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in insert_feriados: {str(e)}")
        return reshaped_final_3

def insert_closed_days(closed_days: pd.DataFrame, reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R insert_closedDays function to Python.
    Insert closed days into the schedule matrix.
    
    Args:
        closed_days: DataFrame with closed day data
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with closed days inserted
    """
    try:
        # Process each closed day
        for _, closed_day_row in closed_days.iterrows():
            temp = str(closed_day_row.iloc[0])
            data = temp[:10]  # Extract date (YYYY-MM-DD format)
            
            # Find column indices for this date
            col_indices = []
            for col_idx, col_data in reshaped_final_3.items():
                if data in col_data.values:
                    col_indices.append(col_idx)
            
            if len(col_indices) >= 2:
                # Mark all employees as L (closed) for both shifts
                reshaped_final_3.iloc[3:, col_indices[0]] = "L"
                reshaped_final_3.iloc[3:, col_indices[1]] = "L"
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in insert_closed_days: {str(e)}")
        return reshaped_final_3

def insert_holidays_absences(employees_tot: List[str], ausencias_total: pd.DataFrame, 
                           reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R insert_holidays_abscences function to Python.
    Insert holidays (V) and absences (A) into the schedule matrix.
    
    Args:
        employees_tot: List of all employee IDs
        ausencias_total: DataFrame with absence data
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with absences inserted
    """
    try:
        for colab in employees_tot:
            colab_pad = colab
            
            # Filter absences for this employee
            ausencias = ausencias_total[ausencias_total['matricula'] == colab_pad]
            
            if len(ausencias) == 0:
                continue
            
            # Find employee row index
            row_indices = []
            for row_idx, row_data in reshaped_final_3.iterrows():
                if colab in row_data.values:
                    row_indices.append(row_idx)
            
            if not row_indices:
                continue
                
            row_index = row_indices[0]
            
            # Process each absence
            for _, absence_row in ausencias.iterrows():
                temp = str(absence_row['data_ini'])
                data = temp[:10]  # Extract date
                val = absence_row['tipo_ausencia']
                fk_motivo_ausencia = int(absence_row['fk_motivo_ausencia']) 
                
                # Find column indices for this date
                col_indices = []
                for col_idx, col_data in reshaped_final_3.items():
                    if data in col_data.values:
                        col_indices.append(col_idx)
                
                #logger.info(f"DEBUG: col_indices: {col_indices}")
                if len(col_indices) >= 2:
                    #logger.info(f"DEBUG: fk_motivo_ausencia: {fk_motivo_ausencia}, type: {type(fk_motivo_ausencia)}")
                    # Get current values
                    current_morning = reshaped_final_3.iloc[row_index, col_indices[0]]
                    current_afternoon = reshaped_final_3.iloc[row_index, col_indices[1]]
                    
                    # TODO: check for codigo_motivo_ausencia in config.py
                    if fk_motivo_ausencia in codigos_motivo_ausencia:
                        # Vacation - check if current value is "-"
                        if current_morning == "-":
                            reshaped_final_3.iloc[row_index, col_indices[0]] = "V-"
                        else:
                            reshaped_final_3.iloc[row_index, col_indices[0]] = "V"
                        
                        if current_afternoon == "-":
                            reshaped_final_3.iloc[row_index, col_indices[1]] = "V-"
                        else:
                            reshaped_final_3.iloc[row_index, col_indices[1]] = "V"
                    else:
                        # Other absence - check if current value is "-" and val is "A"
                        if current_morning == "-" and val == "A":
                            reshaped_final_3.iloc[row_index, col_indices[0]] = "A-"
                        else:
                            reshaped_final_3.iloc[row_index, col_indices[0]] = val
                        
                        if current_afternoon == "-" and val == "A":
                            reshaped_final_3.iloc[row_index, col_indices[1]] = "A-"
                        else:
                            reshaped_final_3.iloc[row_index, col_indices[1]] = val
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in insert_holidays_absences: {str(e)}")
        return reshaped_final_3

def insert_dayoffs_override(df_core_pro_emp_horario_det: pd.DataFrame, 
                           reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Override schedule matrix with day-offs from df_core_pro_emp_horario_det.
    Applies 'F' for EVERY day-off found in df_core_pro_emp_horario_det, regardless of 
    what's currently in the matrix for that employee and day.
    
    Args:
        df_core_pro_emp_horario_det: DataFrame with day-off data (columns: employee_id, schedule_day, tipo_dia)
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with day-offs applied for all matching employee/day combinations
    """
    try:
        logger.info(f"Starting insert_dayoffs_override with df_core_pro_emp_horario_det shape: {df_core_pro_emp_horario_det.shape}")
        
        if df_core_pro_emp_horario_det.empty:
            logger.info("df_core_pro_emp_horario_det is empty, returning unchanged matrix")
            return reshaped_final_3
            
        # Filter for day-offs only (tipo_dia = 'F')
        df_core_dayoffs = df_core_pro_emp_horario_det[
            df_core_pro_emp_horario_det['tipo_dia'] == 'F'
        ].copy()

        df_core_no_work = df_core_pro_emp_horario_det[
            df_core_pro_emp_horario_det['tipo_dia'] == 'S'
        ].copy()
        
        #logger.info(f"Filtered day-offs (F): {len(df_core_dayoffs)} records")
        #logger.info(f"Filtered no-work days (S): {len(df_core_no_work)} records")
        
        if df_core_dayoffs.empty and df_core_no_work.empty:
            logger.info("No special day records found, returning unchanged matrix")
            return reshaped_final_3
            
        # Convert schedule_day to string format (YYYY-MM-DD)
        if not df_core_dayoffs.empty:
            df_core_dayoffs['schedule_day_str'] = pd.to_datetime(df_core_dayoffs['schedule_day']).dt.strftime('%Y-%m-%d')
            logger.info(f"Processing {len(df_core_dayoffs)} day-off records")
        
        if not df_core_no_work.empty:
            df_core_no_work['schedule_day_str'] = pd.to_datetime(df_core_no_work['schedule_day']).dt.strftime('%Y-%m-%d')
        
        # Process each day-off record
        for _, dayoff_row in df_core_dayoffs.iterrows():
            employee_id = str(dayoff_row['employee_id'])
            matricula = str(dayoff_row['matricula']) if 'matricula' in dayoff_row else None
            schedule_day = dayoff_row['schedule_day_str']
            
            # Find employee row index in reshaped_final_3 - use matricula for matching
            # since reshaped_final_3 uses matricula values, not fk_colaborador
            employee_row_idx = None
            search_value = matricula if matricula else employee_id
            
            for row_idx, row_data in reshaped_final_3.iterrows():
                # Check if matricula exists as exact value in this row
                if search_value in row_data.values:
                    employee_row_idx = row_idx
                    break
            
            if employee_row_idx is None:
                logger.warning(f"Employee {employee_id} (matricula: {matricula}) not found in schedule matrix")
                continue
                
            # Find column indices for this date - use exact value matching
            col_indices = []
            for col_idx, col_data in reshaped_final_3.items():
                # Check if schedule_day exists as exact value in this column
                if schedule_day in col_data.values:
                    col_indices.append(col_idx)
            
            if len(col_indices) >= 2:
                # Current behavior: Override EVERY F found in df_core_pro_emp_horario_det
                # regardless of what's currently in the matrix
                reshaped_final_3.iloc[employee_row_idx, col_indices[0]] = "L"
                reshaped_final_3.iloc[employee_row_idx, col_indices[1]] = "L"
                
                # Previous logic (commented for potential future use):
                # Only override if there's already an absence (V, A, or other non-shift values)
                # Exclude normal shift values like M, T, MT, etc. and empty values like '-'
                #absence_indicators = ['V', 'A', 'F']  # Common absence types
                #
                #if (current_morning in absence_indicators or current_afternoon in absence_indicators):
                #    logger.info(f"Overriding absence with day-off (F) for employee {employee_id} (matricula: {matricula}) on {schedule_day}")
                #    # Override with day-off (F) for both morning and afternoon shifts
                #    reshaped_final_3.iloc[employee_row_idx, col_indices[0]] = "F"
                #    reshaped_final_3.iloc[employee_row_idx, col_indices[1]] = "F"
                #else:
                #    logger.info(f"No absence found to override for employee {employee_id} (matricula: {matricula}) on {schedule_day} (current: {current_morning}, {current_afternoon})")
            
        for _, no_work_row in df_core_no_work.iterrows():
            # Step 1: Extract no-work day information
            employee_id = str(no_work_row['employee_id'])
            matricula = str(no_work_row['matricula']) if 'matricula' in no_work_row else None
            schedule_day = no_work_row['schedule_day_str']
            
            # Step 2: Find employee row index in reshaped_final_3
            employee_row_idx = None
            search_value = matricula if matricula else employee_id
            
            for row_idx, row_data in reshaped_final_3.iterrows():
                if search_value in row_data.values:
                    employee_row_idx = row_idx
                    break
            
            if employee_row_idx is None:
                logger.warning(f"Employee {employee_id} (matricula: {matricula}) not found in schedule matrix for no-work day")
                continue
            
            # Step 3: Find column indices for this date
            col_indices = []
            for col_idx, col_data in reshaped_final_3.items():
                if schedule_day in col_data.values:
                    col_indices.append(col_idx)
            
            if len(col_indices) >= 2:
                # Step 4: Override A -> AV and V -> VV
                current_morning = reshaped_final_3.iloc[employee_row_idx, col_indices[0]]
                current_afternoon = reshaped_final_3.iloc[employee_row_idx, col_indices[1]]
                
                if current_morning == "A":
                    reshaped_final_3.iloc[employee_row_idx, col_indices[0]] = "A-"
                if current_afternoon == "A":
                    reshaped_final_3.iloc[employee_row_idx, col_indices[1]] = "A-"
                
                if current_morning == "V":
                    reshaped_final_3.iloc[employee_row_idx, col_indices[0]] = "V-"
                if current_afternoon == "V":
                    reshaped_final_3.iloc[employee_row_idx, col_indices[1]] = "V-"

        logger.info("Completed insert_dayoffs_override processing")
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in insert_dayoffs_override: {str(e)}", exc_info=True)
        return reshaped_final_3

def create_m0_0t(reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R create_M0_0T function to Python.
    Assign 0 after M shift and before T shift to indicate free periods.
    
    Args:
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with 0s for free periods
    """
    try:
        # Iterate through columns in pairs (each date has two columns: M and T)
        for i in range(1, reshaped_final_3.shape[1] - 1, 2):  # Start from 1, step by 2
            # Process each employee row (starting from row 2, index 2)
            for j in range(2, len(reshaped_final_3)):
                current_val = str(reshaped_final_3.iloc[j, i])
                
                if current_val == "M":
                    # Morning shift - set afternoon to 0
                    reshaped_final_3.iloc[j, i + 1] = 0
                elif current_val in ["T", "T1", "T2"]:
                    # Afternoon/Evening shift - set morning to 0
                    reshaped_final_3.iloc[j, i] = 0
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in create_m0_0t: {str(e)}")
        return reshaped_final_3

def create_mt_mtt_cycles(df_alg_variables_filtered: pd.DataFrame, reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R create_MT_MTT_cycles function to Python.
    Create MT or MTT cycles according to shift patterns.
    
    Args:
        df_alg_variables_filtered: DataFrame with employee algorithm variables
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with MT/MTT cycles
    """
    try:
        logger.info(f"=== CALENDAR CREATION DEBUG START ===")
        logger.info(f"Initial reshaped_final_3 shape: {reshaped_final_3.shape}")
        logger.info(f"df_alg_variables_filtered shape: {df_alg_variables_filtered.shape}")
        logger.info(f"df_alg_variables_filtered columns: {df_alg_variables_filtered.columns.tolist()}")
        logger.info(f"df_alg_variables_filtered:\n{df_alg_variables_filtered}")
        
        # Reset column names and row names
        reshaped_final_3.columns = range(reshaped_final_3.shape[1])
        reshaped_final_3.reset_index(drop=True, inplace=True)
        
        # Select required columns
        df_alg_variables_filtered = df_alg_variables_filtered[['emp', 'seq_turno', 'semana_1']].copy()
        
        for idx, emp_row in df_alg_variables_filtered.iterrows():
            emp = emp_row['emp']
            seq_turno = emp_row['seq_turno']
            
            logger.info(f"--- Processing employee {emp} (row {idx}) ---")
            logger.info(f"Raw seq_turno value: {seq_turno} (type: {type(seq_turno)})")
            logger.info(f"pd.isna(seq_turno): {pd.isna(seq_turno)}")
            logger.info(f"seq_turno is None: {seq_turno is None}")
            
            # Handle missing seq_turno
            if pd.isna(seq_turno) or seq_turno is None:
                logger.warning(f"No seq_turno defined for employee: {emp}, setting to 'T'")
                seq_turno = "T"
            
            semana1 = emp_row['semana_1']
            logger.info(f"Employee {emp}: seq_turno='{seq_turno}', semana_1='{semana1}'")
            
            # Calculate days in week (simplified - you may need to adjust)
            if len(reshaped_final_3.columns) > 1:
                first_date_str = str(reshaped_final_3.iloc[0, 1])
                eachrep = count_days_in_week(first_date_str) * 2
            else:
                eachrep = 14
            
            logger.info(f"Employee {emp}: eachrep={eachrep}, matrix width={reshaped_final_3.shape[1]}")

            # Generate shift patterns based on seq_turno and semana1
            if seq_turno == "MT" and semana1 in ["T", "T1"]:
                logger.info(f"Employee {emp}: Using MT pattern with T/T1 start")
                new_row = ['T'] * eachrep
                new_row2 = (['M'] * 14 + ['T'] * 14) * ((reshaped_final_3.shape[1] // 2 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MT" and semana1 in ["M", "M1"]:
                logger.info(f"Employee {emp}: Using MT pattern with M/M1 start")
                new_row = ['M'] * eachrep
                new_row2 = (['T'] * 14 + ['M'] * 14) * ((reshaped_final_3.shape[1] // 2 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MTT" and semana1 in ["M", "M1"]:
                logger.info(f"Employee {emp}: Using MTT pattern with M/M1 start")
                new_row = ['M'] * eachrep
                new_row2 = (['T'] * 14 + ['T'] * 14 + ['M'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MTT" and semana1 == "T1":
                logger.info(f"Employee {emp}: Using MTT pattern with T1 start")
                new_row = ['T'] * eachrep
                new_row2 = (['T'] * 14 + ['M'] * 14 + ['T'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MTT" and semana1 == "T2":
                logger.info(f"Employee {emp}: Using MTT pattern with T2 start")
                new_row = ['T'] * eachrep
                new_row2 = (['M'] * 14 + ['T'] * 14 + ['T'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MMT" and semana1 == "M1":
                logger.info(f"Employee {emp}: Using MMT pattern with M1 start")
                new_row = ['M'] * eachrep
                new_row2 = (['M'] * 14 + ['T'] * 14 + ['M'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MMT" and semana1 == "M2":
                logger.info(f"Employee {emp}: Using MMT pattern with M2 start")
                new_row = ['M'] * eachrep
                new_row2 = (['T'] * 14 + ['M'] * 14 + ['M'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MMT" and semana1 in ["T", "T1"]:
                logger.info(f"Employee {emp}: Using MMT pattern with T/T1 start")
                new_row = ['T'] * eachrep
                new_row2 = (['M'] * 14 + ['M'] * 14 + ['T'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            else:
                # Default case
                logger.info(f"Employee {emp}: Using DEFAULT case with seq_turno='{seq_turno}'")
                new_row = [seq_turno] * reshaped_final_3.shape[1]
                new_row = [emp] + new_row[1:]

            logger.info(f"Employee {emp}: Created new_row with length {len(new_row)}")
            logger.info(f"Employee {emp}: new_row first 10 elements: {new_row[:10]}")
            logger.info(f"Employee {emp}: new_row last 10 elements: {new_row[-10:]}")

            
            # Trim to match matrix width
            elements_to_drop = len(new_row) - reshaped_final_3.shape[1]
            logger.info(f"Employee {emp}: elements_to_drop={elements_to_drop}")
            
            if elements_to_drop > 0:
                logger.info(f"Employee {emp}: Trimming {elements_to_drop} elements from new_row")
                new_row = new_row[:len(new_row) - elements_to_drop]
            elif elements_to_drop < 0:
                logger.info(f"Employee {emp}: Extending new_row with {abs(elements_to_drop)} '-' elements")
                new_row.extend(['-'] * abs(elements_to_drop))
            
            logger.info(f"Employee {emp}: Final new_row length: {len(new_row)}")
            #logger.info(f"Employee {emp}: Final new_row: {new_row}")
            
            # Add row to matrix
            new_row_df = pd.DataFrame([new_row], columns=reshaped_final_3.columns)
            reshaped_final_3 = pd.concat([reshaped_final_3, new_row_df], ignore_index=True)
            
            logger.info(f"Employee {emp}: Matrix shape after adding row: {reshaped_final_3.shape}")
        
        # Reset column and row names
        reshaped_final_3.columns = range(reshaped_final_3.shape[1])
        reshaped_final_3.reset_index(drop=True, inplace=True)
        
        logger.info(f"=== CALENDAR CREATION DEBUG END ===")
        logger.info(f"Final reshaped_final_3 shape: {reshaped_final_3.shape}")
        logger.info(f"Final matrix first column (employee IDs): {reshaped_final_3.iloc[:, 0].tolist()}")
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in create_mt_mtt_cycles: {str(e)}")
        return reshaped_final_3

def assign_days_off(reshaped_final_3: pd.DataFrame, df_daysoff_final: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R assign_days_off function to Python.
    Assign days off to employees in the schedule matrix.
    
    Args:
        reshaped_final_3: Schedule matrix DataFrame
        df_daysoff_final: DataFrame with days off data
        
    Returns:
        Updated schedule matrix with days off assigned
    """
    try:
        emps = df_daysoff_final['employee_id'].unique()
        
        for emp in emps:
            df_daysoff = df_daysoff_final[df_daysoff_final['employee_id'] == emp]
            
            for _, dayoff_row in df_daysoff.iterrows():
                date_temp = str(dayoff_row['schedule_dt'])
                date = date_temp[:10]
                val = dayoff_row['sched_subtype']
                
                # Find employee row index
                row_indices = []
                for row_idx, row_data in reshaped_final_3.iterrows():
                    if str(emp) in row_data.values:
                        row_indices.append(row_idx)
                
                if not row_indices:
                    continue
                    
                row_index = row_indices[0]
                
                # Find column indices for this date
                col_indices = []
                for col_idx, col_data in reshaped_final_3.items():
                    if date in col_data.values:
                        col_indices.append(col_idx)
                
                if len(col_indices) >= 2:
                    reshaped_final_3.iloc[row_index, col_indices[0]] = val
                    reshaped_final_3.iloc[row_index, col_indices[1]] = val
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in assign_days_off: {str(e)}")
        return reshaped_final_3

def assign_empty_days(df_tipo_contrato: pd.DataFrame, reshaped_final_3: pd.DataFrame,
                     not_in_pre_ger: List[str], df_feriados_filtered: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R assign_empty_days function to Python.
    Assign empty days based on contract types.
    
    Args:
        df_tipo_contrato: DataFrame with contract type information
        reshaped_final_3: Schedule matrix DataFrame
        not_in_pre_ger: List of employees not in pre-generated schedules
        df_feriados_filtered: DataFrame with filtered holiday data
        
    Returns:
        Updated schedule matrix with empty days assigned
    """
    try:
        weekday_contrato2 = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        weekday_contrato3 = ['Monday', 'Tuesday', 'Wednesday', 'Thursday']
        absence_types = ['A', 'AP', 'V']
        
        for emp in not_in_pre_ger:
            # Get contract type for this employee
            emp_contract = df_tipo_contrato[df_tipo_contrato['emp'] == emp]
            if len(emp_contract) == 0:
                continue
                
            tipo_de_contrato = emp_contract['tipo_contrato'].iloc[0]
            
            logger.info(f"Colab: {emp}, Tipo de Contrato: {tipo_de_contrato}")
            
            # Find employee row index
            row_indices = []
            for row_idx, row_data in reshaped_final_3.iterrows():
                if emp in row_data.values:
                    row_indices.append(row_idx)
            
            if not row_indices:
                continue
                
            row_index = row_indices[0]
            
            # Skip if contract type is 6
            if tipo_de_contrato == 6:
                logger.info("Tipo de contrato = 6, do nothing, next emp (loop)")
                continue
            
            # Process each date column (step by 2 for M/T pairs)
            for i in range(1, reshaped_final_3.shape[1] - 1, 2):
                date_temp = str(reshaped_final_3.iloc[0, i])
                date = date_temp[:10]
                
                try:
                    weekday = pd.to_datetime(date).day_name()
                    type_of_day = str(reshaped_final_3.iloc[0, i])
                    
                    # Check holiday type
                    holiday_matches = df_feriados_filtered[
                        df_feriados_filtered['data'] == pd.to_datetime(type_of_day)
                    ]
                    type_of_hol = holiday_matches['tipo'].iloc[0] if len(holiday_matches) > 0 else '-'
                    
                    assigned_value = str(reshaped_final_3.iloc[row_index, i])
                    
                    # Apply contract type rules
                    if type_of_hol == 3:
                        # Closed holiday
                        reshaped_final_3.iloc[row_index, i:i+2] = 'F'
                    elif (tipo_de_contrato == 3 and 
                          weekday in weekday_contrato3 and 
                          type_of_hol != 2 and 
                          assigned_value not in absence_types):
                        reshaped_final_3.iloc[row_index, i:i+2] = '-'
                    elif (tipo_de_contrato == 2 and 
                          weekday in weekday_contrato2 and 
                          type_of_hol != 2 and 
                          assigned_value not in absence_types):
                        reshaped_final_3.iloc[row_index, i:i+2] = '-'
                        
                except Exception as date_error:
                    logger.warning(f"Error processing date {date}: {str(date_error)}")
                    continue
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in assign_empty_days: {str(e)}")
        return reshaped_final_3

def add_trads_code(df_cycle90_info_filtered: pd.DataFrame, lim_sup_manha: str, lim_inf_tarde: str) -> pd.DataFrame:
    """
    Convert R add_trads_code function to Python.
    Add TRADS codes to 90-day cycle information.
    
    Args:
        df_cycle90_info_filtered: DataFrame with 90-day cycle information
        lim_sup_manha: Morning limit time
        lim_inf_tarde: Afternoon limit time
        
    Returns:
        DataFrame with TRADS codes added
    """
    try:
        # Convert time columns to datetime
        time_cols = ['hora_ini_1', 'hora_ini_2', 'hora_fim_1', 'hora_fim_2']
        for col in time_cols:
            if col in df_cycle90_info_filtered.columns:
                df_cycle90_info_filtered[col] = pd.to_datetime(
                    df_cycle90_info_filtered[col], format="%Y-%m-%d %H:%M:%S", errors='coerce'
                )
        
        # Convert limit times
        lim_sup_manha = pd.to_datetime(lim_sup_manha, format="%Y-%m-%d %H:%M:%S", errors='coerce')
        
        # Calculate interval and max exit time
        df_cycle90_info_filtered['intervalo'] = np.where(
            df_cycle90_info_filtered['hora_ini_2'].isna(),
            0,
            (df_cycle90_info_filtered['hora_ini_2'] - df_cycle90_info_filtered['hora_fim_1']).dt.total_seconds() / 3600
        )
        
        df_cycle90_info_filtered['max_exit'] = (
            df_cycle90_info_filtered[['hora_ini_1', 'hora_fim_1', 'hora_ini_2', 'hora_fim_2']].max(axis=1) - 
            pd.Timedelta(minutes=15)
        )

        # Calculate intervalo column
        #df_cycle90_info_filtered['intervalo'] = df_cycle90_info_filtered.apply(
        #    lambda row: 0 if pd.isna(row['hora_ini_2']) else 
        #    (pd.to_datetime(row['hora_ini_2']) - pd.to_datetime(row['hora_fim_1'])).total_seconds() / 3600,
        #    axis=1
        #)
#
        # Calculate max_exit column
        #time_columns = ['hora_ini_1', 'hora_fim_1', 'hora_ini_2', 'hora_fim_2']
        #df_cycle90_info_filtered['max_exit'] = df_cycle90_info_filtered[time_columns].apply(
        #    lambda row: pd.to_datetime(row.dropna()).max() - pd.Timedelta(minutes=15),
        #    axis=1
        #)
        
        # Apply TRADS code logic
        def get_trads_code(row):
            tipo_dia = row['tipo_dia']
            descanso = row['descanso']
            horario_ind = row['horario_ind']
            dia_semana = row['dia_semana']
            intervalo = row['intervalo']
            max_exit = row['max_exit']
           
            # Log every row to understand the mapping
            #log_msg = f"[TRADS-MAP] tipo_dia='{tipo_dia}', descanso='{descanso}', horario_ind='{horario_ind}', dia_semana={dia_semana}, intervalo={intervalo}, max_exit={max_exit}"
           
            if tipo_dia == 'F' and (dia_semana == 1 or dia_semana == 8):
                #logger.info(f"{log_msg} → 'L_DOM' (tipo_dia='F' on Sunday/Monday)")
                return 'L_DOM'
            elif tipo_dia == 'F':
                #logger.info(f"{log_msg} → 'L' (tipo_dia='F': free/holiday)")
                return 'L'
            elif tipo_dia == 'A' and (descanso == 'A' or descanso == 'R') and horario_ind == 'N':
                #logger.info(f"{log_msg} → 'MoT' (Active + no rest + no individual schedule)")
                return 'MoT'
            elif tipo_dia == 'A' and (descanso == 'A' or descanso == 'R') and horario_ind == 'S' and max_exit >= lim_sup_manha:
                #logger.info(f"{log_msg} → 'T' (Active + no rest + horario_ind='S' + max_exit >= {lim_sup_manha})")
                return 'T'
            elif tipo_dia == 'A' and (descanso == 'A' or descanso == 'R') and horario_ind == 'S' and max_exit < lim_sup_manha:
                #logger.info(f"{log_msg} → 'M' (Active + no rest + horario_ind='S' + max_exit < {lim_sup_manha})")
                return 'M'
            elif tipo_dia == 'S':
                #logger.info(f"{log_msg} → '-' (tipo_dia='S': suspended/missing)")
                return '-'
            elif tipo_dia == 'A' and (descanso == 'R' or descanso == 'N') and intervalo >= 1:
                #logger.info(f"{log_msg} → 'P' (Active + rest/night + break >= 1h)")
                return 'P'
            elif tipo_dia == 'A' and (descanso == 'R' or descanso == 'N') and intervalo < 1 and max_exit >= lim_sup_manha:
                #logger.info(f"{log_msg} → 'T' (Active + rest/night + break < 1h + max_exit >= {lim_sup_manha})")
                return 'T'
            elif tipo_dia == 'A' and (descanso == 'R' or descanso == 'N') and intervalo < 1 and max_exit < lim_sup_manha:
                #logger.info(f"{log_msg} → 'M' (Active + rest/night + break < 1h + max_exit < {lim_sup_manha})")
                return 'M'
            elif tipo_dia == 'A' and descanso == 'A' and horario_ind == 'Y' and intervalo < 1 and max_exit >= lim_sup_manha:
                #logger.info(f"{log_msg} → 'T' (Active + no rest + individual='Y' + max_exit >= {lim_sup_manha})")
                return 'T'
            elif tipo_dia == 'A' and descanso == 'A' and horario_ind == 'Y' and intervalo < 1 and max_exit < lim_sup_manha:
                #logger.info(f"{log_msg} → 'M' (Active + no rest + individual='Y' + max_exit < {lim_sup_manha})")
                return 'M'
            elif tipo_dia == 'N':
                #logger.info(f"{log_msg} → 'NL' (tipo_dia='N': night shift)")
                return 'NL'
            else:
                #logger.warning(f"{log_msg} → '-' (NO CONDITION MATCHED - this is why you get '-'!)")
                return '-'
        
        df_cycle90_info_filtered['codigo_trads'] = df_cycle90_info_filtered.apply(get_trads_code, axis=1)
        
        return df_cycle90_info_filtered
        
    except Exception as e:
        logger.error(f"Error in add_trads_code: {str(e)}")
        return df_cycle90_info_filtered

def assign_90_cycles(reshaped_final_3: pd.DataFrame, df_cycle90_info_filtered: pd.DataFrame,
                    colab: int, matriz_festivos: pd.DataFrame, lim_sup_manha: str, lim_inf_tarde: str,
                    day: str, reshaped_col_index: list, reshaped_row_index: list, matricula: str) -> pd.DataFrame:
    """
    Convert R assign_90_cycles function to Python.
    Assign 90-day cycles to the schedule matrix.
    
    Args:
        reshaped_final_3: Schedule matrix DataFrame
        df_cycle90_info_filtered: Filtered 90-day cycle information
        colab: Employee ID
        matriz_festivos: Holiday matrix
        lim_sup_manha: Morning limit time
        lim_inf_tarde: Afternoon limit time
        day: Current day string
        reshaped_col_index: Column index in matrix
        reshaped_row_index: Row index in matrix
        matricula: Employee matricula
        
    Returns:
        Updated schedule matrix with 90-day cycles assigned
    """
    try:
        # Convert time limits
        lim_sup_manha = f"2000-01-01 {lim_sup_manha}"
        lim_sup_manha = pd.to_datetime(lim_sup_manha, format="%Y-%m-%d %H:%M")
        lim_inf_tarde = f"2000-01-01 {lim_inf_tarde}"
        lim_inf_tarde = pd.to_datetime(lim_inf_tarde, format="%Y-%m-%d %H:%M")
        #logger.info(f"DEBUG: lim_sup_manha: {lim_sup_manha}")
        #logger.info(f"DEBUG: lim_inf_tarde: {lim_inf_tarde}")
        
        # Add TRADS codes
        df_cycle90_info_filtered = add_trads_code(df_cycle90_info_filtered, 
                                                 lim_sup_manha.strftime("%Y-%m-%d %H:%M:%S"), 
                                                 lim_inf_tarde.strftime("%Y-%m-%d %H:%M:%S"))
        
        #logger.info(f"DEBUG: df_cycle90_info_filtered: {df_cycle90_info_filtered}")

        # Reset row names
        reshaped_final_3.reset_index(drop=True, inplace=True)
        
        # Get holidays as list of strings
        # TODO: remove non used variables. during code convertion we didnt find the need for this variable
        #festivos = [str(date) for date in matriz_festivos['data']]
        
        # Process the specific day range
        if isinstance(reshaped_col_index, list) and len(reshaped_col_index) >= 2:
            col_range = [reshaped_col_index[0], reshaped_col_index[1]]
        else:
            col_range = [reshaped_col_index]

        #logger.info(f"DEBUG: col_range: {col_range}")
        
        for k in col_range:
            
            day_number = pd.to_datetime(day).weekday() + 1  # Convert to 1-7 format
            
            # Find matching cycle row for this day
            cycle_rows = df_cycle90_info_filtered[
                df_cycle90_info_filtered['schedule_day'].dt.strftime('%Y-%m-%d') == day
            ]

            #logger.info(f"DEBUG: cycle_rows: {cycle_rows}")
            #logger.info(f"DEBUG: k: {k}")
            
            if len(cycle_rows) > 0:
                cycle_row = cycle_rows.iloc[0]
                val = cycle_row.get('codigo_trads', '-')
                reshaped_final_3.iloc[reshaped_row_index, k] = val

        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in assign_90_cycles: {str(e)}")
        return reshaped_final_3

def load_pre_ger_scheds(df_pre_ger: pd.DataFrame, employees_tot: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    """
    Convert R load_pre_ger_scheds function to Python.
    Load pre-generated schedules.
    
    Args:
        df_pre_ger: DataFrame with pre-generated schedule data
        employees_tot: List of all employees
        
    Returns:
        Tuple of (reshaped schedule matrix, list of employees with pre-generated schedules)
    """
    try:
        if len(df_pre_ger) == 0:
            return pd.DataFrame(), []
        
        # Rename first column
        df_pre_ger.columns = ['employee_id'] + list(df_pre_ger.columns[1:])
        
        # Get employees with pre-generated schedules
        emp_pre_ger = df_pre_ger['employee_id'].unique().tolist()
        
        # Filter for 'P' indicator
        df_pre_ger_filtered = df_pre_ger[
            df_pre_ger['ind'] == 'P'
        ].drop('ind', axis=1)
        
        # Check for duplicates before pivot to understand why they exist
        initial_rows = len(df_pre_ger_filtered)
        duplicates_check = df_pre_ger_filtered.groupby(['employee_id', 'schedule_dt']).size()
        duplicate_combinations = duplicates_check[duplicates_check > 1]
        
        if len(duplicate_combinations) > 0:
            logger.warning(f"Found {len(duplicate_combinations)} duplicate combinations of employee_id + schedule_dt:")
            for (emp_id, schedule_dt), count in duplicate_combinations.head(10).items():
                logger.warning(f"  Employee {emp_id}, Date {schedule_dt}: {count} occurrences")
            
            # Show sample duplicate rows for debugging
            sample_duplicates = df_pre_ger_filtered[df_pre_ger_filtered.set_index(['employee_id', 'schedule_dt']).index.duplicated(keep=False)]
            logger.warning(f"Sample duplicate rows (first 5):")
            for idx, row in sample_duplicates.head(5).iterrows():
                logger.warning(f"  Row {idx}: employee_id={row['employee_id']}, schedule_dt={row['schedule_dt']}, sched_subtype={row['sched_subtype']}")
        else:
            logger.info("No duplicate combinations found in df_pre_ger_filtered")
        
        # Remove duplicates before pivot to avoid reindexing errors
        df_pre_ger_filtered = df_pre_ger_filtered.drop_duplicates(subset=['employee_id', 'schedule_dt'])
        final_rows = len(df_pre_ger_filtered)
        
        if initial_rows != final_rows:
            logger.warning(f"Removed {initial_rows - final_rows} duplicate rows (from {initial_rows} to {final_rows})")
        
        # Check data before pivot
        logger.info(f"schedule_dt data type: {df_pre_ger_filtered['schedule_dt'].dtype}")
        logger.info(f"schedule_dt sample values: {df_pre_ger_filtered['schedule_dt'].head().tolist()}")
        logger.info(f"Unique employees: {len(df_pre_ger_filtered['employee_id'].unique())}")
        logger.info(f"Unique dates: {len(df_pre_ger_filtered['schedule_dt'].unique())}")
        
        # Pivot wider with error handling
        logger.info(f"Attempting pivot with {len(df_pre_ger_filtered)} rows")
        try:
            reshaped = df_pre_ger_filtered.pivot_table(
                index='employee_id', 
                columns='schedule_dt', 
                values='sched_subtype', 
                aggfunc='first'
            ).reset_index()
            logger.info(f"Pivot successful: {reshaped.shape}")
        except Exception as pivot_error:
            logger.error(f"Pivot failed: {pivot_error}")
            # Try alternative approach - check for actual duplicate column names
            logger.info("Checking for potential duplicate column names...")
            unique_dates = df_pre_ger_filtered['schedule_dt'].unique()
            logger.info(f"Number of unique dates: {len(unique_dates)}")
            logger.info(f"Sample unique dates: {unique_dates[:5].tolist()}")
            
            # Try using unstack instead
            logger.info("Attempting alternative pivot using unstack...")
            df_indexed = df_pre_ger_filtered.set_index(['employee_id', 'schedule_dt'])['sched_subtype']
            reshaped = df_indexed.unstack().reset_index()
            logger.info(f"Unstack successful: {reshaped.shape}")
        
        # Create column names row
        column_names = pd.DataFrame([reshaped.columns.tolist()], columns=reshaped.columns)
        column_names.iloc[0, 0] = "Dia"
        
        # Convert employee_id to string
        reshaped['employee_id'] = reshaped['employee_id'].astype(str)
        
        # Combine column names with data
        reshaped_names = pd.concat([column_names, reshaped], ignore_index=True)
        
        # Duplicate columns to get M/T shifts - INTERLEAVED to match main calendario structure
        first_col = reshaped_names.iloc[:, [0]]
        last_cols = reshaped_names.iloc[:, 1:]
        
        # Sort columns BEFORE duplication to avoid duplicate column name issues
        last_cols = last_cols.reindex(sorted(last_cols.columns), axis=1)
        
        # Interleave columns: date1, date1, date2, date2, date3, date3, ...
        # This matches the main calendario structure where each date appears twice (M shift, T shift)
        duplicated_cols_list = []
        for col in last_cols.columns:
            duplicated_cols_list.append(last_cols[[col]])  # Add column once
            duplicated_cols_list.append(last_cols[[col]])  # Add column twice (duplicate)
        
        duplicated_cols = pd.concat(duplicated_cols_list, axis=1)
        logger.info(f"Duplicated columns interleaved - original dates: {len(last_cols.columns)}, after duplication: {duplicated_cols.shape[1]}")
        
        # Combine first column with duplicated columns
        reshaped_final = pd.concat([first_col, duplicated_cols], axis=1)
        
        # Reset column and row names
        reshaped_final.columns = range(reshaped_final.shape[1])
        reshaped_final.reset_index(drop=True, inplace=True)
        
        # Log the date structure to verify interleaving
        logger.info(f"Date row after interleaving (first 15 cols): {reshaped_final.iloc[0, :15].tolist()}")
        
        # Create TURNO row
        new_row = ['M' if i % 2 == 1 else 'T' for i in range(reshaped_final.shape[1])]
        new_row[0] = "TURNO"
        
        # Trim to match matrix width
        elements_to_drop = len(new_row) - reshaped_final.shape[1]
        if elements_to_drop > 0:
            new_row = new_row[:len(new_row) - elements_to_drop]
        
        # Combine first row, TURNO row, and remaining rows
        reshaped_final_1 = reshaped_final.iloc[[0]]
        new_row_df = pd.DataFrame([new_row], columns=reshaped_final.columns)
        reshaped_final_2 = reshaped_final.iloc[1:]
        
        reshaped_final_3 = pd.concat([reshaped_final_1, new_row_df, reshaped_final_2], ignore_index=True)
        reshaped_final_3.columns = range(reshaped_final_3.shape[1])
        
        # Log final structure to verify it matches main calendario format
        logger.info(f"Final matrix structure (first 2 rows, first 15 cols):")
        logger.info(f"  Row 0 (Dates): {reshaped_final_3.iloc[0, :15].tolist()}")
        logger.info(f"  Row 1 (TURNO): {reshaped_final_3.iloc[1, :15].tolist()}")
        if reshaped_final_3.shape[0] > 2:
            logger.info(f"  Row 2 (First employee): {reshaped_final_3.iloc[2, :15].tolist()}")
        
        return reshaped_final_3, emp_pre_ger
        
    except Exception as e:
        logger.error(f"Error in load_pre_ger_scheds: {str(e)}")
        return pd.DataFrame(), []

def count_days_in_week(date_str: str) -> int:
    """
    Helper function to count days in a week.
    Simplified implementation - you may need to adjust based on business logic.
    
    Args:
        date_str: Date string
        
    Returns:
        Number of days (default 7)
    """
    # Ensure the date is a datetime object
    if isinstance(date_str, str):
        date_str = pd.to_datetime(date_str)
    elif isinstance(date_str, datetime):
        date_str = pd.to_datetime(date_str)
    
    # Convert pandas weekday (0=Monday, 6=Sunday) to R's wday (1=Sunday, 7=Saturday)
    pandas_weekday = date_str.weekday()
    r_weekday = 1 if pandas_weekday == 6 else pandas_weekday + 2
    
    # If the date is a Sunday (wday=1), move to the previous Monday
    if r_weekday == 1:
        start_of_week = date_str - timedelta(days=6)
    else:
        # Otherwise, find the Monday of the given week
        start_of_week = date_str - timedelta(days=r_weekday - 2)
    
    # Find the Sunday of the given week
    end_of_week = start_of_week + timedelta(days=6)
    
    # Generate a sequence of dates from Monday to Sunday
    week_days = pd.date_range(start=start_of_week, end=end_of_week, freq='D')
    
    # Count the number of days from the given date onwards
    num_days = len(week_days[week_days >= date_str])
    
    return num_days

def load_wfm_scheds(df_pre_ger: pd.DataFrame, employees_tot_pad: List[str]) -> Tuple[pd.DataFrame, List[str], pd.DataFrame]:
    """
    Convert R load_WFM_scheds function to Python - simplified version.
    """
    try:
        if len(df_pre_ger) == 0:
            return pd.DataFrame(), [], pd.DataFrame()
        
        # Basic processing
        df_pre_ger = df_pre_ger.copy()
        logger.info(f"load_wfm_scheds - Input df_pre_ger shape: {df_pre_ger.shape}, columns: {df_pre_ger.columns.tolist()}")
        logger.info(f"load_wfm_scheds - First 5 rows BEFORE column rename:\n{df_pre_ger.head()}")
        
        df_pre_ger.columns = ['employee_id'] + list(df_pre_ger.columns[1:])
        
        # Check if TYPE and SUBTYPE columns exist before conversion
        if 'TYPE' in df_pre_ger.columns or 'type' in df_pre_ger.columns:
            logger.info(f"load_wfm_scheds - TYPE/SUBTYPE found, will convert via convert_types_in()")
            if 'TYPE' in df_pre_ger.columns:
                logger.info(f"TYPE/SUBTYPE combinations:\n{df_pre_ger[['TYPE', 'SUBTYPE']].value_counts().head(20)}")
            elif 'type' in df_pre_ger.columns:
                logger.info(f"type/subtype combinations:\n{df_pre_ger[['type', 'subtype']].value_counts().head(20)}")
        
        # Convert WFM types to TRADS and get unique employees
        df_pre_ger = convert_types_in(df_pre_ger)
        logger.info(f"load_wfm_scheds - AFTER convert_types_in, first 5 rows:\n{df_pre_ger.head()}")
        if 'sched_subtype' in df_pre_ger.columns:
            logger.info(f"sched_subtype value counts after conversion:\n{df_pre_ger['sched_subtype'].value_counts()}")
        
        emp_pre_ger = df_pre_ger['employee_id'].unique().tolist()
        
        # Fill missing dates and pivot to matrix format
        # Map schedule_day to schedule_dt for compatibility
        schedule_day_col = 'SCHEDULE_DAY' if 'SCHEDULE_DAY' in df_pre_ger.columns else 'schedule_day'
        if schedule_day_col in df_pre_ger.columns:
            logger.info(f"load_wfm_scheds - Converting {schedule_day_col} to schedule_dt")
            df_pre_ger['schedule_dt'] = pd.to_datetime(df_pre_ger[schedule_day_col]).dt.strftime('%Y-%m-%d')
        else:
            logger.warning(f"load_wfm_scheds - Neither 'SCHEDULE_DAY' nor 'schedule_day' found in columns: {df_pre_ger.columns.tolist()}")
        df_pre_ger['sched_subtype'] = df_pre_ger['sched_subtype'].fillna('-')
        
        # Count days off
        df_count = df_pre_ger.groupby('employee_id')['sched_subtype'].apply(
            lambda x: (x.isin(['L', 'LD', 'LQ', 'F', 'V', '-'])).sum()
        ).reset_index(name='days_off_count')
        
        # Use the same reshaping logic as load_pre_ger_scheds
        logger.info(f"load_wfm_scheds - Calling load_pre_ger_scheds with df shape: {df_pre_ger.shape}")
        reshaped_final_3, _ = load_pre_ger_scheds(df_pre_ger, employees_tot_pad)
        logger.info(f"load_wfm_scheds - AFTER load_pre_ger_scheds, reshaped_final_3 shape: {reshaped_final_3.shape}")
        if not reshaped_final_3.empty:
            logger.info(f"Reshaped matrix (first 5 rows, first 10 cols):\n{reshaped_final_3.iloc[:5, :10]}")
        
        return reshaped_final_3, emp_pre_ger, df_count
        
    except Exception as e:
        logger.error(f"Error in load_wfm_scheds: {str(e)}", exc_info=True)
        return pd.DataFrame(), [], pd.DataFrame()

def convert_types_in(df: pd.DataFrame) -> pd.DataFrame:
    """Convert WFM types to TRADS - simple mapping."""
    type_map = {
        ('T', 'M'): 'M', ('T', 'T'): 'T', ('T', 'H'): 'MoT', ('T', 'P'): 'P',
        ('F', None): 'L', ('F', 'D'): 'LD', ('F', 'Q'): 'LQ', ('F', 'C'): 'C',
        ('R', None): 'F', ('N', None): '-', ('T', 'A'): 'V'
    }
    
    # Check if columns are uppercase or lowercase
    type_col = 'TYPE' if 'TYPE' in df.columns else 'type'
    subtype_col = 'SUBTYPE' if 'SUBTYPE' in df.columns else 'subtype'
    
    logger.info(f"convert_types_in - Using columns: {type_col}, {subtype_col}")
    logger.info(f"convert_types_in - Sample raw values before mapping (first 10):")
    for idx, row in df.head(10).iterrows():
        type_val = row.get(type_col)
        subtype_val = row.get(subtype_col)
        mapped_val = type_map.get((type_val, subtype_val), '-')
        logger.info(f"  Row {idx}: ({type_val}, {subtype_val}) -> {mapped_val}")
    
    df['sched_subtype'] = df.apply(
        lambda row: type_map.get((row.get(type_col), row.get(subtype_col)), '-'), axis=1
    )
    df['ind'] = 'P'
    
    logger.info(f"convert_types_in - Conversion complete. sched_subtype value counts:\n{df['sched_subtype'].value_counts()}")
    
    return df

def count_dates_per_year(start_date_str: str, end_date_str: str) -> str:
    """
    Convert R count_dates_per_year function to Python.
    Count dates per year in a date range and return the year with most dates.
    
    Args:
        start_date_str: Start date as string (YYYY-MM-DD format)
        end_date_str: End date as string (YYYY-MM-DD format)
        
    Returns:
        Year with the most dates as string
    """
    try:
        # Convert input strings to date format
        start_date = pd.to_datetime(start_date_str)
        end_date = pd.to_datetime(end_date_str)
        
        # Generate sequence of dates
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Extract the unique years from the date sequence
        years = dates.year.unique()
        
        # Initialize a dictionary to store the count of dates for each year
        year_counts = {}
        
        # Count the number of dates for each year
        for year in years:
            year_counts[str(year)] = (dates.year == year).sum()
        
        # Display the counts for each year
        logger.info(f"Date counts per year: {year_counts}")
        
        # Determine which year has the most dates
        year_with_most_dates = max(year_counts.keys(), key=lambda x: year_counts[x])
        
        # Output the year with the most dates
        logger.info(f"Year with most dates is: {year_with_most_dates}")
        
        return year_with_most_dates
        
    except Exception as e:
        logger.error(f"Error in count_dates_per_year: {str(e)}")
        # Return current year as fallback
        return str(pd.Timestamp.now().year)

def get_limit_mt(matricula: str, df_colaborador: pd.DataFrame) -> Tuple[str, str]:
    """
    Get MT (Morning/Afternoon) time limits for a specific employee from df_colaborador.
    
    Args:
        matricula: Employee matricula (ID)
        df_colaborador: DataFrame containing employee data with limit columns
        
    Returns:
        Tuple of (lim_sup_manha, lim_inf_tarde) as time strings
    """
    try:
        # Filter df_colaborador for this specific employee
        employee_data = df_colaborador[df_colaborador['matricula'] == matricula]
        
        if len(employee_data) == 0:
            logger.warning(f"No employee data found for matricula {matricula}")
            # Return default values
            return "12:00", "14:00"
        
        # Get the first matching record
        emp_record = employee_data.iloc[0]
        
        # Extract the limit columns
        lim_sup_manha = str(emp_record.get('limite_superior_manha', '12:00'))
        lim_inf_tarde = str(emp_record.get('limite_inferior_tarde', '14:00'))
        
        # Handle potential None or NaN values
        if pd.isna(lim_sup_manha) or lim_sup_manha == 'nan':
            lim_sup_manha = "12:00"
        if pd.isna(lim_inf_tarde) or lim_inf_tarde == 'nan':
            lim_inf_tarde = "14:00"
        
        logger.debug(f"Retrieved MT limits for matricula {matricula}: morning={lim_sup_manha}, afternoon={lim_inf_tarde}")
        
        return lim_sup_manha, lim_inf_tarde
        
    except Exception as e:
        logger.error(f"Error in get_limit_mt for matricula {matricula}: {str(e)}")
        # Return default values in case of error
        return "12:00", "14:00"

def pad_zeros(value: str, length: int = 10) -> str:
    """
    Helper function to pad employee IDs with zeros.
    
    Args:
        value: Value to pad
        length: Total length after padding
        
    Returns:
        Padded string
    """
    try:
        return str(value).zfill(length)
    except Exception as e:
        logger.error(f"Error in pad_zeros: {str(e)}")
        return str(value)
    
def calcular_max(sequencia: List[float]) -> float:
    """
    Helper method to calculate maximum using ocorrencia_A and ocorrencia_B logic.
    """
    try:
        valor_a = ocorrencia_a(sequencia)
        valor_b = ocorrencia_b(sequencia)
        return max(valor_a, valor_b)
    except:
        return max(sequencia) if sequencia else 0

def ocorrencia_a(sequencia: List[float]) -> float:
    """
    Convert R ocorrencia_A function to Python.
    """
    valores_unicos = sorted(set(sequencia), reverse=True)
    n = len(sequencia)
    
    resultado_pass1 = float('-inf')
    
    if n == 1:
        return sequencia[0]
    
    if n == 2:
        return sum(sequencia) / len(sequencia)
    
    # Step 1: Look for 3 consecutive occurrences of the highest value
    for valor in valores_unicos:
        for i in range(n - 2):
            if (sequencia[i] == valor and 
                sequencia[i + 1] == valor and 
                sequencia[i + 2] == valor):
                resultado_pass1 = valor
                break
        if resultado_pass1 != float('-inf'):
            break
    
    # Step 2: If not found, follow second rule
    if len(valores_unicos) < 3:
        x = valores_unicos
    else:
        x = valores_unicos[:3]
    
    melhor_sequencia = None
    melhor_soma = float('-inf')
    resultado_pass2 = float('-inf')
    
    for i in range(n - 2):
        sub_seq = sequencia[i:i + 3]
        if all(val in x for val in sub_seq):
            soma_seq = sum(sub_seq)
            if soma_seq > melhor_soma:
                melhor_soma = soma_seq
                melhor_sequencia = sub_seq
    
    if melhor_sequencia is not None:
        resultado_pass2 = min(melhor_sequencia)
    
    return max(resultado_pass1, resultado_pass2)

def ocorrencia_b(sequencia: List[float]) -> float:
    """
    Convert R ocorrencia_B function to Python.
    """
    valores_unicos = sorted(set(sequencia), reverse=True)
    n = len(sequencia)
    
    if n == 1:
        return sequencia[0]
    
    # Step 1: Calculate the 3 maximum values of the sequence
    if len(valores_unicos) < 3:
        maximos = valores_unicos
    else:
        maximos = valores_unicos[:3]
    
    # Step 2: Check the largest of these maximum values and if there are at least 2 situations of 2 consecutive
    maior_valor = max(maximos)
    contagem_consecutiva = 0
    i = 0
    
    while i <= n - 2:
        if sequencia[i] == maior_valor and sequencia[i + 1] == maior_valor:
            contagem_consecutiva += 1
            i += 2
        else:
            i += 1
    
    if contagem_consecutiva >= 2:
        return maior_valor
    
    # Step 3: If there are not 2 pairs of the highest value, check pairs among the 3 highest values
    pares_validos = []
    i = 0
    while i <= n - 2:
        if sequencia[i] in maximos and sequencia[i + 1] in maximos:
            pares_validos.append(sequencia[i:i + 2])
            i += 2
        else:
            i += 1
    
    if len(pares_validos) >= 2:
        soma_maxima = float('-inf')
        melhor_pares = None
        
        for j in range(len(pares_validos) - 1):
            for k in range(j + 1, len(pares_validos)):
                soma_atual = sum(pares_validos[j]) + sum(pares_validos[k])
                if soma_atual > soma_maxima:
                    soma_maxima = soma_atual
                    melhor_pares = pares_validos[j] + pares_validos[k]
        
        if melhor_pares:
            return min(melhor_pares)
    
    return -1  # Case where there are not enough pairs, return -1

def count_open_holidays(df_festivos: pd.DataFrame, tipo_contrato: int) -> List[int]:
    """
    Helper method to count open holidays based on contract type.
    For 2-day and 3-day contract types, we need to count the number of days that he is working on a feriado.
    
    Args:
        matriz_festivos: DataFrame with holiday data
        tipo_contrato: Contract type (2 or 3)
        
    Returns:
        List with [l_dom_count, total_working_days]
    """
    try:
        # Convert data column to datetime if not already
        logger.info(f"DEBUG: df_festivos columns: {df_festivos.columns}")
        df_festivos['schedule_day'] = pd.to_datetime(df_festivos['schedule_day'])
        
        if tipo_contrato == 3:
            # Count holidays Monday to Thursday (weekday 0-3)
            weekday_holidays = df_festivos[
                (df_festivos['schedule_day'].dt.weekday >= 0) & 
                (df_festivos['schedule_day'].dt.weekday <= 3)
            ]
        elif tipo_contrato == 2:
            # Count holidays Monday to Friday (weekday 0-4)
            weekday_holidays = df_festivos[
                (df_festivos['schedule_day'].dt.weekday >= 0) & 
                (df_festivos['schedule_day'].dt.weekday <= 4)
            ]
        else:
            weekday_holidays = pd.DataFrame()
        
        l_dom_count = len(weekday_holidays)
        
        # Calculate total working days based on contract type
        # This is a simplified calculation - you may need to adjust based on business rules
        if tipo_contrato == 3:
            total_working_days = 4 * 52  # Approximate: 4 days per week * 52 weeks
        elif tipo_contrato == 2:
            total_working_days = 5 * 52  # Approximate: 5 days per week * 52 weeks
        else:
            total_working_days = 0
            
        return [l_dom_count, total_working_days]
        
    except Exception as e:
        logger.error(f"Error in _count_open_holidays: {str(e)}")
        return [0, 0]
    

def func_turnos(matriz2, tipo):
    """Helper function to process specific shift types (MoT, P, etc.)."""
    # Filter rows that match 'tipo' and update TIPO_TURNO
    matriz2_filtered = matriz2[matriz2['TIPO_TURNO'] == tipo].copy()
    
    if len(matriz2_filtered) > 0:
        # Group by COLABORADOR and DATA, then assign M/T based on row number
        def assign_shift_type(group):
            group = group.copy()
            group.loc[group.index[0], 'TIPO_TURNO'] = 'M'  # First row becomes 'M'
            if len(group) > 1:
                group.loc[group.index[1], 'TIPO_TURNO'] = 'T'  # Second row becomes 'T'
            return group
        
        matriz2_filtered = (matriz2_filtered
                           .groupby(['COLABORADOR', 'DATA'])
                           .apply(assign_shift_type)
                           .reset_index(drop=True))
    
    # Combine the filtered and updated data with the rest of the data
    matriz2_rest = matriz2[matriz2['TIPO_TURNO'] != tipo].copy()
    result = pd.concat([matriz2_rest, matriz2_filtered], ignore_index=True)
    
    return result

def adjusted_isoweek(date):
    """Calculate adjusted ISO week number."""
    import pandas as pd
    
    date = pd.to_datetime(date)
    week = date.isocalendar().week
    month = date.month
    
    # If week is 1 but date is in December
    if week == 1 and month == 12:
        return 53
    return week

def custom_round(x):
    """Custom rounding function."""
    import math
    return math.ceil(x) if (x - math.floor(x)) >= 0.3 else math.floor(x)

def calcular_folgas2(semana_df):
    """Calculate folgas for 2-day contracts."""
    # Filter work days (excluding Sunday)
    dias_trabalho = semana_df[
        (semana_df['WDAY'] != 1) & 
        semana_df['HORARIO'].isin(['H', 'OUT'])
    ]
    
    l_res = 0
    if (len(dias_trabalho[dias_trabalho['dia_tipo'] == 'domYf']) > 0 and 
        len(dias_trabalho[(dias_trabalho['WDAY'] == 7) & 
                         (dias_trabalho['HORARIO'] == 'H') & 
                         (dias_trabalho['dia_tipo'] != 'domYf')]) > 0):
        l_res = 1
    
    # Identify holidays
    feriados = semana_df[
        (semana_df['dia_tipo'] == 'domYf') & 
        (semana_df['WDAY'] != 1) & 
        semana_df['HORARIO'].isin(['H', 'OUT'])
    ]
    
    l_dom = max(len(feriados) - 1, 0) if len(feriados) > 0 else 0
    
    return pd.DataFrame({'L_RES': [l_res], 'L_DOM': [l_dom]})

def calcular_folgas3(semana_df):
    """Calculate folgas for 3-day contracts."""
    # Work days in week
    semana_h = len(semana_df[semana_df['HORARIO'].isin(['H', 'OUT', 'NL3D'])])
    
    if semana_h <= 0:
        return pd.DataFrame({'L_RES': [0], 'L_DOM': [0]})
    
    # Work days excluding Sunday
    dias_trabalho = semana_df[
        (semana_df['WDAY'] != 1) & 
        semana_df['HORARIO'].isin(['H', 'OUT', 'NL3D'])
    ]
    
    # Work days Friday and Saturday
    dias_h = len(dias_trabalho[dias_trabalho['dia_tipo'] != 'domYf'])
    l_res = max(min(dias_h, semana_h - 3), 0)
    
    # Identify holidays
    feriados = semana_df[
        (semana_df['dia_tipo'] == 'domYf') & 
        (semana_df['WDAY'] != 1)
    ]
    
    l_dom = max(len(feriados) - 2, 0) if len(feriados) > 0 else 0
    
    return pd.DataFrame({'L_RES': [l_res], 'L_DOM': [l_dom]})

def get_param_for_posto(df, posto_id, unit_id, secao_id, params_names_list=None):
    """
    Get configuration for a specific posto_id following hierarchy:
    1. Posto-specific (fk_tipo_posto = posto_id)
    2. Section-specific (fk_tipo_posto is null, fk_secao = secao_id)  
    3. Unit-specific (fk_tipo_posto is null, fk_secao is null, fk_unidade = unit_id)
    4. Default (all FKs are null)
    Args:
        df: pd.DataFrame, dataframe with parameters
        posto_id: int, posto ID
        unit_id: int, unit ID  
        secao_id: int, section ID
        params_names_list: list, list of parameter names to retrieve
    Returns:
        dict, configuration for the posto_id
    """
    if params_names_list is None or len(params_names_list) == 0 or not isinstance(params_names_list, list):
        logger.error(f"params_names_list is None or empty")
        return None
    posto_id = str(posto_id)
    secao_id = str(secao_id)
    unit_id = str(unit_id)
    # Filter by parameter names only initially
    df_filtered = df[df['sys_p_name'].isin(params_names_list)].copy()
    
    logger.info(f"DEBUG: Input parameters - posto_id: {posto_id}, secao_id: {secao_id}, unit_id: {unit_id}")
    logger.info(f"DEBUG: df_filtered initial (after param names filter):\n {df_filtered}")
    
    # Keep all potentially relevant rows based on hierarchy rules
    # Build conditions that match the hierarchy patterns
    
    conditions = []
    
    # 1. Posto-specific: fk_tipo_posto = posto_id (most specific)
    if posto_id is not None:
        posto_condition = (df_filtered['fk_tipo_posto'] == posto_id)
        conditions.append(posto_condition)
        logger.info(f"DEBUG: Added posto condition for posto_id={posto_id}")
    
    # 2. Section-specific: fk_tipo_posto is null AND fk_secao = secao_id
    if secao_id is not None:
        section_condition = (
            (df_filtered['fk_tipo_posto'].isna()) & 
            (df_filtered['fk_secao'] == secao_id)
        )
        conditions.append(section_condition)
        logger.info(f"DEBUG: Added section condition for secao_id={secao_id}")
        
        # Debug: check which rows match this condition
        matching_section = df_filtered[section_condition]
        logger.info(f"DEBUG: Rows matching section condition:\n {matching_section}")
    
    # 3. Unit-specific: fk_tipo_posto is null AND fk_secao is null AND fk_unidade = unit_id
    if unit_id is not None:
        unit_condition = (
            (df_filtered['fk_tipo_posto'].isna()) & 
            (df_filtered['fk_secao'].isna()) & 
            (df_filtered['fk_unidade'] == unit_id)
        )
        conditions.append(unit_condition)
        logger.info(f"DEBUG: Added unit condition for unit_id={unit_id}")
    
    # 4. Default: all main FKs are null (fallback)
    default_condition = (
        (df_filtered['fk_tipo_posto'].isna()) & 
        (df_filtered['fk_secao'].isna()) & 
        (df_filtered['fk_unidade'].isna()) &
        (df_filtered['fk_grupo'].isna())
    )
    conditions.append(default_condition)
    logger.info(f"DEBUG: Added default condition")
    
    # Combine all conditions with OR
    if conditions:
        final_condition = conditions[0]
        for condition in conditions[1:]:
            final_condition = final_condition | condition
        df_filtered = df_filtered[final_condition]
    
    # Remove duplicates
    df_filtered = df_filtered.drop_duplicates()

    logger.info(f"DEBUG: df_filtered after hierarchy filtering in helpers.py:\n {df_filtered}")
    
    # Now apply hierarchy for each parameter
    params_dict = {}
    
    for param_name in params_names_list:
        param_rows = df_filtered[df_filtered['sys_p_name'] == param_name]
        
        # Priority 1: Exact posto_id match (most specific)
        if posto_id is not None:
            posto_specific = param_rows[param_rows['fk_tipo_posto'] == posto_id]
            if not posto_specific.empty:
                value = get_value_from_row(posto_specific.iloc[0])
                if value is not None:
                    params_dict[param_name] = value
                    logger.info(f"DEBUG: Found posto-specific param {param_name}:{value}")
                    continue
        
        # Priority 2: Section-specific (fk_tipo_posto is null, fk_secao matches)
        if secao_id is not None:
            section_specific = param_rows[
                (param_rows['fk_tipo_posto'].isna()) & 
                (param_rows['fk_secao'] == secao_id)
            ]
            if not section_specific.empty:
                value = get_value_from_row(section_specific.iloc[0])
                if value is not None:
                    params_dict[param_name] = value
                    logger.info(f"DEBUG: Found section-specific param {param_name}:{value}")
                    continue
        
        # Priority 3: Unit-specific (fk_tipo_posto and fk_secao are null, fk_unidade matches)
        if unit_id is not None:
            unit_specific = param_rows[
                (param_rows['fk_tipo_posto'].isna()) & 
                (param_rows['fk_secao'].isna()) & 
                (param_rows['fk_unidade'] == unit_id)
            ]
            if not unit_specific.empty:
                value = get_value_from_row(unit_specific.iloc[0])
                if value is not None:
                    params_dict[param_name] = value
                    logger.info(f"DEBUG: Found unit-specific param {param_name}:{value}")
                    continue
        
        # Priority 4: Default (all FKs are null)
        default_rows = param_rows[
            (param_rows['fk_tipo_posto'].isna()) & 
            (param_rows['fk_secao'].isna()) & 
            (param_rows['fk_unidade'].isna()) &
            (param_rows['fk_grupo'].isna())
        ]
        if not default_rows.empty:
            value = get_value_from_row(default_rows.iloc[0])
            if value is not None:
                params_dict[param_name] = value
                logger.info(f"DEBUG: Found default param {param_name}:{value}")
    
    return params_dict

def get_value_from_row(row):
    """Get the actual value from CHARVALUE, NUMBERVALUE, or DATEVALUE"""
    # TODO: Check this logic
    if pd.notna(row['charvalue']):
        return row['charvalue']
    elif pd.notna(row['numbervalue']):
        return row['numbervalue']
    elif pd.notna(row['datevalue']):
        return row['datevalue']
    return None

def convert_types_out(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert HORARIO values to sched_type and sched_subtype columns
    
    Args:
        df (pd.DataFrame): Input dataframe with HORARIO column
    
    Returns:
        pd.DataFrame: DataFrame with added sched_type and sched_subtype columns
    """
    
    # Create a copy to avoid modifying the original dataframe
    df = pd.DataFrame(df).copy()
    
    # Define mapping for SCHED_TYPE
    sched_type_mapping = {
        'M': 'T',
        'T': 'T', 
        'MoT': 'T',
        'ToM': 'T',
        'P': 'T',
        'L': 'F',
        'LD': 'F',
        'LQ': 'F',
        'C': 'F',
        'F': 'R',
        '-': 'N',
        'V': 'T',
        'A': 'T',
        'DFS': 'T'
    }
    
    # Define mapping for sched_subtype
    sched_subtype_mapping = {
        'M': 'M',
        'T': 'T',
        'MoT': 'H',
        'ToM': 'H', 
        'P': 'P',
        'L': '',
        'LD': 'D',
        'LQ': 'Q',
        'C': 'C',
        'F': '',
        '-': '',
        'V': 'A',
        'A': 'A',
        'DFS': 'C'
    }
    
    # Apply mappings
    df['sched_type'] = df['horario'].map(sched_type_mapping)
    df['sched_subtype'] = df['horario'].map(sched_subtype_mapping)
    
    return df


def bulk_insert_with_query(data_manager: DBDataManager,
                          data: pd.DataFrame,
                          query_file: str,
                          **kwargs) -> bool:
    """
    Execute bulk insert using a parameterized query file with connection handling.
    """
    
    # Validate inputs
    if not hasattr(data_manager, 'session') or data_manager.session is None:
        logger.error("No database session available")
        return False
   
    if not os.path.exists(query_file):
        logger.error(f"Query file not found: {query_file}")
        return False
   
    if data.empty:
        logger.warning("Empty DataFrame provided, no records to insert")
        return True
 
    # Simple retry loop for connection issues
    max_retries = 2
   
    for attempt in range(max_retries + 1):
        try:
            from sqlalchemy import text
           
            # If this is a retry attempt, recreate the session completely
            if attempt > 0:
                logger.info(f"Recreating session for retry attempt {attempt + 1}")
                try:
                    # Close the old session
                    data_manager.session.close()
                   
                    # Create a completely new session
                    from sqlalchemy.orm import sessionmaker
                    Session = sessionmaker(bind=data_manager.engine)
                    data_manager.session = Session()
                   
                    # Test the new session
                    data_manager.session.execute(text("SELECT 1 FROM DUAL")).fetchone()
                    logger.info("New session created and tested successfully")
                   
                except Exception as recreate_error:
                    logger.error(f"Failed to recreate session: {recreate_error}")
                    if attempt == max_retries:
                        return False
                    continue
           
            # Read the insert query
            with open(query_file, 'r', encoding='utf-8') as f:
                insert_query = f.read().strip()
           
            if not insert_query:
                logger.error(f"Query file is empty: {query_file}")
                return False
           
            logger.info(f"Executing bulk insert of {len(data)} rows (attempt {attempt + 1})")
           
            # Convert DataFrame to list of dictionaries for parameter binding
            records = data.to_dict('records')
           
            # Execute the insert for each record
            for i, record in enumerate(records):
                try:
                    # Merge additional kwargs with record data
                    params = {**kwargs, **record}
                    # Remove pathOS from params if it exists (it's for connection handling only)
                    params.pop('pathOS', None)
                   
                    data_manager.session.execute(text(insert_query), params)
                   
                    # Log progress for large datasets
                    if (i + 1) % 1000 == 0:
                        logger.info(f"Processed {i + 1}/{len(records)} records")
                       
                except Exception as record_error:
                    logger.error(f"Error inserting record {i + 1}: {str(record_error)}")
                    logger.debug(f"Failed record data: {record}")
                    raise
           
            # Commit all inserts
            data_manager.session.commit()
           
            logger.info(f"Successfully inserted {len(records)} records")
            return True
           
        except Exception as e:
            error_str = str(e).lower()
           
            # Check if this is a connection-related error
            connection_errors = [
                'not connected', 'dpi-1010', 'connection', 'timeout', 'closed',
                'broken', 'lost', 'ora-12170', 'ora-03135', 'ora-00028', 'ora-02391'
            ]
           
            is_connection_error = any(err_keyword in error_str for err_keyword in connection_errors)
           
            if is_connection_error and attempt < max_retries:
                logger.warning(f"Connection error detected (attempt {attempt + 1}): {e}")
               
                # Rollback current transaction
                try:
                    data_manager.session.rollback()
                except Exception as rollback_error:
                    logger.debug(f"Rollback failed (expected): {rollback_error}")
               
                logger.info(f"Will retry with new session (attempt {attempt + 2})")
                continue
            else:
                # Not a connection error or max retries reached
                logger.error(f"Bulk insert failed: {e}")
               
                # Rollback on error
                try:
                    data_manager.session.rollback()
                except:
                    pass
               
                return False
   
    # Should not reach here
    logger.error(f"Bulk insert failed after {max_retries + 1} attempts")
    return False

def _create_empty_results(algo_name: str, process_id: int, start_date: str, end_date: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Create empty results structure when no data is available."""
    return {
        'core_results': {
            'schedule': pd.DataFrame(),
            'formatted_schedule': pd.DataFrame(),
            'wide_format_schedule': pd.DataFrame(),
            'status': 'UNKNOWN'
        },
        'metadata': {
            'algorithm_name': algo_name,
            'algorithm_version': '1.0',
            'execution_timestamp': datetime.now().isoformat(),
            'process_id': process_id,
            'date_range': {
                'start_date': start_date,
                'end_date': end_date,
                'total_days': 0
            },
            'parameters_used': parameters,
            'solver_info': {
                'solver_name': 'CP-SAT',
                'solving_time_seconds': None,
                'num_branches': None,
                'num_conflicts': None
            }
        },
        'scheduling_stats': {},
        'constraint_validation': {},
        'quality_metrics': {},
        'validation': {'is_valid_solution': False, 'validation_errors': ['No schedule data available']},
        'export_info': {},
        'summary': {
            'status': 'failed',
            'message': 'No schedule data available',
            'key_metrics': {}
        }
    }


def _calculate_comprehensive_stats(algorithm_results: pd.DataFrame, start_date: str, end_date: str, data_processed: Dict[str, Any] = None) -> Dict[str, Any]:
    """Calculate comprehensive statistics from algorithm results in wide format."""
    try:
        # Basic counts
        total_workers = len(algorithm_results) if not algorithm_results.empty else 0
        
        # Get day columns (all columns except 'Worker')
        day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]
        total_days = len(day_columns)
        
        # Calculate date range
        if start_date and end_date:
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            total_days = len(date_range)
        
        # Shift distribution - flatten all shift values
        shift_distribution = {}
        unassigned_slots = 0
        total_assignments = 0
        
        if not algorithm_results.empty and day_columns:
            # Get all shift values from the wide format
            all_shifts = []
            for col in day_columns:
                all_shifts.extend(algorithm_results[col].dropna().astype(str).tolist())
            
            # Count shift types
            shift_series = pd.Series(all_shifts)
            shift_distribution = shift_series.value_counts().to_dict()
            total_assignments = len(all_shifts)
            unassigned_slots = shift_distribution.get('N', 0) + shift_distribution.get('ERROR', 0) + shift_distribution.get('-', 0)
        
        # Worker statistics
        workers_scheduled = total_workers
        worker_list = algorithm_results['Worker'].astype(str).tolist() if 'Worker' in algorithm_results.columns else []
        
        # Time coverage
        scheduled_days = total_days
        coverage_percentage = 100.0 if total_days > 0 else 0
        
        # Working days and special days coverage
        working_days_covered = 0
        special_days_covered = 0
        
        if data_processed:
            working_days = data_processed.get('working_days', [])
            special_days = data_processed.get('special_days', [])
            working_days_covered = len(working_days)
            special_days_covered = len(special_days)
        
        return {
            'workers': {
                'total_workers': total_workers,
                'workers_scheduled': workers_scheduled,
                'worker_list': worker_list
            },
            'shifts': {
                'shift_distribution': shift_distribution,
                'total_assignments': total_assignments,
                'shift_types_used': list(shift_distribution.keys()),
                'unassigned_slots': unassigned_slots
            },
            'time_coverage': {
                'total_days': total_days,
                'working_days_covered': working_days_covered,
                'special_days_covered': special_days_covered,
                'coverage_percentage': coverage_percentage
            }
        }
    except Exception as e:
        logger.error(f"Error calculating comprehensive stats: {e}")
        return {}

def _validate_constraints(algorithm_results: pd.DataFrame) -> Dict[str, Any]:
    """Validate constraint satisfaction from wide format."""
    try:
        constraint_validation = {
            'working_days': {
                'violations': [],
                'satisfied': True,
                'details': 'All workers have proper working day assignments'
            },
            'continuous_working_days': {
                'violations': [],
                'max_continuous_exceeded': [],
                'satisfied': True
            },
            'salsa_specific': {
                'consecutive_free_days': {'satisfied': True, 'violations': []},
                'quality_weekends': {'satisfied': True, 'violations': []},
                'saturday_L_constraint': {'satisfied': True, 'violations': []}
            },
            'overall_satisfaction': 100
        }
        
        if algorithm_results.empty:
            constraint_validation['working_days']['satisfied'] = False
            constraint_validation['working_days']['violations'].append('No schedule data available')
            constraint_validation['overall_satisfaction'] = 0
            return constraint_validation
        
        # Get day columns
        day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]
        
        # Check for continuous working days violations
        continuous_violations = []
        max_continuous_work = 5  # Assume max 5 consecutive working days
        
        for idx, row in algorithm_results.iterrows():
            worker = row['Worker']
            worker_shifts = [str(row[col]) for col in day_columns if pd.notna(row[col])]
            
            consecutive_work = 0
            max_consecutive = 0
            
            for shift in worker_shifts:
                if shift in ['M', 'T']:  # Working shifts
                    consecutive_work += 1
                    max_consecutive = max(max_consecutive, consecutive_work)
                else:
                    consecutive_work = 0
            
            if max_consecutive > max_continuous_work:
                continuous_violations.append(f"Worker {worker}: {max_consecutive} consecutive working days")
        
        if continuous_violations:
            constraint_validation['continuous_working_days']['satisfied'] = False
            constraint_validation['continuous_working_days']['violations'] = continuous_violations
            constraint_validation['overall_satisfaction'] -= 20
        
        # Check SALSA-specific constraints
        weekend_violations = []
        for idx, row in algorithm_results.iterrows():
            worker = row['Worker']
            worker_shifts = [str(row[col]) for col in day_columns if pd.notna(row[col])]
            
            # Check for quality weekends (LQ should be followed by proper rest)
            lq_count = worker_shifts.count('LQ')
            if lq_count == 0:
                weekend_violations.append(f"Worker {worker}: No quality weekends assigned")
        
        if weekend_violations:
            constraint_validation['salsa_specific']['quality_weekends']['satisfied'] = False
            constraint_validation['salsa_specific']['quality_weekends']['violations'] = weekend_violations
            constraint_validation['overall_satisfaction'] -= 10
        
        return constraint_validation
    except Exception as e:
        logger.error(f"Error validating constraints: {e}")
        return {}
    
def _calculate_quality_metrics(algorithm_results: pd.DataFrame) -> Dict[str, Any]:
    """Calculate quality metrics for the solution from wide format."""
    try:
        # Calculate basic metrics
        total_workers = len(algorithm_results) if not algorithm_results.empty else 0
        
        # Get day columns
        day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]

        
        # SALSA-specific metrics
        two_day_quality_weekends = 0
        consecutive_free_days_achieved = 0
        saturday_L_assignments = 0
        
        if not algorithm_results.empty and day_columns:
            # Count LQ (two-day quality weekends)
            for col in day_columns:
                two_day_quality_weekends += (algorithm_results[col] == 'LQ').sum()
            
            # Count L assignments (including Saturday L)
            for col in day_columns:
                saturday_L_assignments += (algorithm_results[col] == 'L').sum()
            
            # Count consecutive free days (simplified - count sequences of L, LQ, F)
            for idx, row in algorithm_results.iterrows():
                worker_shifts = [str(row[col]) for col in day_columns if pd.notna(row[col])]
                consecutive_count = 0
                max_consecutive = 0
                
                for shift in worker_shifts:
                    if shift in ['L', 'LQ']:
                        consecutive_count += 1
                        max_consecutive = max(max_consecutive, consecutive_count)
                    else:
                        consecutive_count = 0
                
                if max_consecutive >= 2:
                    consecutive_free_days_achieved += 1
        
        
        return {
            'salsa_specific_metrics': {
                'two_day_quality_weekends': two_day_quality_weekends,
                'consecutive_free_days_achieved': consecutive_free_days_achieved,
                'saturday_L_assignments': saturday_L_assignments
            }
        }
    except Exception as e:
        logger.error(f"Error calculating quality metrics: {e}")
        return {}
    
def _format_schedules(algorithm_results: pd.DataFrame, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
    """Format schedule for different output types from wide format."""
    try:
        formatted_schedules = {
            'database_format': pd.DataFrame(),
            'wide_format': pd.DataFrame()
        }
        
        if algorithm_results.empty:
            return formatted_schedules
        
        # Wide format is already available (the input format)
        formatted_schedules['wide_format'] = algorithm_results.copy()
        
        # Database format (long format) - convert from wide to long
        if 'Worker' in algorithm_results.columns:
            day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]
            
            if day_columns:
                # Melt the DataFrame to long format
                melted_df = pd.melt(
                    algorithm_results,
                    id_vars=['Worker'],
                    value_vars=day_columns,
                    var_name='Day',
                    value_name='Shift'
                )
                
                # Clean up the Day column to extract day numbers
                logger.info(f"DEBUG: melted_df['Day'] type: {type(melted_df['Day'])}")
                logger.info(f"DEBUG: melted_df['Day'].str.replace('Day ', ''): {melted_df['Day'].str.replace('Day ', '')}")
                melted_df['Day'] = melted_df['Day'].str.replace('Day_', '').astype(int)
                
                # Convert day numbers to actual dates if start_date is available
                if start_date:
                    try:
                        base_date = pd.to_datetime(start_date)
                        melted_df['Date'] = melted_df['Day'].apply(lambda x: base_date + pd.Timedelta(days=x-1))
                        melted_df['Date'] = melted_df['Date'].dt.strftime('%Y-%m-%d')
                    except:
                        melted_df['Date'] = melted_df['Day'].astype(str)
                else:
                    melted_df['Date'] = melted_df['Day'].astype(str)
                
                # Rename columns and select relevant ones
                formatted_schedules['database_format'] = melted_df[['Worker', 'Date', 'Shift']].copy()
                formatted_schedules['database_format'].rename(columns={'Worker': 'colaborador'}, inplace=True)
                formatted_schedules['database_format'].rename(columns={'Shift': 'horario'}, inplace=True)
                formatted_schedules['database_format'].rename(columns={'Date': 'data'}, inplace=True)

        return formatted_schedules
    except Exception as e:
        logger.error(f"Error formatting schedules: {e}", exc_info=True)
        return {'database_format': pd.DataFrame(), 'wide_format': pd.DataFrame()}

def _create_metadata(algo_name: str, process_id: int, start_date: str, end_date: str, parameters: Dict[str, Any], stats: Dict[str, Any], solver_attributes: Dict[str, Any]) -> Dict[str, Any]:
    """Create metadata information."""
    return {
        'algorithm_name': algo_name,
        'algorithm_version': '1.0',
        'execution_timestamp': datetime.now().isoformat(),
        'process_id': process_id,
        'date_range': {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': stats.get('time_coverage', {}).get('total_days', 0)
        },
        'parameters_used': parameters,
        'solver_info': {
            'solver_name': 'CP-SAT',
            'solving_time_seconds': solver_attributes.get('solving_time_seconds'),
            'num_branches': solver_attributes.get('num_branches'),
            'num_conflicts': solver_attributes.get('num_conflicts')
        }
    }

def _validate_solution(algorithm_results: pd.DataFrame) -> Dict[str, Any]:
    """Validate the solution and return validation results for wide format."""
    try:
        validation_errors = []
        warnings = []
        recommendations = []
        
        # Check if solution is valid
        if algorithm_results.empty:
            validation_errors.append("No schedule data available")
        else:
            # Check for required columns
            if 'Worker' not in algorithm_results.columns:
                validation_errors.append("Missing 'Worker' column")
            
            # Check for day columns
            day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]
            if not day_columns:
                validation_errors.append("No day columns found")
            
            # Check for unassigned days
            if day_columns:
                unassigned_count = 0
                for col in day_columns:
                    unassigned_count += (algorithm_results[col].isin(['N', 'ERROR', '-'])).sum()
                
                if unassigned_count > 0:
                    warnings.append(f"Found {unassigned_count} unassigned shifts")
                    recommendations.append("Review worker constraints and availability")
                
                # Check for missing data
                missing_count = 0
                for col in day_columns:
                    missing_count += algorithm_results[col].isna().sum()
                
                if missing_count > 0:
                    warnings.append(f"Found {missing_count} missing shift assignments")
                    recommendations.append("Verify data completeness")
        
        is_valid_solution = len(validation_errors) == 0
        
        return {
            'is_valid_solution': is_valid_solution,
            'validation_errors': validation_errors,
            'warnings': warnings,
            'recommendations': recommendations
        }
    except Exception as e:
        logger.error(f"Error validating solution: {e}")
        return {
            'is_valid_solution': False,
            'validation_errors': [f"Validation error: {e}"],
            'warnings': [],
            'recommendations': []
        }
    
def _create_export_info(process_id: int, ROOT_DIR) -> Dict[str, Any]:
    """Create export information."""
    try:
        # Get output filename from ROOT_DIR
        output_filename = os.path.join(ROOT_DIR, 'data', 'output', f'salsa_schedule_{process_id}.xlsx')
        
        return {
            'export_files': {
                'excel_file': output_filename,
                'csv_file': None,
                'json_file': None
            },
            'export_timestamp': datetime.now().isoformat(),
            'export_status': 'completed'
        }
    except Exception as e:
        logger.error(f"Error creating export info: {e}")
        return {}

# Pass this function to the data_treatment_functions
def get_colabs_passado(wfm_proc_colab: str, df_mpd_valid_employees: pd.DataFrame, fk_tipo_posto: str) -> Tuple[bool, List[int], str]:
    """
    Get employees from the past (colabs_passado) for a specific job position type.
    
    Args:
        wfm_proc_colab: The employee ID to exclude from the past employees list
        df_mpd_valid_employees: DataFrame containing valid employees data
        fk_tipo_posto: Job position type filter
        
    Returns:
        Tuple of (success: bool, colabs_passado: List[int], message: str)
    """

    # Validate there is not multiple employees with expected conditions
    try:
        df = df_mpd_valid_employees[df_mpd_valid_employees['fk_tipo_posto'] == fk_tipo_posto]
        mask = (df['gera_horario_ind'] == 'Y') & (df['existe_horario_ind'] == 'N')
        colabs_a_gerar = df[mask]['fk_colaborador'].unique()
        
        logger.info(f"DEBUG: colabs_a_gerar type: {type(colabs_a_gerar)}")
        logger.info(f"DEBUG: wfm_proc_colab type: {type(wfm_proc_colab)}")
        
        wfm_proc_colab = int(wfm_proc_colab)
        
        if len(colabs_a_gerar) == 0:
            return False, [], "No employees found with the expected conditions."
            
        if len(colabs_a_gerar) > 1:
            return False, [], "There should be only one employee for allocation."

        if int(colabs_a_gerar[0]) != wfm_proc_colab:
            return False, [], "The employee is not present in the df_mpd_valid_employees query."

        colabs_passado = [int(x) for x in df['fk_colaborador'].unique()]
        colabs_passado.remove(wfm_proc_colab)
        logger.info(f"Created colabs_passado list: {colabs_passado}")
        return True, colabs_passado, "Success"
    except Exception as e:
        logger.error(f"Error creating colabs_passado list: {e}", exc_info=True)
        return False, [], "Error creating the colabs_passado_list"