"""File containing the functions for loading process data validations."""

# Dependencies
import pandas as pd
from typing import List

# Local stuff
from base_data_project.log_config import setup_logger

logger = setup_logger('algoritmo_GD')

# Validate parameters_cfg
def validate_parameters_cfg(parameters_cfg: str) -> bool:
    """
    Validate parameters_cfg.
    """
    # Define the available values here - shouldnt be here hardcoded but defined by params
    valid_parameters_cfg = ['floor', 'ceil']

    if parameters_cfg not in valid_parameters_cfg:
        return False
    return True

def validate_employees_id_list(employees_id_list: list) -> bool:
    """
    Validate employees_id_list.
    """
    if len(employees_id_list) == 0:
        return False
    return True

def validate_posto_id_list(posto_id_list: list) -> bool:
    """
    Validate posto_id_list.
    """
    if len(posto_id_list) == 0:
        return False
    return True

def validate_posto_id(posto_id: int) -> bool:
    """
    Validate posto_id.
    """
    if posto_id == 0 or posto_id == None:
        return False
    return True

def validate_df_valid_emp(df_valid_emp: pd.DataFrame) -> bool:
    """
    Validate df_valid_emp.
    """
    if df_valid_emp.empty:
        return False
    if len(df_valid_emp) == 0:
        return False
    if 'employee_id' not in df_valid_emp.columns:
        return False
    if 'fk_unidade' not in df_valid_emp.columns:
        return False
    if 'fk_secao' not in df_valid_emp.columns:
        return False
    if 'fk_tipo_posto' not in df_valid_emp.columns:
        return False
    if 'prioridade_folgas' not in df_valid_emp.columns:
        return False
    return True

# Be careful with this one, it may end up being a list of strings
def validate_past_employee_id_list(past_employee_id_list: List[int], case_type: int) -> bool:
    """
    Validate past_employee_id_list.
    """
    if len(past_employee_id_list) == 0 and case_type == 4:
        return False
    return True

def validate_date_passado(date_str: str) -> bool:
    """
    Validate first_day_passado.
    """
    # If empty, return False
    if date_str == '':
        return False

    # If not a valid date, return False
    try:
        date_dt = pd.to_datetime(date_str, format='%Y-%m-%d')
    except Exception as e:
        return False

    # If valid date, return True
    return True

def validate_df_ausencias_ferias(df_ausencias_ferias: pd.DataFrame) -> bool:
    """
    Validate df_ausencias_ferias input structure.
    """
    if df_ausencias_ferias.empty:
        return True  # Empty is valid for ausencias_ferias
    if len(df_ausencias_ferias) == 0:
        return True  # Empty is valid for ausencias_ferias
        
    # Check required columns for processing
    if 'employee_id' not in df_ausencias_ferias.columns:
        return False
    if 'data' in df_ausencias_ferias.columns:
        # Already processed format
        required_columns = ['codigo', 'employee_id', 'matricula', 'data', 'tipo_ausencia', 'fk_motivo_ausencia']
    else:
        # Raw format that needs processing
        required_columns = ['codigo', 'employee_id', 'matricula', 'data_ini', 'data_fim', 'tipo_ausencia', 'fk_motivo_ausencia']
        
    for col in required_columns:
        if col not in df_ausencias_ferias.columns:
            return False

    return True

def validate_df_ciclos_completos(df_ciclos_completos: pd.DataFrame) -> bool:
    """
    Validate df_ciclos_completos structure.
    
    Note: Empty DataFrame is valid - occurs when no employees have 90-day cycles.
    """
    # Empty is valid - means no employees with 90-day cycles
    if df_ciclos_completos.empty:
        return True
    
    # If not empty, validate required columns exist
    required_columns = ['employee_id', 'schedule_day', 'tipo_dia']
    if not all(col in df_ciclos_completos.columns for col in required_columns):
        return False

    return True

def validate_df_folgas_ciclos(df_folgas_ciclos: pd.DataFrame) -> bool:
    """
    Validate df_folgas_ciclos structure.
    
    Note: Empty DataFrame is valid - occurs when all employees have 90-day cycles.
    """
    # Empty is valid - means all employees have 90-day cycles
    if df_folgas_ciclos.empty:
        return True
    
    # If not empty, validate required columns exist
    required_columns = ['employee_id', 'schedule_day', 'tipo_dia']
    if not all(col in df_folgas_ciclos.columns for col in required_columns):
        return False
    
    # Check if the tipo_dia has only "F" (folga/fixed day-off)
    if not df_folgas_ciclos['tipo_dia'].isin(['L']).all():
        return False
        
    return True

def validate_df_colaborador(df_colaborador: pd.DataFrame, employees_id_list: List[str]) -> bool:
    """
    Validate df_colaborador structure and employee IDs consistency.
    
    Args:
        df_colaborador: Employee DataFrame with standardized fields
        employees_id_list: List of valid employee IDs (as strings)
        
    Returns:
        bool: True if validation passes, False otherwise
    """
    if df_colaborador.empty:
        return False

    needed_columns = ['employee_id', 'loja', 'secao', 'fk_tipo_posto', 'convenio']
    if not all(col in df_colaborador.columns for col in needed_columns):
        logger.error(f"df_colaborador columns: {df_colaborador.columns.tolist()}")
        logger.error(f"needed_columns not in df_colaborador columns: {needed_columns}")
        return False
    if not df_colaborador['employee_id'].isin(employees_id_list).all():
        logger.error(f"employee_id not in employees_id_list: {df_colaborador['employee_id'].unique()}, employees_id_list: {employees_id_list}")
        return False
    return True

# OUTPUT VALIDATION FUNCTIONS
# These complement the input validations and check that treatment was applied correctly

def validate_treated_df_valid_emp(df_valid_emp: pd.DataFrame) -> bool:
    """
    Validate df_valid_emp after treatment - check business rules applied correctly.
    """
    error_msgs = []
    if df_valid_emp.empty:
        error_msgs.append("df_valid_emp is empty")
        return False
    if 'prioridade_folgas' not in df_valid_emp.columns:
        error_msgs.append("prioridade_folgas not in df_valid_emp columns")
        return False

    if len(df_valid_emp['fk_unidade'].unique()) > 1:
        error_msgs.append("More than one fk_unidade associated with the process")
        return False
    if len(df_valid_emp['fk_secao'].unique()) > 1:
        error_msgs.append("More than one fk_secao associated with the process")
        return False
    if len(df_valid_emp['fk_tipo_posto'].unique()) == 0:
        error_msgs.append("No fk_tipo_posto associated with the process")
        return False

    # Check that prioridade_folgas values are valid after treatment
    valid_priorities = ['manager', 'keyholder', 'normal', '']
    if not df_valid_emp['prioridade_folgas'].isin(valid_priorities).all():
        return False
        
    return True

def validate_treated_df_ausencias_ferias(df_ausencias_ferias: pd.DataFrame) -> bool:
    """
    Validate df_ausencias_ferias after treatment.
    """
    if df_ausencias_ferias.empty:
        return True  # Empty is valid after treatment
        
    # Check required columns after treatment
    required_columns = ['employee_id', 'data', 'tipo_ausencia']
    for col in required_columns:
        if col not in df_ausencias_ferias.columns:
            return False
            
    # Check that date intervals were properly expanded
    if 'data_ini' in df_ausencias_ferias.columns or 'data_fim' in df_ausencias_ferias.columns:
        return False  # These should be removed after treatment
        
    return True

def validate_treated_df_calendario(df_calendario: pd.DataFrame) -> bool:
    """
    Validate df_calendario after creation/treatment.
    """
    if df_calendario.empty:
        return False
        
    # Check required columns
    required_columns = ['employee_id', 'schedule_day', 'tipo_turno', 'horario', 'wd', 'dia_tipo', 'matricula']
    for col in required_columns:
        if col not in df_calendario.columns:
            return False
            
    # Check that tipo_turno values are valid
    valid_turnos = ['M', 'T']
    if not df_calendario['tipo_turno'].isin(valid_turnos).all():
        return False
        
    # Check that wd values are valid (1-7)
    if not df_calendario['wd'].between(1, 7).all():
        return False
        
    return True

def validate_valid_emp_info(unit_id: int, secao_id: int, posto_id_list: List[int], employees_id_list: List[int]) -> bool:
    """
    Validate valid_emp info.
    """
    error_msgs = []
    if unit_id == 0 or unit_id == None:
        error_msgs.append("unit_id is 0 or None")
        return False
    if secao_id == 0 or secao_id == None:
        error_msgs.append("secao_id is 0 or None")
        return False
    if len(posto_id_list) == 0:
        error_msgs.append("posto_id_list is empty")
        return False
    if len(employees_id_list) == 0:
        error_msgs.append("employees_id_list is empty")
        return False
    return True

def validate_df_feriados(df_feriados: pd.DataFrame) -> bool:
    """
    Validate df_feriados.
    """
    if df_feriados.empty:
        return False
    if 'schedule_day' not in df_feriados.columns:
        return False
    if 'tipo_feriado' not in df_feriados.columns:
        return False
    if 'fk_unidade' not in df_feriados.columns:
        return False
    if 'fk_cidade' not in df_feriados.columns:
        return False
    if 'fk_estado' not in df_feriados.columns:
        return False
    if 'fk_pais' not in df_feriados.columns:
        return False
    return True

def validate_num_sundays_year(num_sundays_year: int) -> bool:
    """
    Validate num_sundays_year.
    """
    if not isinstance(num_sundays_year, int) or num_sundays_year < 0:
        return False
    return True