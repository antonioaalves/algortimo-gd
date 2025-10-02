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
    if 'fk_colaborador' not in df_valid_emp.columns:
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

def validate_df_core_pro_emp_horario_det(df_core_pro_emp_horario_det):
    """
    Validate df_core_pro_emp_horario_det structure.
    """
    if df_core_pro_emp_horario_det.empty:
        return False
    if 'employee_id' not in df_core_pro_emp_horario_det.columns:
        return False
    if 'schedule_day' not in df_core_pro_emp_horario_det.columns:
        return False
    if 'tipo_dia' not in df_core_pro_emp_horario_det.columns:
        return False

    return True

def validate_df_colaborador(df_colaborador: pd.DataFrame, employees_id_list: List[int]) -> bool:
    """
    Validate df_colaborador.
    """
    if df_colaborador.empty:
        return False

    needed_columns = ['codigo', 'loja', 'secao', 'fk_tipo_posto', 'convenio']
    if not all(col in df_colaborador.columns for col in needed_columns):
        return False
    if not df_colaborador['codigo'].isin(employees_id_list).all():
        return False
    return True

# OUTPUT VALIDATION FUNCTIONS
# These complement the input validations and check that treatment was applied correctly

def validate_treated_df_valid_emp(df_valid_emp: pd.DataFrame) -> bool:
    """
    Validate df_valid_emp after treatment - check business rules applied correctly.
    """
    if df_valid_emp.empty:
        return False
    if 'prioridade_folgas' not in df_valid_emp.columns:
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
    required_columns = ['employee_id', 'data', 'tipo_turno', 'horario', 'wday', 'dia_tipo', 'matricula']
    for col in required_columns:
        if col not in df_calendario.columns:
            return False
            
    # Check that tipo_turno values are valid
    valid_turnos = ['M', 'T']
    if not df_calendario['tipo_turno'].isin(valid_turnos).all():
        return False
        
    # Check that wday values are valid (1-7)
    if not df_calendario['wday'].between(1, 7).all():
        return False
        
    return True