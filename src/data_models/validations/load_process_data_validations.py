"""File containing the functions for loading process data validations."""

# Dependencies
import pandas as pd

# Local stuff

# Validate parameters_cfg
def validate_parameters_cfg(parameters_cfg: str) -> bool:
    """
    Validate parameters_cfg.
    """
    # Define the available values here
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
    if len(df_valid_emp.columns) == 0:
        return False
    if len(df_valid_emp.columns) != 10:
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
