"""
Validation functions for func_inicializa - ensuring data integrity before solver execution.

These validations replace the old implicit checks from metadata rows (like 'Dia', 'TURNO', 
'maxTurno', etc.) with explicit structure and content validations.

Functions:
- validate_df_calendario_structure: Validates calendario (schedule) dataframe
- validate_df_estimativas_structure: Validates estimativas (workload) dataframe  
- validate_df_colaborador_structure: Validates colaborador (employee) dataframe
"""

# Dependencies
import pandas as pd
import numpy as np
from typing import Tuple, List
from datetime import datetime

# Local stuff
from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config

# Get configuration singleton
_config = get_config()
PROJECT_NAME = _config.project_name

# Set up logger
logger = get_logger(PROJECT_NAME)


def validate_df_calendario_structure(df_calendario: pd.DataFrame, start_date: str = None, end_date: str = None) -> Tuple[bool, str]:
    """
    Validate df_calendario structure and content before func_inicializa cross-dataframe operations.
    
    Replaces old metadata row checks:
    - Old: Looked for 'Dia', 'TURNO' rows
    - New: Validates actual data structure and completeness
    
    Args:
        df_calendario: Calendar/schedule dataframe
        start_date: Optional start date for date range validation (YYYY-MM-DD)
        end_date: Optional end date for date range validation (YYYY-MM-DD)
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    try:
        # BASIC STRUCTURE VALIDATION
        if df_calendario is None:
            return False, "df_calendario is None"
        
        if df_calendario.empty:
            return False, "df_calendario is empty"
        
        if len(df_calendario) == 0:
            return False, "df_calendario has 0 rows"
        
        # REQUIRED COLUMNS CHECK
        required_columns = [
            'COLABORADOR',  # Employee identifier (matricula)
            'DATA',         # Date
            'TIPO_TURNO',   # Shift type (M, T, MoT, P, etc.)
            'HORARIO',      # Work status (H, L, F, V, etc.)
        ]
        
        missing_columns = [col for col in required_columns if col not in df_calendario.columns]
        if missing_columns:
            return False, f"df_calendario missing required columns: {missing_columns}"
        
        # DATA TYPE VALIDATION
        # Check DATA column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df_calendario['DATA']):
            return False, "df_calendario['DATA'] column is not datetime type"
        
        # Check for null values in critical columns
        null_checks = {
            'COLABORADOR': df_calendario['COLABORADOR'].isnull().sum(),
            'DATA': df_calendario['DATA'].isnull().sum(),
            'TIPO_TURNO': df_calendario['TIPO_TURNO'].isnull().sum(),
        }
        
        null_columns = [col for col, null_count in null_checks.items() if null_count > 0]
        if null_columns:
            null_details = ', '.join([f"{col}: {null_checks[col]} nulls" for col in null_columns])
            logger.warning(f"df_calendario has null values in critical columns: {null_details}")
            # Don't fail, just warn - nulls might be valid in some cases
        
        # CONTENT VALIDATION
        # Check TIPO_TURNO values are valid
        valid_tipo_turno = ['M', 'T', 'MoT', 'P', 'F', 'L', 'V', 'A', 'DFS', 'OUT', 'NL', '-']
        invalid_tipos = df_calendario[~df_calendario['TIPO_TURNO'].isin(valid_tipo_turno)]['TIPO_TURNO'].unique()
        if len(invalid_tipos) > 0:
            logger.warning(f"df_calendario contains unexpected TIPO_TURNO values: {invalid_tipos}")
            # Don't fail - might be new valid types
        
        # Check HORARIO values are valid (if column exists)
        if 'HORARIO' in df_calendario.columns:
            valid_horario = ['H', 'L', 'L_', 'L_DOM', 'F', 'V', 'NL', 'A', 'DFS', 'OUT', 'NL2D', 'NL3D']
            invalid_horarios = df_calendario[~df_calendario['HORARIO'].isin(valid_horario)]['HORARIO'].unique()
            if len(invalid_horarios) > 0:
                logger.warning(f"df_calendario contains unexpected HORARIO values: {invalid_horarios}")
        
        # DATE RANGE VALIDATION (if provided)
        if start_date and end_date:
            try:
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                
                min_date = df_calendario['DATA'].min()
                max_date = df_calendario['DATA'].max()
                
                if min_date > start_dt:
                    logger.warning(f"df_calendario min date ({min_date}) is after start_date ({start_dt})")
                
                if max_date < end_dt:
                    logger.warning(f"df_calendario max date ({max_date}) is before end_date ({end_dt})")
                
            except Exception as e:
                logger.warning(f"Could not validate date range: {e}")
        
        # EMPLOYEE COUNT CHECK
        unique_employees = df_calendario['COLABORADOR'].nunique()
        if unique_employees == 0:
            return False, "df_calendario has 0 unique employees"
        
        logger.info(f"df_calendario validation passed: {len(df_calendario)} rows, {unique_employees} employees")
        return True, ""
        
    except Exception as e:
        error_msg = f"Error validating df_calendario: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg


def validate_df_estimativas_structure(df_estimativas: pd.DataFrame, start_date: str = None, end_date: str = None) -> Tuple[bool, str]:
    """
    Validate df_estimativas structure and content before func_inicializa cross-dataframe operations.
    
    Replaces old metadata row checks:
    - Old: Looked for 'maxTurno', 'minTurno', 'mediaTurno', 'sdTurno' rows
    - New: Validates actual data structure and value ranges
    
    Args:
        df_estimativas: Workload estimates dataframe
        start_date: Optional start date for date range validation (YYYY-MM-DD)
        end_date: Optional end date for date range validation (YYYY-MM-DD)
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    try:
        # BASIC STRUCTURE VALIDATION
        if df_estimativas is None:
            return False, "df_estimativas is None"
        
        if df_estimativas.empty:
            return False, "df_estimativas is empty"
        
        if len(df_estimativas) == 0:
            return False, "df_estimativas has 0 rows"
        
        # REQUIRED COLUMNS CHECK
        required_columns = [
            'data',         # Date
            'turno',        # Shift type (M/T)
            'min_turno',    # Minimum staffing
            'max_turno',    # Maximum staffing
            'media_turno',  # Average staffing
            'sd_turno',     # Standard deviation
        ]
        
        missing_columns = [col for col in required_columns if col not in df_estimativas.columns]
        if missing_columns:
            return False, f"df_estimativas missing required columns: {missing_columns}"
        
        # DATA TYPE VALIDATION
        # Check data column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df_estimativas['data']):
            # Try to convert
            try:
                df_estimativas['data'] = pd.to_datetime(df_estimativas['data'])
            except:
                return False, "df_estimativas['data'] column is not datetime type and cannot be converted"
        
        # Check numeric columns
        numeric_columns = ['min_turno', 'max_turno', 'media_turno', 'sd_turno']
        for col in numeric_columns:
            if not pd.api.types.is_numeric_dtype(df_estimativas[col]):
                return False, f"df_estimativas['{col}'] is not numeric type"
        
        # CONTENT VALIDATION
        # Check turno values are valid
        valid_turnos = ['M', 'T', 'm', 't']
        invalid_turnos = df_estimativas[~df_estimativas['turno'].isin(valid_turnos)]['turno'].unique()
        if len(invalid_turnos) > 0:
            return False, f"df_estimativas contains invalid turno values: {invalid_turnos}"
        
        # Check for negative values (shouldn't happen)
        for col in numeric_columns:
            negative_count = (df_estimativas[col] < 0).sum()
            if negative_count > 0:
                logger.warning(f"df_estimativas['{col}'] has {negative_count} negative values")
        
        # Check min_turno <= max_turno
        invalid_ranges = df_estimativas[df_estimativas['min_turno'] > df_estimativas['max_turno']]
        if len(invalid_ranges) > 0:
            logger.warning(f"df_estimativas has {len(invalid_ranges)} rows where min_turno > max_turno")
        
        # Check for all zeros (might indicate missing data)
        all_zeros = df_estimativas[
            (df_estimativas['min_turno'] == 0) & 
            (df_estimativas['max_turno'] == 0) & 
            (df_estimativas['media_turno'] == 0)
        ]
        if len(all_zeros) > 0:
            logger.warning(f"df_estimativas has {len(all_zeros)} rows with all zeros (might indicate missing forecasts)")
        
        # DATE RANGE VALIDATION (if provided)
        if start_date and end_date:
            try:
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                
                min_date = df_estimativas['data'].min()
                max_date = df_estimativas['data'].max()
                
                if min_date > start_dt:
                    logger.warning(f"df_estimativas min date ({min_date}) is after start_date ({start_dt})")
                
                if max_date < end_dt:
                    logger.warning(f"df_estimativas max date ({max_date}) is before end_date ({end_dt})")
                
            except Exception as e:
                logger.warning(f"Could not validate date range: {e}")
        
        # SHIFT COVERAGE CHECK
        unique_dates = df_estimativas['data'].nunique()
        expected_rows = unique_dates * 2  # Should have M and T for each date
        actual_rows = len(df_estimativas)
        
        if actual_rows < expected_rows:
            logger.warning(f"df_estimativas might be missing some shifts: {actual_rows} rows for {unique_dates} dates (expected ~{expected_rows})")
        
        logger.info(f"df_estimativas validation passed: {len(df_estimativas)} rows, {unique_dates} dates")
        return True, ""
        
    except Exception as e:
        error_msg = f"Error validating df_estimativas: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg


def validate_df_colaborador_structure(df_colaborador: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate df_colaborador structure and content before func_inicializa cross-dataframe operations.
    
    Validates employee data is complete and consistent for the allocation cycle.
    
    Args:
        df_colaborador: Employee/collaborator dataframe
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    try:
        # BASIC STRUCTURE VALIDATION
        if df_colaborador is None:
            return False, "df_colaborador is None"
        
        if df_colaborador.empty:
            return False, "df_colaborador is empty"
        
        if len(df_colaborador) == 0:
            return False, "df_colaborador has 0 rows"
        
        # REQUIRED COLUMNS CHECK
        required_columns = [
            'matricula',        # Employee ID
            'tipo_contrato',    # Contract type (2, 3, 4, 5, 6)
            'l_total',          # Total days off owed
        ]
        
        missing_columns = [col for col in required_columns if col not in df_colaborador.columns]
        if missing_columns:
            return False, f"df_colaborador missing required columns: {missing_columns}"
        
        # RECOMMENDED COLUMNS CHECK (warn if missing)
        recommended_columns = [
            'l_dom',            # Sunday/holiday days off
            'ld', 'lq',         # Specific day off types
            'c2d', 'c3d', 'cxx',  # Consecutive day patterns
            'ciclo',            # Work cycle
            'data_admissao',    # Admission date
            'data_demissao',    # Dismissal date (if applicable)
        ]
        
        missing_recommended = [col for col in recommended_columns if col not in df_colaborador.columns]
        if missing_recommended:
            logger.warning(f"df_colaborador missing recommended columns: {missing_recommended}")
        
        # DATA TYPE VALIDATION
        # Check matricula is not null
        null_matriculas = df_colaborador['matricula'].isnull().sum()
        if null_matriculas > 0:
            return False, f"df_colaborador has {null_matriculas} rows with null matricula"
        
        # Check for duplicate matriculas
        duplicates = df_colaborador['matricula'].duplicated().sum()
        if duplicates > 0:
            logger.warning(f"df_colaborador has {duplicates} duplicate matriculas")
        
        # CONTENT VALIDATION
        # Check tipo_contrato values are valid
        valid_tipos_contrato = [2, 3, 4, 5, 6]
        invalid_tipos = df_colaborador[~df_colaborador['tipo_contrato'].isin(valid_tipos_contrato)]['tipo_contrato'].unique()
        if len(invalid_tipos) > 0:
            logger.warning(f"df_colaborador contains unexpected tipo_contrato values: {invalid_tipos}")
        
        # Check l_total values (should be >= 0)
        if 'l_total' in df_colaborador.columns:
            negative_l_total = (df_colaborador['l_total'] < 0).sum()
            if negative_l_total > 0:
                logger.warning(f"df_colaborador has {negative_l_total} employees with negative l_total")
        
        # Check date columns if they exist
        date_columns = ['data_admissao', 'data_demissao']
        for col in date_columns:
            if col in df_colaborador.columns:
                if not pd.api.types.is_datetime64_any_dtype(df_colaborador[col]):
                    logger.warning(f"df_colaborador['{col}'] is not datetime type")
        
        # CONTRACT TYPE DISTRIBUTION
        if 'tipo_contrato' in df_colaborador.columns:
            tipo_contrato_dist = df_colaborador['tipo_contrato'].value_counts().to_dict()
            logger.info(f"Contract type distribution: {tipo_contrato_dist}")
        
        # EMPLOYEE COUNT
        unique_employees = df_colaborador['matricula'].nunique()
        logger.info(f"df_colaborador validation passed: {len(df_colaborador)} rows, {unique_employees} unique employees")
        
        return True, ""
        
    except Exception as e:
        error_msg = f"Error validating df_colaborador: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg


def validate_all_core_dataframes(
    df_calendario: pd.DataFrame,
    df_estimativas: pd.DataFrame, 
    df_colaborador: pd.DataFrame,
    start_date: str = None,
    end_date: str = None
) -> Tuple[bool, str]:
    """
    Convenience function to validate all three core dataframes at once.
    
    Args:
        df_calendario: Calendar/schedule dataframe
        df_estimativas: Workload estimates dataframe
        df_colaborador: Employee/collaborator dataframe
        start_date: Optional start date for validation (YYYY-MM-DD)
        end_date: Optional end date for validation (YYYY-MM-DD)
        
    Returns:
        Tuple[bool, str]: (all_valid, error_message)
    """
    logger.info("Starting validation of all core dataframes")
    
    # Validate df_calendario
    valid, error = validate_df_calendario_structure(df_calendario, start_date, end_date)
    if not valid:
        return False, f"df_calendario validation failed: {error}"
    
    # Validate df_estimativas
    valid, error = validate_df_estimativas_structure(df_estimativas, start_date, end_date)
    if not valid:
        return False, f"df_estimativas validation failed: {error}"
    
    # Validate df_colaborador
    valid, error = validate_df_colaborador_structure(df_colaborador)
    if not valid:
        return False, f"df_colaborador validation failed: {error}"
    
    logger.info("All core dataframes validation passed ✓")
    return True, ""

