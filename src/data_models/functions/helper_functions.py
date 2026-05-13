"""File containing the helper functions for the DescansosDataModel"""

# Dependencies
import os
import numpy as np
import pandas as pd
import datetime as dt
from typing import Any, List, Optional, Tuple, Dict, Union
from base_data_project.log_config import get_logger
from base_data_project.data_manager.managers import DBDataManager

# Local stuff
from src.configuration_manager.instance import get_config

# Get configuration singleton
_config = get_config()
PROJECT_NAME = _config.project_name

# Set up logger
logger = get_logger(PROJECT_NAME)


def get_granularity_minutes() -> int:
    """
    Get the configured time-slot granularity in minutes.
    
    Uses the system configuration (src/settings/system_settings.py) and enforces that
    the value is a positive integer.
    
    Returns:
        int: Granularity in minutes.
    
    Raises:
        ValueError: If the configured granularity is missing, non-integer or <= 0.
    """
    granularity = getattr(_config.system, "granularity", None)
    try:
        granularity_int = int(granularity)
    except (TypeError, ValueError):
        error_msg = "granularity must be an integer number of minutes"
        logger.error(error_msg)
        raise ValueError(error_msg)

    if granularity_int <= 0:
        error_msg = "granularity must be a positive integer number of minutes"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return granularity_int


def count_dates_per_year(start_date_str: str, end_date_str: str) -> tuple[str, str, int]:
    """
    Convert R count_dates_per_year function to Python.
    Count dates per year in a date range and return the year with most dates.
    
    Args:
        start_date_str: Start date as string (YYYY-MM-DD format)
        end_date_str: End date as string (YYYY-MM-DD format)
        
    Returns:
        Tuple with the first and last day of the year with the most dates and the year with the most dates
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

        # Get the first and last day of the year with the most dates
        first_day_of_year = pd.to_datetime(f"{year_with_most_dates}-01-01").strftime('%Y-%m-%d')
        last_day_of_year = pd.to_datetime(f"{year_with_most_dates}-12-31").strftime('%Y-%m-%d')

        return first_day_of_year, last_day_of_year, int(year_with_most_dates)
        
    except Exception as e:
        logger.error(f"Error in count_dates_per_year: {str(e)}")
        # Return current year as fallback
        return '', '', ''

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
        unit_id: int or str, unit ID  
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
    
    #logger.info(f"DEBUG: Input parameters - posto_id: {posto_id}, secao_id: {secao_id}, unit_id: {unit_id}")
    #logger.info(f"DEBUG: df_filtered initial (after param names filter):\n {df_filtered}")
    
    # Keep all potentially relevant rows based on hierarchy rules
    # Build conditions that match the hierarchy patterns
    
    conditions = []
    
    # 1. Posto-specific: fk_tipo_posto = posto_id (most specific)
    if posto_id is not None:
        posto_condition = (df_filtered['fk_tipo_posto'] == posto_id)
        conditions.append(posto_condition)
        #logger.info(f"DEBUG: Added posto condition for posto_id={posto_id}")
    
    # 2. Section-specific: fk_tipo_posto is null AND fk_secao = secao_id
    if secao_id is not None:
        section_condition = (
            (df_filtered['fk_tipo_posto'].isna()) & 
            (df_filtered['fk_secao'] == secao_id)
        )
        conditions.append(section_condition)
        #logger.info(f"DEBUG: Added section condition for secao_id={secao_id}")
        
        # Debug: check which rows match this condition
        matching_section = df_filtered[section_condition]
        #logger.info(f"DEBUG: Rows matching section condition:\n {matching_section}")
    
    # 3. Unit-specific: fk_tipo_posto is null AND fk_secao is null AND fk_unidade = unit_id
    if unit_id is not None and str(unit_id).strip():
        unit_condition = (
            (df_filtered['fk_tipo_posto'].isna()) & 
            (df_filtered['fk_secao'].isna()) & 
            (df_filtered['fk_unidade'].astype(str) == str(unit_id).strip())
        )
        conditions.append(unit_condition)
        #logger.info(f"DEBUG: Added unit condition for unit_id={unit_id}")
    
    # 4. Default: all main FKs are null (fallback)
    default_condition = (
        (df_filtered['fk_tipo_posto'].isna()) & 
        (df_filtered['fk_secao'].isna()) & 
        (df_filtered['fk_unidade'].isna()) &
        (df_filtered['fk_grupo'].isna())
    )
    conditions.append(default_condition)
    #logger.info(f"DEBUG: Added default condition")
    
    # Combine all conditions with OR
    if conditions:
        final_condition = conditions[0]
        for condition in conditions[1:]:
            final_condition = final_condition | condition
        df_filtered = df_filtered[final_condition]
    
    # Remove duplicates
    df_filtered = df_filtered.drop_duplicates()

    #logger.info(f"DEBUG: df_filtered after hierarchy filtering in helpers.py:\n {df_filtered}")
    
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
                    #logger.info(f"DEBUG: Found posto-specific param {param_name}:{value}")
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
                    #logger.info(f"DEBUG: Found section-specific param {param_name}:{value}")
                    continue
        
        # Priority 3: Unit-specific (fk_tipo_posto and fk_secao are null, fk_unidade matches)
        if unit_id is not None and str(unit_id).strip():
            unit_specific = param_rows[
                (param_rows['fk_tipo_posto'].isna()) & 
                (param_rows['fk_secao'].isna()) & 
                (param_rows['fk_unidade'].astype(str) == str(unit_id).strip())
            ]
            if not unit_specific.empty:
                value = get_value_from_row(unit_specific.iloc[0])
                if value is not None:
                    params_dict[param_name] = value
                    #logger.info(f"DEBUG: Found unit-specific param {param_name}:{value}")
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
                #logger.info(f"DEBUG: Found default param {param_name}:{value}")
    
    return params_dict

def get_value_from_row(row):
    """
    Extract the actual parameter value from configuration row based on data type.
    
    Configuration parameters in the database are stored in type-specific columns.
    This function checks each column in priority order and returns the first
    non-null value found.
    
    Column Priority:
        1. charvalue: String/text parameters
        2. numbervalue: Numeric parameters (integers, floats)
        3. datevalue: Date/datetime parameters
    
    Business Context:
        Used in hierarchical parameter resolution (get_param_for_posto) to extract
        the actual configuration value regardless of its storage type. This enables
        a flexible parameter system that can handle different data types.
    
    Args:
        row: DataFrame row or Series with columns: charvalue, numbervalue, datevalue
        
    Returns:
        The first non-null value found, or None if all columns are null
        Type depends on which column has data (str, int, float, or datetime)
        
    Example:
        >>> row = pd.Series({'charvalue': 'ALCAMPO', 'numbervalue': None, 'datevalue': None})
        >>> get_value_from_row(row)
        'ALCAMPO'
        
        >>> row = pd.Series({'charvalue': None, 'numbervalue': 5, 'datevalue': None})
        >>> get_value_from_row(row)
        5
    """
    # TODO: Check this logic
    if pd.notna(row['charvalue']):
        return row['charvalue']
    elif pd.notna(row['numbervalue']):
        return row['numbervalue']
    elif pd.notna(row['datevalue']):
        return row['datevalue']
    return None

def load_wfm_scheds(df_pre_ger: pd.DataFrame, employees_tot_pad: List[str]) -> Tuple[pd.DataFrame, List[str], pd.DataFrame]:
    """
    Load and process pre-generated schedules from WFM (Workforce Management) system.
    
    This function integrates externally generated schedules (from WFM or other sources)
    into the algorithm's internal format. It converts WFM type codes to algorithm codes,
    counts rest days, and reshapes the data into the expected matrix format.
    
    Processing Steps:
        1. Renames employee column to 'employee_id' for consistency
        2. Converts WFM types to TRADS (algorithm internal codes)
        3. Extracts unique employee list from pre-generated schedules
        4. Fills missing dates and normalizes schedule subtypes
        5. Counts rest days per employee (L, LD, LQ, F, V, -)
        6. Reshapes to matrix format (employees × dates × shifts)
    
    Rest Day Types Counted:
        - L: Regular day off (Libre)
        - LD: Sunday/holiday off (Libre Domingo)
        - LQ: Quality rest (Libre Quincenal)
        - F: Holiday (Feriado)
        - V: Vacation (Vacaciones)
        - -: Not scheduled
    
    Business Context:
        Pre-generated schedules from WFM allow:
        - Integration with external scheduling systems
        - Manual override of algorithm-generated schedules
        - Hybrid scheduling (partial WFM, partial algorithm)
        - Historical schedule preservation
    
    Args:
        df_pre_ger: DataFrame with WFM schedule data (columns: employee_id, schedule_dt, type, subtype)
        employees_tot_pad: List of all employees for padding/validation
        
    Returns:
        Tuple containing:
            - reshaped_final_3 (pd.DataFrame): Matrix format schedule (row 0: dates, row 1: shifts M/T, rows 2+: employees)
            - emp_pre_ger (List[str]): List of employee IDs with pre-generated schedules
            - df_count (pd.DataFrame): Day-off count per employee (columns: employee_id, days_off_count)
            
    Note:
        This is a simplified version converted from R. Returns empty structures if input is empty.
    """
    try:
        if len(df_pre_ger) == 0:
            return pd.DataFrame(), [], pd.DataFrame()
        
        # Basic processing
        df_pre_ger = df_pre_ger.copy()
        df_pre_ger.columns = ['employee_id'] + list(df_pre_ger.columns[1:])
        
        # Convert WFM types to TRADS and get unique employees
        df_pre_ger = convert_types_in(df_pre_ger)
        emp_pre_ger = df_pre_ger['employee_id'].unique().tolist()
        
        # Fill missing dates and pivot to matrix format
        df_pre_ger['schedule_dt'] = pd.to_datetime(df_pre_ger['schedule_dt']).dt.strftime('%Y-%m-%d')
        df_pre_ger['sched_subtype'] = df_pre_ger['sched_subtype'].fillna('-')
        
        # Count days off
        df_count = df_pre_ger.groupby('employee_id')['sched_subtype'].apply(
            lambda x: (x.isin(['L', 'LD', 'LQ', 'F', 'V', '-'])).sum()
        ).reset_index(name='days_off_count')
        
        # Use the same reshaping logic as load_pre_ger_scheds
        reshaped_final_3, _ = load_pre_ger_scheds(df_pre_ger, employees_tot_pad)
        
        return reshaped_final_3, emp_pre_ger, df_count
        
    except Exception as e:
        logger.error(f"Error in load_wfm_scheds: {str(e)}")
        return pd.DataFrame(), [], pd.DataFrame()

def convert_types_in(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert WFM types to TRADS - creates 'horario' column based on type and subtype mapping.
    
    Args:
        df: DataFrame with 'type' and 'subtype' columns
        
    Returns:
        DataFrame with new 'horario' column containing mapped values
    """
    # Mapping from (type, subtype) combinations to horario values
    type_map = {
        ('T', 'M'): 'M',      # Trabajo Mañana -> M
        ('T', 'T'): 'T',      # Trabajo Tarde -> T
        ('T', 'H'): 'MoT',    # Trabajo Manhã ou tarde -> MoT (Mañana or Tarde)
        ('T', 'P'): 'P',      # Trabajo Partido -> P
        ('F', None): 'L',     # Folga -> L
        ('F', 'D'): 'LD',     # Folga por trabalhar no domingo/feriado -> LD
        ('F', 'Q'): 'LQ',     # Folga Quincenal -> LQ
        ('F', 'C'): 'C',      # Folga Compensatorio -> C
        ('R', None): 'F',     # Feriado fechado -> F
        ('N', None): '-',     # Dia vazio -> -
        ('T', 'A'): 'V',      # Férias -> V (Férias)
    }
    
    # Create a copy to avoid modifying the original DataFrame
    df_result = df.copy()
    
    # Create the horario column using vectorized operation for better performance
    df_result['horario'] = df_result.apply(
        lambda row: type_map.get((row['type'], row['subtype']), '-'), 
        axis=1
    )
    
    # Set indicator column
    df_result['ind'] = 'P'
    
    return df_result

def convert_ciclos_to_horario(df: pd.DataFrame, l_dom_days: List[int]) -> pd.DataFrame:
    """
    Convert ciclos completos WFM data to algorithm 'horario' codes.
    Simplified version focusing on P (split shift) and MoT (continuous shift).
    
    Mapping logic:
    - tipo_dia == 'F' + dia_semana in [1,8] → 'L_DOM'
    - tipo_dia == 'F' → 'L'
    - tipo_dia == 'S' → '-'
    - tipo_dia == 'N' → 'NL'
    - tipo_dia == 'A':
        - intervalo >= 1 hour → 'P' (split shift)
        - intervalo < 1 hour → 'MoT' (continuous shift)
    - Default → '-'
    
    Args:
        df: DataFrame with columns: tipo_dia, dia_semana, hora_ini_1, hora_fim_1, hora_ini_2, hora_fim_2
        
    Returns:
        DataFrame with 'horario' column added
    """
    df_result = df.copy()
    
    # Normalize column names to lowercase
    df_result.columns = df_result.columns.str.lower()
    
    # Calculate intervalo if time columns exist
    has_time_cols = all(col in df_result.columns for col in ['hora_ini_1', 'hora_fim_1', 'hora_ini_2', 'hora_fim_2'])
    
    if has_time_cols:
        try:
            # Convert time columns to datetime
            for col in ['hora_ini_1', 'hora_fim_1', 'hora_ini_2', 'hora_fim_2']:
                df_result[col] = pd.to_datetime(df_result[col], format="%Y-%m-%d %H:%M:%S", errors='coerce')
            
            # Calculate intervalo (break time between shifts in hours)
            df_result['intervalo'] = df_result.apply(
                lambda row: 0 if pd.isna(row['hora_ini_2']) else 
                (row['hora_ini_2'] - row['hora_fim_1']).total_seconds() / 3600,
                axis=1
            )
        except Exception as e:
            logger.warning(f"Error calculating intervalo: {e}. Setting intervalo to 0.")
            df_result['intervalo'] = 0
    else:
        df_result['intervalo'] = 0
    
    # Apply simplified mapping logic
    def get_horario_code(row):
        tipo_dia = row.get('tipo_dia', '')
        dia_semana = row.get('dia_semana', 0)
        intervalo = row.get('intervalo', 0)
        
        # Free days
        if tipo_dia == 'F':
            if dia_semana in l_dom_days:
                return 'L_DOM'
            return 'L'
        
        # Skip day
        if tipo_dia == 'S':
            return '-'
        
        # Night leave
        if tipo_dia == 'N':
            return 'NL'
        
        # Working days
        if tipo_dia in ['A', 'H']:
            if intervalo >= 1:
                return 'P'
            return 'MoT'
        
        # Default
        return '-'
    
    df_result['horario'] = df_result.apply(get_horario_code, axis=1)
    
    # Log conversion statistics
    horario_counts = df_result['horario'].value_counts()
    tipo_dia_counts = df_result.get('tipo_dia', pd.Series()).value_counts()
    nl_count = (df_result['horario'] == 'NL').sum()
    n_count = (df_result.get('tipo_dia', pd.Series()) == 'N').sum() if 'tipo_dia' in df_result.columns else 0
    
    logger.info(f"convert_ciclos_to_horario: tipo_dia counts - {tipo_dia_counts.to_dict()}")
    logger.info(f"convert_ciclos_to_horario: horario counts - {horario_counts.to_dict()}")
    logger.info(f"convert_ciclos_to_horario: tipo_dia='N' count: {n_count}, horario='NL' count: {nl_count}")
    
    return df_result

def convert_fields_to_int(df: pd.DataFrame, fields: List[str]) -> Tuple[bool, pd.DataFrame, str]:
    """
    Convert fields to int.
    """
    try:
        if len(df) == 0:
            return False, pd.DataFrame(), "Input validation failed: empty dataframe"

        logger.info(f"convert_fields_to_int: starting conversion for fields={fields}")
        logger.info(f"convert_fields_to_int: dtypes BEFORE -> {df[fields].dtypes.to_dict()}")

        for field in fields:
            if field not in df.columns:
                logger.warning(f"convert_fields_to_int: field '{field}' not in dataframe, skipping")
                continue

            converted = pd.to_numeric(df[field], errors='coerce')
            na_count = converted.isna().sum()
            logger.info(
                f"convert_fields_to_int: field='{field}' "
                f"min={converted.min(skipna=True)}, max={converted.max(skipna=True)}, "
                f"na_count={na_count}"
            )

            if na_count > 0:
                return False, pd.DataFrame(), f"Error in convert_fields_to_int: {field} contains NaN values"

            df[field] = converted.astype(np.int64)

        logger.info(f"convert_fields_to_int: dtypes AFTER  -> {df[fields].dtypes.to_dict()}")
        return True, df, ""
    except Exception as e:
        logger.error(f"Error in convert_fields_to_int: {str(e)}")
        return False, pd.DataFrame(), f"Error in convert_fields_to_int: {str(e)}"

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
        
        # Pivot wider
        reshaped = df_pre_ger_filtered.pivot_table(
            index='employee_id', 
            columns='schedule_dt', 
            values='sched_subtype', 
            aggfunc='first'
        ).reset_index()
        
        # Create column names row
        column_names = pd.DataFrame([reshaped.columns.tolist()], columns=reshaped.columns)
        column_names.iloc[0, 0] = "Dia"
        
        # Convert employee_id to string
        reshaped['employee_id'] = reshaped['employee_id'].astype(str)
        
        # Combine column names with data
        reshaped_names = pd.concat([column_names, reshaped], ignore_index=True)
        
        # Duplicate columns to get M/T shifts
        first_col = reshaped_names.iloc[:, [0]]
        last_cols = reshaped_names.iloc[:, 1:]
        
        # Duplicate last columns
        duplicated_cols = pd.concat([last_cols, last_cols], axis=1)
        
        # Sort columns by name
        duplicated_cols = duplicated_cols.reindex(sorted(duplicated_cols.columns), axis=1)
        
        # Combine first column with duplicated columns
        reshaped_final = pd.concat([first_col, duplicated_cols], axis=1)
        
        # Reset column and row names
        reshaped_final.columns = range(reshaped_final.shape[1])
        reshaped_final.reset_index(drop=True, inplace=True)
        
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
        
        return reshaped_final_3, emp_pre_ger
        
    except Exception as e:
        logger.error(f"Error in load_pre_ger_scheds: {str(e)}")
        return pd.DataFrame(), []

def get_first_and_last_day_passado_arguments(start_date_str: str, end_date_str: str, main_year: str, wfm_proc_colab: str, use_case: int = 0) -> Tuple[str, str, int]:
    """
    Data treatment logic for past dates according to what the external_call_data params are.
    The output of this function is going to query the database for past calendars. 
    Find the rest of the logic on treat_df_calendario_passado helper function.
    
    This function implements 8 different business logic cases based on:
    - Whether start_date equals January 1st of main_year
    - Whether end_date equals December 31st of main_year
    - Whether wfm_proc_colab parameter is empty or not
    
    Args:
        start_date_str (str): Start date in 'YYYY-MM-DD' format
        end_date_str (str): End date in 'YYYY-MM-DD' format
        main_year (str): The main year to compare dates against (YYYY format)
        wfm_proc_colab (str): WFM process collaborator parameter, empty string or value
    Returns:
        Tuple[str, str, int]: A tuple containing (first_day_passado, last_day_passado, case_type) in 'YYYY-MM-DD' format.
                        Returns ('', '') if an error occurs.
                        
    Business Logic Cases:
        CASE 1: start=01-01, end=31-12, colab='' -> (Monday of prev week, day before start)
        CASE 2: start>01-01, end=31-12, colab='' -> (01-01, day before start)
        CASE 3: start=01-01, end<31-12, colab='' -> (Monday of prev week, day before start)
        CASE 4: start>01-01, end<31-12, colab='' -> (01-01, 31-12)
        CASE 5: start=01-01, end=31-12, colab!='' -> (Monday of prev week, Sunday of next week)
        CASE 6: start>01-01, end=31-12, colab!='' -> (01-01, Sunday of next week)
        CASE 7: start=01-01, end<31-12, colab!='' -> (Monday of prev week, Sunday of next week)
        CASE 8: start>01-01, end<31-12, colab!='' -> (01-01, 31-12)
        
    Raises:
        Logs error and returns empty strings if date parsing fails or invalid date ranges provided.
    """
    try:
        # Validate arguments - Note: wfm_proc_colab can be empty string (it's a valid business case)
        if not start_date_str or not end_date_str or not main_year or wfm_proc_colab is None:
            logger.error(f"Invalid arguments provided to get_first_and_last_day_passado")
            return '', '', 0
        
        # Treat date 
        start_date_dt = pd.to_datetime(start_date_str, format='%Y-%m-%d')
        end_date_dt = pd.to_datetime(end_date_str, format='%Y-%m-%d')

        first_january = pd.to_datetime(f'{main_year}-01-01', format='%Y-%m-%d')
        last_december = pd.to_datetime(f'{main_year}-12-31', format='%Y-%m-%d')

        # Salsa uses this!!!
        if use_case == 0:
            first_day_passado_str = get_monday_of_previous_week(first_january)
            last_day_passado_str = get_sunday_of_next_week(last_december)
            if wfm_proc_colab == '':
                case_type = 1
            else:
                case_type = 8

        # Deprecated logic, only here if its needed again 
        elif use_case == 1:
            # CASO 1: start_date = 01-01 e end_date = 31-12 e wfm_proc_colab = ''
            if start_date_dt == first_january and end_date_dt == last_december and wfm_proc_colab == '':
                first_day_passado_str = get_monday_of_previous_week(start_date_str)
                last_day_passado_str = (start_date_dt - dt.timedelta(days=1)).strftime('%Y-%m-%d')
                case_type = 1

            # CASO 2: start_date > 01-01 e end_date = 31-12 e wfm_proc_colab = ''
            elif start_date_dt > first_january and end_date_dt == last_december and wfm_proc_colab == '':
                first_day_passado_str = first_january.strftime('%Y-%m-%d')
                last_day_passado_str = (start_date_dt - dt.timedelta(days=1)).strftime('%Y-%m-%d')
                case_type = 2

            # CASO 3: start_date = 01-01 e end_date < 31-12 e wfm_proc_colab = ''
            elif start_date_dt == first_january and end_date_dt < last_december and wfm_proc_colab == '':
                first_day_passado_str = get_monday_of_previous_week(start_date_str)
                last_day_passado_str = (start_date_dt - dt.timedelta(days=1)).strftime('%Y-%m-%d')
                case_type = 3
                
            # CASO 4: start_date > 01-01 e end_date < 31-12 e wfm_proc_colab = ''
            elif start_date_dt > first_january and end_date_dt < last_december and wfm_proc_colab == '':
                first_day_passado_str = first_january.strftime('%Y-%m-%d')
                last_day_passado_str = last_december.strftime('%Y-%m-%d')
                case_type = 4

            # CASO 5: start_date = 01-01 e end_date = 31-12 e wfm_proc_colab != ''
            elif start_date_dt == first_january and end_date_dt == last_december and wfm_proc_colab != '':
                first_day_passado_str = get_monday_of_previous_week(start_date_str)
                last_day_passado_str = get_sunday_of_next_week(end_date_str)
                case_type = 5

            # CASO 6: start_date > 01-01 e end_date = 31-12 e wfm_proc_colab != ''
            elif start_date_dt > first_january and end_date_dt == last_december and wfm_proc_colab != '':
                first_day_passado_str = first_january.strftime('%Y-%m-%d')
                last_day_passado_str = get_sunday_of_next_week(end_date_str)
                case_type = 6

            # CASO 7: start_date = 01-01 e end_date < 31-12 e wfm_proc_colab != ''
            elif start_date_dt == first_january and end_date_dt < last_december and wfm_proc_colab != '':
                first_day_passado_str = get_monday_of_previous_week(start_date_str)
                last_day_passado_str = get_sunday_of_next_week(end_date_str)
                case_type = 7

            # CASO 8: start_date > 01-01 e end_date < 31-12 e wfm_proc_colab != ''
            elif start_date_dt > first_january and end_date_dt < last_december and wfm_proc_colab != '':
                first_day_passado_str = first_january.strftime('%Y-%m-%d')
                last_day_passado_str = last_december.strftime('%Y-%m-%d')
                case_type = 8
            # No other cases are predicted - fall back gracefully
            else:
                logger.error(f"start_date {start_date_str} and end_date {end_date_str} are not compatible with the programed logic")
                return '', '', 0

        else:
            logger.error(f"use_case provided not valid, please ensure the correct use_case is defined.")
            return '', '', 0

        return first_day_passado_str, last_day_passado_str, case_type
    except Exception as e:
        logger.error(f"Error in get_first_and_last_day_passado: {str(e)}", exc_info=True)
        return '', '', 0

def get_monday_of_previous_week(date_str: str) -> str:
    """
    Get the Monday of the week before the given date.
    
    This function calculates the Monday of the week preceding the week that contains
    the input date. For example, if the input date is a Wednesday, it will return
    the Monday of the previous week (not the Monday of the current week).
    
    Args:
        date_str (str): Date in 'YYYY-MM-DD' format
        
    Returns:
        str: Date of the Monday of the previous week in 'YYYY-MM-DD' format.
             Returns empty string if date parsing fails.
             
    Examples:
        get_monday_of_previous_week('2024-01-08')  # Monday -> '2024-01-01'
        get_monday_of_previous_week('2024-01-10')  # Wednesday -> '2024-01-01'  
        get_monday_of_previous_week('2024-01-14')  # Sunday -> '2024-01-01'
        
    Algorithm:
        - Get weekday (0=Monday, 6=Sunday)
        - Calculate days to subtract: weekday + 7
        - Subtract from input date to get previous Monday
    """
    try:
        date_dt = pd.to_datetime(date_str, format='%Y-%m-%d')
        
        # Get the weekday (0=Monday, 6=Sunday)
        weekday = date_dt.weekday()
        
        # Calculate days to subtract to get to the Monday of the previous week
        # If it's Monday (0), go back 7 days to previous Monday
        # If it's Tuesday (1), go back 8 days, etc.
        days_to_subtract = weekday + 7
        
        previous_monday = date_dt - dt.timedelta(days=days_to_subtract)
        return previous_monday.strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Error in get_monday_of_previous_week: {str(e)}")
        return ''

def get_sunday_of_next_week(date_str: str) -> str:
    """
    Get the Sunday of the week after the given date.
    
    This function calculates the Sunday of the week following the week that contains
    the input date. For example, if the input date is a Wednesday, it will return
    the Sunday of the following week (not the Sunday of the current week).
    
    Args:
        date_str (str): Date in 'YYYY-MM-DD' format
        
    Returns:
        str: Date of the Sunday of the next week in 'YYYY-MM-DD' format.
             Returns empty string if date parsing fails.
             
    Examples:
        get_sunday_of_next_week('2024-01-01')  # Monday -> '2024-01-07'
        get_sunday_of_next_week('2024-01-03')  # Wednesday -> '2024-01-07'
        get_sunday_of_next_week('2024-01-06')  # Saturday -> '2024-01-07'
        get_sunday_of_next_week('2024-01-07')  # Sunday -> '2024-01-14'
        
    Algorithm:
        - Get weekday (0=Monday, 6=Sunday)
        - Calculate days to add: (6 - weekday) % 7, with special case for Sunday
        - If already Sunday, add 7 days to get next Sunday
        - Add to input date to get next Sunday
    """
    try:
        date_dt = pd.to_datetime(date_str, format='%Y-%m-%d')
        weekday = date_dt.weekday()
        # Calculate days to add to get to next Sunday
        # Monday=0 needs +6, Tuesday=1 needs +5, ..., Saturday=5 needs +1, Sunday=6 needs +7
        days_to_add = (6 - weekday) % 7
        if days_to_add == 0:  # If it's already Sunday, go to next Sunday
            days_to_add = 7
        next_sunday = date_dt + dt.timedelta(days=days_to_add)
        return next_sunday.strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Error in get_sunday_of_next_week: {str(e)}")
        return ''

def create_employee_query_string(employee_id_list: List[str]) -> str:
    """
    Creates a string with the employee_ids for query substitution.
    Args:
        employee_id_list (List[str]): List of employee ids - could be fk_colaborador or matricula
    Returns:
        str: String with the employee ids for query substitution (e.g., "'123','456','789'")
    Raises:
        Exception: If an error occurs
    """
    try:
        if len(employee_id_list) == 0:
            logger.warning("Empty employee_id_list provided to create_employee_query_string")
            return ''
        
        # Convert all IDs to strings and filter out empty values
        employee_ids = [str(x).strip() for x in employee_id_list if x is not None and str(x).strip()]
        
        if not employee_ids:
            logger.warning("No valid employee IDs after filtering")
            return ''
        
        # Create comma-separated string with each ID wrapped in single quotes
        employee_str = ','.join(f"'{x}'" for x in employee_ids)
        
        logger.info(f"Employee ids string for query created: {employee_str} ({len(employee_ids)} IDs)")
        return employee_str
    except Exception as e:
        logger.error(f"Error creating employee query string: {str(e)}")
        logger.exception("Exception details:")
        return ''

def count_holidays_in_period(start_date_str: str, end_date_str: str, df_feriados: pd.DataFrame, use_case: int) -> Tuple[int, int]:
    """
    Count open and closed holidays within a date range for scheduling calculations.
    
    Holidays are classified into two categories that affect employee scheduling:
    - Open holidays: Store is open, employees work (tipo='A')
    - Closed holidays: Store is closed, no one works (tipo='F')
    
    This distinction is critical for:
    - Calculating Sunday/holiday work requirements
    - Determining rest day allocations
    - Adjusting employee quotas for the period
    
    Holiday Type Classification:
        - Type 'A' (Abierto): Store open on holiday (employees must work)
        - Type 'F' (Feriado): Store closed on holiday (no staffing needed)
    
    Use Cases:
        - Case 0: Return zeros (disabled, used when l_dom calculation doesn't need holidays)
        - Case 1: Count holidays by type ('A' vs 'F')
        - Case 2: Reserved for future implementation
    
    Args:
        start_date_str: Period start date (YYYY-MM-DD format) - currently unused
        end_date_str: Period end date (YYYY-MM-DD format) - currently unused
        df_feriados: Holiday calendar DataFrame with columns: schedule_day, tipo_feriado
        use_case: Processing mode (0=disabled, 1=count by type)
        
    Returns:
        Tuple containing:
            - num_feriados_abertos (int): Count of open holidays (type 'A')
            - num_feriados_fechados (int): Count of closed holidays (type 'F')
            Returns (-1, -1) on error
            
    Note:
        Current implementation filters entire df_feriados for matching tipos
        but doesn't filter by date range. Consider adding date filtering
        for more accurate period-specific counts.
    """
    try:
        # Case 0: num_festivos is 0 for l_dom calculations
        if use_case == 0:
            num_feriados_abertos = 0
            num_feriados_fechados = 0
        else:
            df_feriados['schedule_day'] = pd.to_datetime(df_feriados['schedule_day'])
            # Case 1: count the number of feriados by type
            if use_case == 1:
                tipo_feriado_values = ['A', 'F']
                df_filtered = df_feriados[df_feriados['tipo_feriado'].isin(tipo_feriado_values)]
                num_feriados_abertos = len(df_filtered[df_filtered['tipo_feriado'] == 'A'])
                num_feriados_fechados = len(df_filtered[df_filtered['tipo_feriado'] == 'F'])

            # Case 2: count the number of feriados from tipo 2
            elif use_case == 2:
                raise NotImplementedError
            else:
                logger.error(f"Use case provided not valid: {use_case}")
                return -1, -1

        return num_feriados_abertos, num_feriados_fechados
    except Exception as e:
        logger.error(f"Error in count_holidays_in_period: {str(e)}", exc_info=True)
        return -1, -1

def count_sundays_in_period(first_day_year_str: str, last_day_year_str: str, start_date_str: Optional[str], end_date_str: Optional[str]) -> int:
    """
    Count the number of Sundays within a specified date range.
    
    This function generates a complete date sequence and identifies all Sundays
    (weekday 7) within the period. Sunday counts are critical for:
    - Calculating Sunday/holiday work quotas
    - Determining l_dom (Sunday rest day allocations)
    - Adjusting contract-specific rest requirements
    
    Business Context:
        Labor agreements often specify:
        - Minimum Sundays off per year
        - Maximum Sundays worked per contract type
        - Proportional Sunday quotas for part-year employees
        
        This count enables accurate quota calculations and compliance tracking.
    
    Algorithm:
        1. Generate date range from first_day_year to last_day_year
        2. Calculate weekday for each date (1=Monday, 7=Sunday)
        3. Count dates where weekday equals 7
    
    Args:
        first_day_year_str: Start of counting period (YYYY-MM-DD format)
        last_day_year_str: End of counting period (YYYY-MM-DD format)
        start_date_str: Optional - reserved for future use
        end_date_str: Optional - reserved for future use
        
    Returns:
        int: Number of Sundays in the period
             Returns -1 on error
             
    Example:
        >>> count_sundays_in_period('2024-01-01', '2024-01-31', None, None)
        5  # January 2024 has 5 Sundays
        
    Note:
        Currently uses first_day_year and last_day_year parameters.
        start_date_str and end_date_str are included in signature but not used
        (possibly for future flexible date range filtering).
    """
    try:
        # Validate inputs before attempting to parse
        if not first_day_year_str or not last_day_year_str:
            logger.error(f"Invalid arguments: first_day_year_str='{first_day_year_str}', last_day_year_str='{last_day_year_str}'")
            return -1
            
        start_date_dt = pd.to_datetime(first_day_year_str, format='%Y-%m-%d')
        end_date_dt = pd.to_datetime(last_day_year_str, format='%Y-%m-%d')
        
        day_seq = pd.date_range(start=start_date_dt, end=end_date_dt, freq='D')

        df = pd.DataFrame({
            'day_seq': day_seq,
            'wd': pd.Series(day_seq).dt.dayofweek + 1  # Convert to 1-7 where 1=Monday, 7=Sunday            
        })

        num_sundays_year = len(df[df['wd'] == 7])

        return num_sundays_year

    except Exception as e:
        logger.error(f"Error calculating sundays amount on count_sundays_in_period helper function:{str(e)}", exc_info=True)
        return -1

# Taken from original helpers functions but maybe no need for it
def count_open_holidays(matriz_festivos: pd.DataFrame, tipo_contrato: int) -> List[int]:
    """
    Helper method to count open holidays based on contract type.
    
    Args:
        matriz_festivos: DataFrame with holiday data
        tipo_contrato: Contract type (2 or 3)
        
    Returns:
        List with [l_dom_count, total_working_days]
    """
    try:
        # Convert data column to datetime if not already
        matriz_festivos['data'] = pd.to_datetime(matriz_festivos['data'])
        
        if tipo_contrato == 3:
            # Count holidays Monday to Thursday (weekday 0-3)
            weekday_holidays = matriz_festivos[
                (matriz_festivos['data'].dt.weekday >= 0) & 
                (matriz_festivos['data'].dt.weekday <= 3)
            ]
        elif tipo_contrato == 2:
            # Count holidays Monday to Friday (weekday 0-4)
            weekday_holidays = matriz_festivos[
                (matriz_festivos['data'].dt.weekday >= 0) & 
                (matriz_festivos['data'].dt.weekday <= 4)
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
        logger.error(f"Error in count_open_holidays: {str(e)}")
        return [0, 0]

def convert_types_out(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert internal algorithm codes (HORARIO) to WFM system format.
    
    This function performs the reverse transformation of convert_types_in(), mapping
    algorithm-generated schedule codes back to WFM (Workforce Management) system
    format for export and integration.
    
    Transformation: HORARIO → (sched_type, sched_subtype)
    
    Schedule Type Mappings (sched_type):
        - T (Trabajo): Work shifts
          - M, T, MoT, ToM, P, V, A, DFS → 'T'
        - F (Folga/Free): Rest days
          - L, LD, LQ, C → 'F'
        - R (Rotativo): Holiday work
          - F → 'R'
        - N (No definido): Not scheduled
          - '-' → 'N'
    
    Schedule Subtype Mappings (sched_subtype):
        - Shift specifics:
          - M → 'M' (Morning), T → 'T' (Afternoon)
          - MoT/ToM → 'H' (Hours/flexible)
          - P → 'P' (Split shift/Partido)
        - Rest day specifics:
          - LD → 'D' (Sunday/Domingo)
          - LQ → 'Q' (Quality/Quincenal)
          - C → 'C' (Compensatory)
        - Absence codes:
          - V, A → 'A' (Absence/Ausencia)
          - DFS → 'C' (Compensatory for special days)
    
    Business Context:
        Essential for:
        - Exporting schedules to WFM system
        - Integration with payroll systems
        - Reporting in external formats
        - Compliance documentation
    
    Args:
        df: DataFrame with 'horario' column containing algorithm codes
    
    Returns:
        DataFrame with added columns:
            - sched_type: High-level schedule type (T/F/R/N)
            - sched_subtype: Detailed schedule variant (M/T/H/P/D/Q/C/A or empty)
            
    Example:
        Input:  | horario |
                | M       |
                | LD      |
                | P       |
        
        Output: | horario | sched_type | sched_subtype |
                | M       | T          | M             |
                | LD      | F          | D             |
                | P       | T          | P             |
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
    Execute bulk database insert with automatic retry logic for connection failures.
    
    This function provides robust database insertion with:
    - Parameterized queries from SQL files (prevents SQL injection)
    - Automatic connection recovery and retry logic
    - Progress logging for large datasets
    - Transaction management (commit on success, rollback on error)
    - Detailed error handling and logging
    
    Retry Strategy:
        - Detects connection-related errors (DPI-1010, ORA-12170, ORA-03135, etc.)
        - Automatically recreates database session on connection failure
        - Up to 2 retry attempts (3 total attempts including initial)
        - Tests new session before retrying insert
    
    Connection Error Detection:
        Monitors for: 'not connected', 'dpi-1010', 'connection', 'timeout',
        'closed', 'broken', 'lost', 'ora-12170', 'ora-03135', 'ora-00028', 'ora-02391'
    
    Performance Features:
        - Batch execution using executemany (via SQLAlchemy) for dramatically faster inserts
        - Fallback to row-by-row execution if batch mode fails
        - Progress logging every 1000 records
        - Single transaction for all records
        - Parameter binding for efficient execution
        - Minimal overhead for record conversion
    
    Business Context:
        Used for:
        - Persisting algorithm-generated schedules to database
        - Bulk loading configuration data
        - Saving optimization results
        - Archiving historical data
    
    Args:
        data_manager: DBDataManager instance with active database session
        data: DataFrame containing records to insert (columns must match query parameters)
        query_file: Path to SQL file with parameterized INSERT statement
        **kwargs: Additional parameters to merge with each record (e.g., execution metadata)
        
    Returns:
        bool: True if all records inserted successfully, False on failure
        
    Query File Format:
        SQL file should contain parameterized INSERT with named placeholders:
        ```sql
        INSERT INTO table_name (col1, col2, col3)
        VALUES (:param1, :param2, :param3)
        ```
        
    Example:
        >>> success = bulk_insert_with_query(
        ...     data_manager=db_manager,
        ...     data=df_results,
        ...     query_file='insert_schedule.sql',
        ...     execution_id=12345,
        ...     execution_date='2024-01-15'
        ... )
        
    Error Handling:
        - Validates session before execution
        - Checks query file existence
        - Handles empty DataFrames gracefully
        - Rolls back on any insert failure
        - Provides detailed error logging
        
    Note:
        - Removes 'pathOS' from kwargs if present (connection metadata)
        - Creates completely new session on retry (not just reconnect)
        - Commits only after all records inserted successfully
        - Uses batch executemany for performance, with automatic fallback to row-by-row
        - NaN/None in string columns replaced with '' before batch (Oracle treats '' as NULL)
    """
    logger = get_logger(PROJECT_NAME)
   
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
    batch_size = 1000
   
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
           
            # Prepare records for insertion
            # Fill NaN/None with '' ONLY in string/object columns for cx_Oracle executemany compatibility.
            # cx_Oracle infers column types from the first row; if the first row has None for a string
            # column, it cannot determine the type and fails on subsequent rows with actual values.
            # Oracle treats '' and NULL as identical, so this has zero effect on stored data.
            data_copy = data.copy()
            string_cols = data_copy.select_dtypes(include=['object']).columns
            if len(string_cols) > 0:
                data_copy[string_cols] = data_copy[string_cols].fillna('')
            
            records = data_copy.to_dict('records')
            
            # Merge kwargs into records if provided
            clean_kwargs = {k: v for k, v in kwargs.items() if k != 'pathOS'} if kwargs else {}
            if clean_kwargs:
                records = [{**clean_kwargs, **record} for record in records]
            
            # --- Batch execution (executemany) with full fallback to row-by-row ---
            # Strategy: try ALL batches first. If executemany fails on any batch,
            # rollback EVERYTHING and re-insert ALL records row-by-row from scratch.
            # This avoids partial state where some batches succeed and others don't.
            total_records = len(records)
            inserted_count = 0
            use_row_by_row = False
            
            try:
                for batch_start in range(0, total_records, batch_size):
                    batch_end = min(batch_start + batch_size, total_records)
                    batch = records[batch_start:batch_end]
                    
                    # Fast path: batch executemany
                    # SQLAlchemy 2.0 uses cursor.executemany() when given a list of dicts
                    data_manager.session.execute(text(insert_query), batch)
                    inserted_count += len(batch)
                    logger.info(f"Batch inserted {inserted_count}/{total_records} records")
                    
            except Exception as batch_error:
                # Check if this is a connection error — let the outer retry handler deal with it
                batch_error_str = str(batch_error).lower()
                connection_error_keywords = [
                    'not connected', 'dpi-1010', 'connection', 'timeout', 'closed',
                    'broken', 'lost', 'ora-12170', 'ora-03135', 'ora-00028', 'ora-02391'
                ]
                if any(kw in batch_error_str for kw in connection_error_keywords):
                    raise  # Propagate to outer retry loop
                
                # Non-connection error: executemany is not compatible with this data.
                # Rollback everything and switch to row-by-row for ALL records.
                logger.warning(f"Batch executemany failed, rolling back and retrying "
                               f"all {total_records} records row-by-row: {batch_error}")
                try:
                    data_manager.session.rollback()
                except Exception:
                    pass
                use_row_by_row = True
                inserted_count = 0
            
            # Fallback: row-by-row for ALL records (only runs if batch mode failed)
            if use_row_by_row:
                for i, record in enumerate(records):
                    try:
                        data_manager.session.execute(text(insert_query), record)
                        inserted_count += 1
                        
                        if inserted_count % 1000 == 0:
                            logger.info(f"Row-by-row: processed {inserted_count}/{total_records} records")
                            
                    except Exception as record_error:
                        logger.error(f"Error inserting record {i + 1}: {str(record_error)}")
                        logger.debug(f"Failed record data: {record}")
                        raise
            
            # Commit all inserts
            data_manager.session.commit()
           
            logger.info(f"Successfully inserted {inserted_count} records")
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


def adjusted_isoweek(date) -> int:
    """
    Calculate adjusted ISO week number.
    
    Special handling: If ISO week is 1 but month is December, return 53 instead.
    This prevents last week of December from being labeled as week 1 of next year.
    
    Args:
        date: Date to calculate week for (can be datetime, Timestamp, or string)
        
    Returns:
        Adjusted ISO week number (1-53)
    """
    date = pd.to_datetime(date)
    week = date.isocalendar().week
    month = date.month
    
    # If week is 1 but date is in December, it's actually week 53
    if week == 1 and month == 12:
        return 53
    
    return week


def get_section_employees_id_list(df_mpd_valid_employees: pd.DataFrame) -> Tuple[bool, List[int], str]:
    """
    Get all employee IDs from the entire section for matricula mapping.
    
    This function extracts ALL employees from df_mpd_valid_employees regardless of
    posto or execution mode. Used to build employee_id_matriculas_map which needs
    the complete section context even when executing for a single employee.
    
    Why this exists:
        - In single-employee mode (wfm_proc_colab set), df_valid_emp only contains 1 employee
        - But employee_id_matriculas_map needs ALL employees to create df_calendario
        - df_mpd_valid_employees always contains all employees in the section
        - This function provides section-level data needed before the posto loop begins
    
    Note: 
        This is different from get_past_employees_id_list which filters by posto
        and is called per-posto. This function is called ONCE before the posto loop.
        
    TODO: This logic will be abandoned after STRSOL-1180 is implemented.
          After STRSOL-1180, df_valid_emp will contain all employees in the section
          with a field indicating if the employee should be executed or not.
          At that point, we can use employees_id_total_list directly.
    
    Args:
        df_mpd_valid_employees: DataFrame containing all employees in the section
        
    Returns:
        Tuple of (success: bool, section_employees_id_list: List[int], message: str)
    """
    try:
        # Validate input DataFrame
        if df_mpd_valid_employees is None or df_mpd_valid_employees.empty:
            return False, [], "df_mpd_valid_employees is None or empty"
        
        if 'employee_id' not in df_mpd_valid_employees.columns:
            return False, [], "employee_id column not found in df_mpd_valid_employees"
        
        # Extract all unique employee IDs from the section
        section_employees_id_list = [int(x) for x in df_mpd_valid_employees['employee_id'].unique()]
        
        if len(section_employees_id_list) == 0:
            return False, [], "No employees found in df_mpd_valid_employees"
        
        logger.info(f"Extracted {len(section_employees_id_list)} employees from section (df_mpd_valid_employees)")
        
        return True, section_employees_id_list, "Success"
        
    except Exception as e:
        logger.error(f"Error extracting section employees list: {e}", exc_info=True)
        return False, [], f"Error extracting section employees list: {str(e)}"


def get_past_employees_id_list(wfm_proc_colab: str, df_mpd_valid_employees: pd.DataFrame, fk_tipo_posto: str, employees_id_list_for_posto: List[str]) -> Tuple[bool, List[int], str]:
    """
    Get employees from the past (colabs_passado) for a specific job position type. this variabe is going to contain wfm_proc_colab and everytime we need, we pop it out.
    
    Args:
        wfm_proc_colab: The employee ID to exclude from the past employees list
        df_mpd_valid_employees: DataFrame containing valid employees data
        fk_tipo_posto: Job position type filter
        
    Returns:
        Tuple of (success: bool, colabs_passado: List[int], message: str)
    """

    # Validate there is not multiple employees with expected conditions
    try:
        # CASE 1: SECTION EXECUTION
        if wfm_proc_colab == '':
            colabs_passado = employees_id_list_for_posto
        # CASE 2: SINGLE-COLAB EXECUTION
        else:
            df = df_mpd_valid_employees[df_mpd_valid_employees['fk_tipo_posto'] == fk_tipo_posto]
            mask = (df['gera_horario_ind'] == 'Y') & (df['existe_horario_ind'] == 'N')
            colabs_a_gerar = df[mask]['employee_id'].unique()

            wfm_proc_colab = int(wfm_proc_colab)
            
            if len(colabs_a_gerar) == 0:
                return False, [], "No employees found with the expected conditions."
                
            if len(colabs_a_gerar) > 1:
                return False, [], "There should be only one employee for allocation."

            if int(colabs_a_gerar[0]) != wfm_proc_colab:
                return False, [], "The employee is not present in the df_mpd_valid_employees query."

            colabs_passado = [int(x) for x in df['employee_id'].unique()]
            #colabs_passado.remove(wfm_proc_colab)
            logger.info(f"Created colabs_passado list: {colabs_passado}")


        return True, colabs_passado, "Success"
    except Exception as e:
        logger.error(f"Error creating colabs_passado list: {e}", exc_info=True)
        return False, [], "Error creating the colabs_passado_list"

def get_employees_id_90_list(employees_id_list_for_posto: List[str], df_colaborador: pd.DataFrame) -> Tuple[bool, List[str], str]:
    """
    Identify employees who follow 90-day complete rotation cycles (CICLO COMPLETO).
    
    Complete 90-day cycles are comprehensive schedules that define every working day
    and rest day over a 3-month period. These employees have their schedules fully
    predetermined and don't follow the standard weekly rotation patterns.
    
    Identification Logic:
        1. Filter df_colaborador to only employees in employees_id_list_for_posto
        2. Identify employees where seq_turno (upper case) equals 'CICLO'
        3. Return list of their employee IDs (fk_colaborador)
    
    Business Context:
        Employees with complete cycles:
        - Have complex rotation patterns not suitable for weekly scheduling
        - Require special handling in the algorithm
    
    Common Use Case:
        In single-employee generation mode (wfm_proc_colab specified):
        - employees_id_list_for_posto contains only the target employee
        - Function checks if that employee uses complete cycles
        - Enables algorithm to apply appropriate scheduling logic
    
    Args:
        employees_id_list_for_posto: List of employee IDs to check (scope filter)
        df_colaborador: Employee master data with seq_turno column
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded, False on error
            - employees_id_90_list (List[str]): Employee IDs using CICLO COMPLETO
            - error_message (str): Error description if failed, empty string otherwise
            
    Example:
        >>> success, ciclo_employees, err = get_employees_id_90_list(
        ...     [101, 102, 103], df_colaborador
        ... )
        >>> print(ciclo_employees)
        [102]  # Only employee 102 has seq_turno='CICLO'
        
    Note:
        Returns empty list (not error) if no employees match CICLO criteria.
        This is a valid scenario when all employees use standard rotations.
    """
    try:
        df_colaborador = df_colaborador.copy()
        # First mask is to ensure that for "geracao ao colaborador" we only consider for this list that employee (in this cases, employees_id_list_for_posto will only contain wfm_proc_colab)
        mask = df_colaborador['employee_id'].isin(employees_id_list_for_posto)
        df_colaborador = df_colaborador[mask]

        # Second mask is to get the employees that are considered CICLO COMPLETO
        mask = (df_colaborador['seq_turno'].str.upper() == 'CICLO') & (df_colaborador['ciclo'].str.upper() == 'COMPLETO')
        employees_id_90_list = df_colaborador[mask]['employee_id'].to_list()

        return True, employees_id_90_list, ""

    except Exception as e:
        logger.error(f"Error getting employees id 90 list: {e}", exc_info=True)
        return False, [], "Error getting employees id 90 list"

def get_week_pattern(seq_turno: str, semana1: str, week_in_cycle: int) -> str:
    """
    Determine the shift pattern for a specific week in the rotation cycle.
    
    Args:
        seq_turno: Shift sequence type (M, T, MT, MMT, MTT, MoT, P, CICLO)
        semana1: Starting week indicator (M, M1, M2, T, T1, T2)
        week_in_cycle: Position in the rotation cycle (0-based)
        
    Returns:
        str: The shift pattern for that week ('M', 'T', 'MoT', 'P', 'CICLO')
        
    Examples:
        - get_week_pattern('MT', 'M1', 0) -> 'M'  (first week is morning)
        - get_week_pattern('MT', 'M1', 1) -> 'T'  (second week is afternoon)
        - get_week_pattern('MoT', 'M', 0) -> 'MoT'  (all weeks are morning or afternoon)
    """
    # Fixed patterns (no rotation)
    if seq_turno in ['M', 'T', 'MoT', 'P', 'CICLO']:
        return seq_turno
    
    # MT pattern (2-week cycle)
    if seq_turno == 'MT':
        if semana1 in ['M', 'M1']:
            return 'M' if week_in_cycle == 0 else 'T'
        else:  # T, T1
            return 'T' if week_in_cycle == 0 else 'M'
    
    # MMT pattern (3-week cycle)
    if seq_turno == 'MMT':
        if semana1 == 'M1':
            return ['M', 'M', 'T'][week_in_cycle]
        elif semana1 == 'M2':
            return ['M', 'T', 'M'][week_in_cycle]
        else:  # T, T1
            return ['T', 'M', 'M'][week_in_cycle]
    
    # MTT pattern (3-week cycle)
    if seq_turno == 'MTT':
        if semana1 in ['M', 'M1']:
            return ['M', 'T', 'T'][week_in_cycle]
        elif semana1 == 'T1':
            return ['T', 'M', 'T'][week_in_cycle]
        else:  # T2
            return ['T', 'T', 'M'][week_in_cycle]
    
    # Unknown pattern - return as-is
    return seq_turno

def get_valid_emp_info(df_valid_emp: pd.DataFrame) -> Tuple[Union[int, str], int, List[int], Dict[int, List[str]], List[str]]:
    """
    Extract organizational structure and employee groupings from validated employee data.
    
    This function parses the validated employee DataFrame to identify the organizational
    hierarchy and employee distribution. It assumes all employees belong to the same
    unit and section but may be distributed across multiple job positions (postos).
    
    Extracted Information:
        1. unit_id: Organizational unit (assumes single unit); preserved as string or int from source
        2. secao_id: Section within unit (assumes single section)
        3. posto_id_list: All unique job position types in scope
        4. employees_by_posto_dict: Employees grouped by their job position
        5. employees_id_total_list: All employee IDs across all positions
    
    Data Structure Assumptions:
        - All employees share same fk_unidade (single unit execution)
        - All employees share same fk_secao (single section execution)
        - Employees may belong to different fk_tipo_posto (multiple job positions)
        - Each employee has exactly one position
    
    Business Context:
        Used for:
        - Hierarchical parameter resolution (get_param_for_posto)
        - Position-specific schedule generation
        - Resource allocation by job type
        - Organizational reporting and validation
    
    Args:
        df_valid_emp: Validated employee DataFrame with columns:
            - fk_unidade: Unit identifier (int or str)
            - fk_secao: Section identifier
            - fk_tipo_posto: Job position type
            - fk_colaborador: Employee identifier
        
    Returns:
        Tuple containing:
            - unit_id (str): Unique unit ID as string (first value from unique set; supports numeric or string IDs)
            - secao_id (int): Unique section ID (first value from unique set)
            - posto_id_list (List[int]): List of all job position types
            - employees_by_posto_dict (Dict[int, List[str]]): Map of posto_id → employee_id_list
            - employees_id_total_list (List[str]): All unique employee IDs as strings
            
    Example Output:
        >>> unit_id, secao_id, posto_ids, emp_by_posto, all_emps = get_valid_emp_info(df)
        >>> print(unit_id)
        '1' or 'ABC'
        >>> print(secao_id)
        5
        >>> print(posto_ids)
        [10, 20, 30]
        
    Error Handling:
        Returns ("", 0, [], {}, []) on any exception, with error logged.
        This signals to caller that organizational info extraction failed.
        
    Note:
        unit_id is normalized to string to support both numeric and string unit identifiers.
        Converts posto_id keys to int and employee IDs to str for consistency
        with downstream processing requirements.
    """
    try:
        logger.info(f"Getting unit_id, secao_id, posto_id_list, employees_by_posto_dict, employees_id_total_list")
        raw_unit = df_valid_emp['fk_unidade'].unique()[0]
        unit_id = str(raw_unit).strip() if raw_unit is not None else ""
        secao_id = int(df_valid_emp['fk_secao'].unique()[0])   # Get first (and only) unique value
        posto_id_list = df_valid_emp['fk_tipo_posto'].unique().astype(int).tolist()  # Get list of unique values

        # Group employees by posto_id
        employees_id_total_list = df_valid_emp['employee_id'].unique().astype(str).tolist()
        employees_by_posto_dict = df_valid_emp.groupby('fk_tipo_posto')['employee_id'].apply(lambda x: x.astype(str).tolist()).to_dict()
        # Convert posto_id keys to int
        employees_by_posto_dict = {int(k): v for k, v in employees_by_posto_dict.items()}

        return unit_id, secao_id, posto_id_list, employees_by_posto_dict, employees_id_total_list

    except Exception as e:
        logger.error(f"Error getting valid_emp info: {e}", exc_info=True)
        return "", 0, [], {}, []

def get_matriculas_for_employee_id(employee_id_list: List[int], employee_id_matriculas_map: Dict[int, str]) -> Tuple[bool, List[str], str]:
    """
    Function that gets a list with matriculas values for a list of employee_ids
    
    Args:
        employee_id_list: List of employee_id values
        employee_id_matriculas_map: Dictionary mapping employee_id to matricula
        
    Returns:
        Tuple[bool, List[str], str]: (success, matriculas_list, error_message)
    """
    try:
        logger.info(f"Getting matriculas for {len(employee_id_list)} employee_ids")
        
        # Check if all employee_ids exist in the mapping (vectorized check)
        missing_ids = set(employee_id_list) - set(employee_id_matriculas_map.keys())
        if missing_ids:
            error_msg = f"Employee IDs not found in mapping: {sorted(missing_ids)}"
            logger.error(error_msg)
            return False, [], error_msg
        
        # Build matriculas list using list comprehension (maintains order)
        matriculas_list = [employee_id_matriculas_map[emp_id] for emp_id in employee_id_list]
        
        logger.info(f"Successfully retrieved {len(matriculas_list)} matriculas")
        return True, matriculas_list, ""
        
    except Exception as e:
        error_msg = f"Error getting matriculas for employee_ids: {e}"
        logger.error(error_msg, exc_info=True)
        return False, [], error_msg

def get_employee_id_matriculas_map_dict(df_employee_id_matriculas: pd.DataFrame) -> Tuple[bool, Dict[int, str], str]:
    """
    Convert employee_id_matriculas_map DataFrame to a dictionary mapping employee_id to matricula.
    
    Args:
        df_employee_id_matriculas: DataFrame with columns 'employee_id' and 'matricula'
        
    Returns:
        Tuple[bool, Dict[int, str], str]: (success, employee_id_matriculas_dict, error_message)
    """
    try:
        # INPUT VALIDATION
        if df_employee_id_matriculas.empty:
            error_msg = "Input validation failed: DataFrame is empty"
            logger.error(error_msg)
            return False, {}, error_msg
            
        required_columns = ['employee_id', 'matricula']
        missing_columns = [col for col in required_columns if col not in df_employee_id_matriculas.columns]
        if missing_columns:
            error_msg = f"Input validation failed: missing required columns {missing_columns}"
            logger.error(error_msg)
            return False, {}, error_msg
        
        logger.info(f"Converting employee_id_matriculas_map DataFrame with {len(df_employee_id_matriculas)} rows to dictionary")
        
        # Check for duplicates in employee_id
        duplicates = df_employee_id_matriculas['employee_id'].duplicated()
        if duplicates.any():
            duplicate_ids = df_employee_id_matriculas[duplicates]['employee_id'].tolist()
            logger.warning(f"Found {duplicates.sum()} duplicate employee_ids: {duplicate_ids}. Using last occurrence.")
        
        # Convert to dictionary (int -> str mapping)
        employee_id_matriculas_dict = dict(
            zip(
                df_employee_id_matriculas['employee_id'].astype(int),
                df_employee_id_matriculas['matricula'].astype(str)
            )
        )
        
        # OUTPUT VALIDATION
        if not employee_id_matriculas_dict:
            error_msg = "Treatment failed: resulting dictionary is empty"
            logger.error(error_msg)
            return False, {}, error_msg
        
        logger.info(f"Successfully created employee_id_matriculas_dict with {len(employee_id_matriculas_dict)} entries")
        return True, employee_id_matriculas_dict, ""
        
    except Exception as e:
        error_msg = f"Error converting employee_id_matriculas_map to dictionary: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, {}, error_msg

def treat_df_faixa_secao_to_long(df_faixa_secao_wide: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Transform wide esc_faixa_horario (aber_seg, fech_seg, ... per weekday) to long format
    with one row per schedule_day and hora_inicio_faixa / hora_fim_faixa.

    Expects columns: fk_secao, aber_seg, fech_seg, aber_ter, fech_ter, ... aber_fer, fech_fer,
    and DATA_INI/DATA_FIM or data_ini/data_fim (date range to expand).

    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, df with schedule_day, hora_inicio_faixa, hora_fim_faixa [, fk_secao], error_message)
    """
    try:
        if df_faixa_secao_wide is None or df_faixa_secao_wide.empty:
            return False, pd.DataFrame(), "df_faixa_secao_wide is empty or None"

        df = df_faixa_secao_wide.copy()
        # Normalize column names (Oracle may return DATA_INI/DATA_FIM uppercase)
        df.columns = [c.lower() if isinstance(c, str) else c for c in df.columns]
        date_ini_col = 'data_ini' if 'data_ini' in df.columns else None
        date_fim_col = 'data_fim' if 'data_fim' in df.columns else None
        if date_ini_col is None or date_fim_col is None:
            return False, pd.DataFrame(), "df_faixa_secao_wide missing data_ini and/or data_fim columns"

        time_columns = [
            "aber_seg", "fech_seg", "aber_ter", "fech_ter", "aber_qua", "fech_qua",
            "aber_qui", "fech_qui", "aber_sex", "fech_sex", "aber_sab", "fech_sab",
            "aber_dom", "fech_dom", "aber_fer", "fech_fer"
        ]
        missing = [c for c in time_columns if c not in df.columns]
        if missing:
            return False, pd.DataFrame(), f"df_faixa_secao_wide missing time columns: {missing}"

        df[date_ini_col] = pd.to_datetime(df[date_ini_col])
        df[date_fim_col] = pd.to_datetime(df[date_fim_col])

        expanded_rows = []
        for _, row in df.iterrows():
            date_range = pd.date_range(start=row[date_ini_col], end=row[date_fim_col], freq='D')
            for date in date_range:
                new_row = row.copy()
                new_row['data'] = date
                expanded_rows.append(new_row)

        if not expanded_rows:
            return False, pd.DataFrame(), "No dates in data_ini..data_fim range"

        df_exp = pd.DataFrame(expanded_rows)
        id_vars = [c for c in ['fk_secao', 'data', date_ini_col, date_fim_col] if c in df_exp.columns]
        df_long = pd.melt(
            df_exp,
            id_vars=id_vars,
            value_vars=time_columns,
            var_name='wd_ab',
            value_name='value'
        )
        df_long[['a_f', 'wd']] = df_long['wd_ab'].str.split('_', expand=True)
        pivot_index = [c for c in id_vars if c in df_long.columns] + ['wd']
        df_wide = df_long.pivot_table(
            index=pivot_index,
            columns='a_f',
            values='value',
            aggfunc='first'
        ).reset_index()
        df_wide.columns.name = None

        # Match weekday from date to column (seg, ter, ...)
        df_wide['wd'] = df_wide['wd'].str.replace('sab', 'sáb')
        df_wide['wd_date'] = df_wide['data'].dt.day_name().str.lower()
        df_wide['wd_date'] = df_wide['wd_date'].str.replace('saturday', 'sáb')
        df_wide['wd_date'] = df_wide['wd_date'].str.replace('sunday', 'dom')
        df_wide['wd_date'] = df_wide['wd_date'].str.replace('monday', 'seg')
        df_wide['wd_date'] = df_wide['wd_date'].str.replace('tuesday', 'ter')
        df_wide['wd_date'] = df_wide['wd_date'].str.replace('wednesday', 'qua')
        df_wide['wd_date'] = df_wide['wd_date'].str.replace('thursday', 'qui')
        df_wide['wd_date'] = df_wide['wd_date'].str.replace('friday', 'sex')

        out = df_wide[df_wide['wd'] == df_wide['wd_date']].copy()
        out['schedule_day'] = pd.to_datetime(out['data']).dt.normalize()
        # Keep start/end as timestamps with date (raw from DB); same structure as df_orc_filtered
        out['hora_inicio_faixa'] = pd.to_datetime(out['aber'], errors='coerce')
        out['hora_fim_faixa'] = pd.to_datetime(out['fech'], errors='coerce')
        out = out.dropna(subset=['hora_inicio_faixa', 'hora_fim_faixa'])
        result_cols = ['schedule_day', 'hora_inicio_faixa', 'hora_fim_faixa']
        if 'fk_secao' in out.columns:
            result_cols.insert(0, 'fk_secao')
        result = out[result_cols].sort_values('schedule_day').reset_index(drop=True)
        logger.info(f"treat_df_faixa_secao_to_long: produced {len(result)} rows (long format)")
        return True, result, ""
    except Exception as e:
        logger.error(f"treat_df_faixa_secao_to_long failed: {e}", exc_info=True)
        return False, pd.DataFrame(), str(e)


def _fill_faixa_fallback(
    df_faixa: pd.DataFrame,
    missing_mask: pd.Series,
    unique_days: np.ndarray,
    df_faixa_secao: Optional[pd.DataFrame],
    log: Any,
) -> None:
    """Fill hora_inicio_faixa/hora_fim_faixa for rows where missing_mask is True. Use df_faixa_secao if provided and valid, else 06:40/22:40."""
    if not missing_mask.any():
        return
    required = ['schedule_day', 'hora_inicio_faixa', 'hora_fim_faixa']
    use_secao = (
        df_faixa_secao is not None
        and not df_faixa_secao.empty
        and all(c in df_faixa_secao.columns for c in required)
    )
    if use_secao:
        secao = df_faixa_secao.copy()
        secao['schedule_day'] = pd.to_datetime(secao['schedule_day']).dt.normalize()
        secao = secao.drop_duplicates(subset=['schedule_day'], keep='first')
        # Normalize to same structure as df_orc_filtered: schedule_day + time part of start/end
        t_ini = secao['hora_inicio_faixa']
        t_fim = secao['hora_fim_faixa']
        time_ini = pd.to_timedelta(
            t_ini.dt.hour * 3600 + t_ini.dt.minute * 60 + t_ini.dt.second, unit='s'
        )
        time_fim = pd.to_timedelta(
            t_fim.dt.hour * 3600 + t_fim.dt.minute * 60 + t_fim.dt.second, unit='s'
        )
        # When faixa spans past midnight (e.g. 06:00–00:00 next day), t_fim date > t_ini date.
        # Use schedule_day + 1 day as base for end so hora_fim_faixa is end-of-day (e.g. next day 00:00).
        fim_next_day = t_fim.dt.normalize() > t_ini.dt.normalize()
        secao = secao.assign(
            hora_inicio_faixa=secao['schedule_day'] + time_ini,
            hora_fim_faixa=np.where(
                fim_next_day,
                secao['schedule_day'] + pd.Timedelta(days=1) + time_fim,
                secao['schedule_day'] + time_fim,
            ),
        )
        df_faixa.loc[missing_mask, 'hora_inicio_faixa'] = df_faixa.loc[missing_mask, 'schedule_day'].map(
            secao.set_index('schedule_day')['hora_inicio_faixa']
        )
        df_faixa.loc[missing_mask, 'hora_fim_faixa'] = df_faixa.loc[missing_mask, 'schedule_day'].map(
            secao.set_index('schedule_day')['hora_fim_faixa']
        )
        still_missing = df_faixa['hora_inicio_faixa'].isna()
        if still_missing.any():
            log.info("get_df_faixa_horario: df_faixa_secao did not cover all days; filling remaining with 06:40/22:40")
            missing_mask = still_missing
        else:
            log.info("get_df_faixa_horario: used df_faixa_secao for fallback faixa times")
            return
    default_start = pd.Timedelta(hours=6, minutes=40)
    default_end = pd.Timedelta(hours=22, minutes=40)
    df_faixa.loc[missing_mask, 'hora_inicio_faixa'] = df_faixa.loc[missing_mask, 'schedule_day'] + default_start
    df_faixa.loc[missing_mask, 'hora_fim_faixa'] = df_faixa.loc[missing_mask, 'schedule_day'] + default_end


def get_df_faixa_horario(
    df_orcamento: pd.DataFrame,
    df_turnos: pd.DataFrame,
    use_case: int = 0,
    df_faixa_secao: Optional[pd.DataFrame] = None,
) -> Tuple[bool, pd.DataFrame, str]:
    """
    Function that gets the df_faixa_horario from the df_orcamento and df_turnos.

    For use_case 1, if there are no rows with pessoas_min > 0, fallback is used:
    df_faixa_secao (long format with schedule_day, hora_inicio_faixa, hora_fim_faixa)
    if provided and non-empty; otherwise hardcoded 06:40 / 22:40.

    Args:
        df_orcamento: DataFrame with the orcamento data
        df_turnos: DataFrame with the turnos data
        use_case: int with the use case
        df_faixa_secao: Optional long-format faixa per day (schedule_day, hora_inicio_faixa, hora_fim_faixa); used as fallback in use_case 1

    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, df_faixa_horario, error_message)
    """
    try:
        if use_case == 0:
            # Get limite_superior_manha and limite_inferior_tarde from df_turnos
            # Then create a DataFrame with one row per schedule_day
            # hora_inicio_faixa = 00:00, hora_fim_faixa = 24:00 (next day 00:00)
            
            required_cols = ['limite_superior_manha', 'limite_inferior_tarde']
            missing_cols = [col for col in required_cols if col not in df_turnos.columns]
            if missing_cols:
                raise ValueError(f"df_turnos missing required columns: {missing_cols}")
            
            if df_turnos.empty:
                raise ValueError("df_turnos is empty")
            
            # Parse limite times from df_turnos (get mode or average)
            limite_times = {}
            for col in required_cols:
                values = df_turnos[col].dropna()
                if values.empty:
                    raise ValueError(f"All values in '{col}' are null")
                
                # Vectorized time parsing: handle "HH:MM" or "HH:MM:SS" formats
                parsed_times = pd.to_datetime(values.astype(str), format='%H:%M', errors='coerce')
                mask_nat = parsed_times.isna()
                if mask_nat.any():
                    parsed_times_hms = pd.to_datetime(values[mask_nat].astype(str), format='%H:%M:%S', errors='coerce')
                    parsed_times = parsed_times.fillna(parsed_times_hms)
                
                mask_nat = parsed_times.isna()
                if mask_nat.any():
                    parsed_times_general = pd.to_datetime(values[mask_nat], errors='coerce')
                    parsed_times = parsed_times.fillna(parsed_times_general)
                
                parsed_times = parsed_times.dropna()
                if parsed_times.empty:
                    raise ValueError(f"Could not parse any time values from '{col}'")
                
                # Extract time as timedelta (hours + minutes + seconds)
                time_deltas = pd.to_timedelta(
                    parsed_times.dt.hour * 3600 + parsed_times.dt.minute * 60 + parsed_times.dt.second,
                    unit='s'
                )
                
                # Try to find mode (most common value)
                mode_result = time_deltas.mode()
                if len(mode_result) >= 1:
                    limite_times[col] = mode_result.iloc[0]
                    logger.info(f"get_df_faixa_horario: Using mode for {col}: {limite_times[col]}")
                else:
                    # No mode - use average
                    avg_seconds = time_deltas.dt.total_seconds().mean()
                    limite_times[col] = pd.Timedelta(seconds=avg_seconds)
                    logger.info(f"get_df_faixa_horario: No mode for {col}, using average: {limite_times[col]}")
            
            # Get unique schedule_day values from df_orcamento
            if df_orcamento is None or df_orcamento.empty:
                raise ValueError("df_orcamento is empty or None")
            
            df_orc = df_orcamento.copy()
            df_orc['schedule_day'] = pd.to_datetime(df_orc['schedule_day']).dt.normalize()
            unique_days = df_orc['schedule_day'].unique()
            
            # Create DataFrame with one row per schedule_day
            df_faixa = pd.DataFrame({'schedule_day': unique_days})
            df_faixa['schedule_day'] = pd.to_datetime(df_faixa['schedule_day']).dt.normalize()
            
            # hora_inicio_faixa = schedule_day at 00:00 (same as schedule_day normalized)
            df_faixa['hora_inicio_faixa'] = df_faixa['schedule_day']
            
            # hora_fim_faixa = schedule_day + 1 day at 00:00 (represents 24:00)
            df_faixa['hora_fim_faixa'] = df_faixa['schedule_day'] + pd.Timedelta(days=1)
            
            # limite_superior_manha = schedule_day + limite time
            df_faixa['limite_superior_manha'] = df_faixa['schedule_day'] + limite_times['limite_superior_manha']
            
            # limite_inferior_tarde = schedule_day + limite time
            df_faixa['limite_inferior_tarde'] = df_faixa['schedule_day'] + limite_times['limite_inferior_tarde']
            
            # Sort by schedule_day
            df_faixa = df_faixa.sort_values('schedule_day').reset_index(drop=True)
            
            logger.info(f"get_df_faixa_horario: Created df_faixa with {len(df_faixa)} rows")
            return True, df_faixa, ""
        
        elif use_case == 1:
            df_orc_full = df_orcamento.copy()
            df_orc_full['schedule_day'] = pd.to_datetime(df_orc_full['schedule_day']).dt.normalize()
            unique_days = df_orc_full['schedule_day'].unique()
            mask_pessoas_min = df_orc_full['pessoas_min'] > 0
            df_orc_filtered = df_orc_full[mask_pessoas_min]

            if not df_orc_filtered.empty:
                df_faixa = df_orc_filtered.groupby('schedule_day', as_index=False).agg(
                    hora_inicio_faixa=('hora_ini', 'min'),
                    hora_fim_faixa=('hora_ini', 'max')
                )
                # Ensure all days from df_orcamento are present; fill missing from df_faixa_secao or default
                df_days = pd.DataFrame({'schedule_day': unique_days})
                df_days['schedule_day'] = pd.to_datetime(df_days['schedule_day']).dt.normalize()
                df_faixa = df_days.merge(df_faixa, on='schedule_day', how='left')
                missing = df_faixa['hora_inicio_faixa'].isna()
                if missing.any():
                    _fill_faixa_fallback(df_faixa, missing, unique_days, df_faixa_secao, logger)
            else:
                # No rows with pessoas_min > 0: use df_faixa_secao fallback or hardcoded 06:40 / 22:40
                logger.warning(
                    "get_df_faixa_horario (use_case=1): no rows with pessoas_min > 0 "
                    "(total rows: %d). Using fallback (df_faixa_secao or 06:40/22:40).",
                    len(df_orc_full),
                )
                df_faixa = pd.DataFrame({'schedule_day': unique_days})
                df_faixa['schedule_day'] = pd.to_datetime(df_faixa['schedule_day']).dt.normalize()
                _fill_faixa_fallback(df_faixa, pd.Series(True, index=df_faixa.index), unique_days, df_faixa_secao, logger)

            # Calculate ponto_medio and limites
            df_faixa['ponto_medio'] = df_faixa['hora_inicio_faixa'] + (df_faixa['hora_fim_faixa'] - df_faixa['hora_inicio_faixa']) / 2
            df_faixa['limite_superior_manha'] = df_faixa['ponto_medio']
            df_faixa['limite_inferior_tarde'] = df_faixa['ponto_medio']
            df_faixa = df_faixa.sort_values('schedule_day').reset_index(drop=True)
            return True, df_faixa, ""

        else:
            return False, pd.DataFrame(), "Invalid use case"
    except Exception as e:
        return False, pd.DataFrame(), f"Error getting df_faixa_horario: {e}"

def get_df_estrutura_wfm_info(df_estrutura_wfm: pd.DataFrame) -> str:
    """
    """
    nome_pais = str(df_estrutura_wfm['nome_pais'].unique()[0])

    return nome_pais

def filter_insert_results(df: pd.DataFrame, start_date: str, end_date: str, wfm_proc_colab: str = '') -> pd.DataFrame:
    """
    Filter the results dataframe by date range and optionally by a specific employee.
    
    This function is called before insertion to ensure we only insert data within the 
    process date range and for the specific employee if one is specified.
    
    Args:
        df: DataFrame containing the results to be filtered
        start_date: Start date of the process (YYYY-MM-DD format)
        end_date: End date of the process (YYYY-MM-DD format)
        wfm_proc_colab: Employee ID to filter by. If empty string, no employee filtering is applied.
        
    Returns:
        pd.DataFrame: Filtered dataframe
    """
    try:
        if df.empty:
            logger.warning("filter_insert_results: Input dataframe is empty")
            return df
        
        initial_rows = len(df)
        logger.info(f"filter_insert_results: Starting with {initial_rows} rows")
        
        # Convert start_date and end_date to datetime
        start_dt = pd.to_datetime(start_date, format='%Y-%m-%d')
        end_dt = pd.to_datetime(end_date, format='%Y-%m-%d')
        logger.info(f"Filtering by date range: {start_date} to {end_date}")
        
        # Identify the date column (could be 'data' or 'date')
        date_col = None
        if 'data' in df.columns:
            date_col = 'data'
        elif 'date' in df.columns:
            date_col = 'date'
        else:
            logger.error("filter_insert_results: No date column found (expected 'data' or 'date')")
            return df
        
        # Convert date column to datetime if not already
        df[date_col] = pd.to_datetime(df[date_col])
        
        # Filter by date range
        df_filtered = df[(df[date_col] >= start_dt) & (df[date_col] <= end_dt)].copy()
        rows_after_date_filter = len(df_filtered)
        logger.info(f"After date range filter: {rows_after_date_filter} rows (removed {initial_rows - rows_after_date_filter} rows)")
        
        # Filter by employee if wfm_proc_colab is specified
        if wfm_proc_colab and wfm_proc_colab != '':
            # Identify the employee column (could be 'colaborador', 'employee_id', or 'matricula')
            employee_col = None
            if 'colaborador' in df_filtered.columns:
                employee_col = 'colaborador'
            elif 'employee_id' in df_filtered.columns:
                employee_col = 'employee_id'
            elif 'matricula' in df_filtered.columns:
                employee_col = 'matricula'
            else:
                logger.warning("filter_insert_results: No employee column found, skipping employee filter")
                return df_filtered
            
            logger.info(f"Filtering by employee: {wfm_proc_colab} (using column '{employee_col}')")
            
            # Convert to string for comparison
            df_filtered[employee_col] = df_filtered[employee_col].astype(str)
            wfm_proc_colab_str = str(wfm_proc_colab)
            
            df_filtered = df_filtered[df_filtered[employee_col] == wfm_proc_colab_str].copy()
            rows_after_employee_filter = len(df_filtered)
            logger.info(f"After employee filter: {rows_after_employee_filter} rows (removed {rows_after_date_filter - rows_after_employee_filter} rows)")
        
        logger.info(f"filter_insert_results: Final result has {len(df_filtered)} rows (removed {initial_rows - len(df_filtered)} total rows)")
        return df_filtered
        
    except Exception as e:
        logger.error(f"Error in filter_insert_results: {str(e)}", exc_info=True)
        return df


# ──────────────────────────────────────────────────────────────────────────────
# ECI Unit Helper Functions
# ──────────────────────────────────────────────────────────────────────────────

def is_eci_unit(df_estrutura_wfm: pd.DataFrame) -> bool:
    """
    Check if the current unit is an ECI unit based on the unit name.
    
    ECI units are identified by their name starting with "ECI".
    
    Args:
        df_estrutura_wfm: DataFrame with organizational structure (must contain 'nome_unidade')
        
    Returns:
        bool: True if this is an ECI unit, False otherwise
    """
    try:
        if df_estrutura_wfm is None or df_estrutura_wfm.empty:
            return False
        if 'nome_unidade' not in df_estrutura_wfm.columns:
            logger.warning("is_eci_unit: 'nome_unidade' column not found in df_estrutura_wfm")
            return False
        nome_unidade = str(df_estrutura_wfm['nome_unidade'].iloc[0]).strip().upper()
        return nome_unidade.startswith("ECI")
    except Exception as e:
        logger.error(f"Error in is_eci_unit: {str(e)}", exc_info=True)
        return False


def get_sibling_section_name(nome_secao: str) -> Optional[str]:
    """
    Get the sibling section name for ECI units.
    
    ECI units operate in pairs: if the current section contains "WOMAN", the sibling
    is "MAN", and vice versa.
    
    Note: "WOMAN" must be checked before "MAN" because "WOMAN" contains "MAN".
    
    Args:
        nome_secao: Current section name (e.g. "SHOP OPERATION MAN", "SHOP OPERATION WOMAN")
        
    Returns:
        Optional[str]: The sibling section keyword ("MAN" or "WOMAN"), or None if not identifiable
    """
    try:
        nome_upper = str(nome_secao).strip().upper()
        # WOMAN must be checked first because "WOMAN" contains "MAN"
        if "WOMAN" in nome_upper:
            return "MAN"
        elif "MAN" in nome_upper:
            return "WOMAN"
        else:
            logger.warning(f"get_sibling_section_name: Could not identify sibling for section '{nome_secao}'")
            return None
    except Exception as e:
        logger.error(f"Error in get_sibling_section_name: {str(e)}", exc_info=True)
        return None