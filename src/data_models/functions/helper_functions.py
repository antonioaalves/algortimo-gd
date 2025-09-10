"""File containing the helper functions for the DescansosDataModel"""

# Dependencies
import pandas as pd
import datetime as dt
from typing import List, Tuple, Dict
from base_data_project.log_config import get_logger

# Local stuff
from src.config import PROJECT_NAME

# Set up logger
logger = get_logger(PROJECT_NAME)


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
    if unit_id is not None:
        unit_condition = (
            (df_filtered['fk_tipo_posto'].isna()) & 
            (df_filtered['fk_secao'].isna()) & 
            (df_filtered['fk_unidade'] == unit_id)
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
    """Get the actual value from CHARVALUE, NUMBERVALUE, or DATEVALUE"""
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
    Convert R load_WFM_scheds function to Python - simplified version.
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
    """Convert WFM types to TRADS - simple mapping."""
    type_map = {
        ('T', 'M'): 'M', 
        ('T', 'T'): 'T', 
        ('T', 'H'): 'MoT', 
        ('T', 'P'): 'P',
        ('F', None): 'L', 
        ('F', 'D'): 'LD', 
        ('F', 'Q'): 'LQ',
        ('F', 'C'): 'C',
        ('R', None): 'F', 
        ('N', None): '-', 
        ('T', 'A'): 'V',
    }
    
    df['sched_subtype'] = df.apply(
        lambda row: type_map.get((row.get('type'), row.get('subtype')), '-'), axis=1
    )
    df['ind'] = 'P'
    return df

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

def get_first_and_last_day_passado(start_date_str: str, end_date_str: str, main_year: str, wfm_proc_colab: str) -> Tuple[str, str]:
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
        Tuple[str, str]: A tuple containing (first_day_passado, last_day_passado) in 'YYYY-MM-DD' format.
                        Returns ('', '') if an error occurs.
                        
    Business Logic Cases:
        CASE 1: start=01-01, end=31-12, wfm='' -> (Monday of prev week, day before start)
        CASE 2: start>01-01, end=31-12, wfm='' -> (01-01, day before start)
        CASE 3: start=01-01, end<31-12, wfm='' -> (Monday of prev week, day before start)
        CASE 4: start>01-01, end<31-12, wfm='' -> (01-01, 31-12)
        CASE 5: start=01-01, end=31-12, wfm!='' -> (Monday of prev week, Sunday of next week)
        CASE 6: start>01-01, end=31-12, wfm!='' -> (01-01, Sunday of next week)
        CASE 7: start=01-01, end<31-12, wfm!='' -> (Monday of prev week, Sunday of next week)
        CASE 8: start>01-01, end<31-12, wfm!='' -> (01-01, 31-12)
        
    Raises:
        Logs error and returns empty strings if date parsing fails or invalid date ranges provided.
    """
    try:
        start_date_dt = pd.to_datetime(start_date_str, format='%Y-%m-%d')
        end_date_dt = pd.to_datetime(end_date_str, format='%Y-%m-%d')

        first_january = pd.to_datetime(f'{main_year}-01-01', format='%Y-%m-%d')
        last_december = pd.to_datetime(f'{main_year}-12-31', format='%Y-%m-%d')

        # CASO 1: start_date = 01-01 e end_date = 31-12 e wfm_proc_colab = ''
        if start_date_dt == first_january and end_date_dt == last_december and wfm_proc_colab == '':
            first_day_passado_str = get_monday_of_previous_week(start_date_str)
            last_day_passado_str = (start_date_dt - dt.timedelta(days=1)).strftime('%Y-%m-%d')

        # CASO 2: start_date > 01-01 e end_date = 31-12 e wfm_proc_colab = ''
        elif start_date_dt > first_january and end_date_dt == last_december and wfm_proc_colab == '':
            first_day_passado_str = first_january.strftime('%Y-%m-%d')
            last_day_passado_str = (start_date_dt - dt.timedelta(days=1)).strftime('%Y-%m-%d')

        # CASO 3: start_date = 01-01 e end_date < 31-12 e wfm_proc_colab = ''
        elif start_date_dt == first_january and end_date_dt < last_december and wfm_proc_colab == '':
            first_day_passado_str = get_monday_of_previous_week(start_date_str)
            last_day_passado_str = (start_date_dt - dt.timedelta(days=1)).strftime('%Y-%m-%d')

        # CASO 4: start_date > 01-01 e end_date < 31-12 e wfm_proc_colab = ''
        elif start_date_dt > first_january and end_date_dt < last_december and wfm_proc_colab == '':
            first_day_passado_str = first_january.strftime('%Y-%m-%d')
            last_day_passado_str = last_december.strftime('%Y-%m-%d')

        # CASO 5: start_date = 01-01 e end_date = 31-12 e wfm_proc_colab != ''
        elif start_date_dt == first_january and end_date_dt == last_december and wfm_proc_colab != '':
            first_day_passado_str = get_monday_of_previous_week(start_date_str)
            last_day_passado_str = get_sunday_of_next_week(end_date_str)

        # CASO 6: start_date > 01-01 e end_date = 31-12 e wfm_proc_colab != ''
        elif start_date_dt > first_january and end_date_dt == last_december and wfm_proc_colab != '':
            first_day_passado_str = first_january.strftime('%Y-%m-%d')
            last_day_passado_str = get_sunday_of_next_week(end_date_str)

        # CASO 7: start_date = 01-01 e end_date < 31-12 e wfm_proc_colab != ''
        elif start_date_dt == first_january and end_date_dt < last_december and wfm_proc_colab != '':
            first_day_passado_str = get_monday_of_previous_week(start_date_str)
            last_day_passado_str = get_sunday_of_next_week(end_date_str)

        # CASO 8: start_date > 01-01 e end_date < 31-12 e wfm_proc_colab != ''
        elif start_date_dt > first_january and end_date_dt < last_december and wfm_proc_colab != '':
            first_day_passado_str = first_january.strftime('%Y-%m-%d')
            last_day_passado_str = last_december.strftime('%Y-%m-%d')

        # No other cases are predicted
        else:
            logger.error(f"start_date {start_date_str} and end_date {end_date_str} are not compatible with the programed logic")
            return '', ''

        return first_day_passado_str, last_day_passado_str
    except Exception as e:
        logger.error(f"Error in get_first_and_last_day_passado: {str(e)}")
        return '', ''

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