"""File containing the helper functions for the DescansosDataModel"""

# Dependencies
import pandas as pd
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
        ('T', 'M'): 'M', ('T', 'T'): 'T', ('T', 'H'): 'MoT', ('T', 'P'): 'P',
        ('F', None): 'L', ('F', 'D'): 'LD', ('F', 'Q'): 'LQ', ('F', 'C'): 'C',
        ('R', None): 'F', ('N', None): '-', ('T', 'A'): 'V'
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


def treat_valid_emp(df_valid_emp: pd.DataFrame) -> pd.DataFrame:
    """
    Treat valid_emp dataframe.
    """
    try:
        df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].fillna(0.0)
        df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].astype(int)
        df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].astype(str)
        
        # Convert prioridade_folgas values: '1' -> 'manager', '2' -> 'keyholder'
        df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].replace({
            '1': 'manager',
            '2': 'keyholder',
            '1.0': 'manager',
            '2.0': 'keyholder',
            '0': 'normal'
        })
        df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].fillna('')
        logger.info(f"valid_emp:\n{df_valid_emp}")
        return df_valid_emp
    except Exception as e:
        logger.error(f"Error in helper function treat_valid_emp: {str(e)}")
        return pd.DataFrame()

def treat_df_closed_days(df_closed_days: pd.DataFrame, start_date2: pd.Timestamp, end_date2: pd.Timestamp) -> Tuple[pd.DataFrame, str]:
    """
    Treat df_closed_days dataframe.
    """
    try:

        logger.info(f"Treating df_closed_days")
        if len(df_closed_days) > 0:
            logger.info(f"df_closed_days has more than 0 rows")
            df_closed_days = (df_closed_days
                    .assign(data=pd.to_datetime(df_closed_days['data'].dt.strftime('%Y-%m-%d')))
                    .query('(data >= @start_date2 and data <= @end_date2) or data < "2000-12-31"')
                    .assign(data=lambda x: x['data'].apply(lambda d: d.replace(year=start_date2.year)))
                    [['data']]
                    .drop_duplicates())
        return df_closed_days, ""


    except Exception as e:
        logger.error(f"Error in helper function treat_df_closed_days: {str(e)}", exc_info=True)
        return pd.DataFrame(), str(e)


def create_df_calendario(start_date: str, end_date: str, employee_id_matriculas_map: Dict[str, str]) -> pd.DataFrame:
    """
    Create df_calendario dataframe with employee schedules for the specified date range using vectorized operations.
    
    Args:
        start_date: Start date as string (YYYY-MM-DD format)
        end_date: End date as string (YYYY-MM-DD format)
        employee_id_matriculas_map: Dictionary mapping employee_ids to matriculas
        
    Returns:
        DataFrame with columns: employee_id, data, tipo_turno, horario, wday, dia_tipo, matricula, data_admissao, data_demissao
    """
    try:
        logger.info(f"Creating df_calendario from {start_date} to {end_date} for {len(employee_id_matriculas_map)} employees")
        
        # Convert input strings to date format
        start_dt = pd.to_datetime(start_date, format='%Y-%m-%d')
        end_dt = pd.to_datetime(end_date, format='%Y-%m-%d')
        
        # Generate sequence of dates
        date_range = pd.date_range(start=start_dt, end=end_dt, freq='D')
        
        # Create employee DataFrame
        employees_df = pd.DataFrame(list(employee_id_matriculas_map.items()), 
                                  columns=['employee_id', 'matricula'])
        
        # Create dates DataFrame with weekday calculation
        dates_df = pd.DataFrame({
            'data': date_range,
            'wday': date_range.weekday + 1  # Convert to 1-7 (Monday-Sunday)
        })
        
        # Create shifts DataFrame
        shifts_df = pd.DataFrame({'tipo_turno': ['M', 'T']})
        
        # Create cartesian product using cross merge
        # First: employees × dates
        emp_dates = employees_df.assign(key=1).merge(dates_df.assign(key=1), on='key').drop('key', axis=1)
        
        # Second: (employees × dates) × shifts
        df_calendario = emp_dates.assign(key=1).merge(shifts_df.assign(key=1), on='key').drop('key', axis=1)
        
        # Vectorized operations for final formatting
        df_calendario['employee_id'] = df_calendario['employee_id'].astype(str)
        df_calendario['matricula'] = df_calendario['matricula'].astype(str)
        df_calendario['data'] = df_calendario['data'].dt.strftime('%Y-%m-%d')
        
        # Add empty columns
        df_calendario['horario'] = ''
        df_calendario['dia_tipo'] = ''
        df_calendario['data_admissao'] = ''
        df_calendario['data_demissao'] = ''
        
        # Reorder columns
        column_order = ['employee_id', 'data', 'tipo_turno', 'horario', 'wday', 'dia_tipo', 'matricula', 'data_admissao', 'data_demissao']
        df_calendario = df_calendario[column_order]
        
        # Sort by employee_id, date, and shift type for consistent ordering
        df_calendario = df_calendario.sort_values(['employee_id', 'data', 'tipo_turno']).reset_index(drop=True)
        
        logger.info(f"Created df_calendario with {len(df_calendario)} rows ({len(employee_id_matriculas_map)} employees × {len(date_range)} days × 2 shifts)")
        
        return df_calendario
        
    except Exception as e:
        logger.error(f"Error in helper function create_df_calendario: {str(e)}", exc_info=True)
        return pd.DataFrame()


def add_calendario_passado(df_calendario: pd.DataFrame, df_calendario_passado: pd.DataFrame) -> pd.DataFrame:
    """
    Add df_calendario_passado to df_calendario.
    """
    try:
        logger.info(f"Adding df_calendario_passado to df_calendario. Not implemented yet.")
        return df_calendario
    except Exception as e:
        logger.error(f"Error in helper function add_calendario_passado: {str(e)}", exc_info=True)
        return pd.DataFrame()

def add_ausencias_ferias(df_calendario: pd.DataFrame, df_ausencias_ferias: pd.DataFrame) -> pd.DataFrame:
    """
    Add df_ausencias_ferias to df_calendario.
    """
    try:
        logger.info(f"Adding df_ausencias_ferias to df_calendario. Not implemented yet.")
        return df_calendario
    except Exception as e:
        logger.error(f"Error in helper function add_ausencias_ferias: {str(e)}", exc_info=True)
        return pd.DataFrame()

def add_folgas_ciclos(df_calendario: pd.DataFrame, df_core_pro_emp_horario_det: pd.DataFrame) -> pd.DataFrame:
    """
    Add df_core_pro_emp_horario_det to df_calendario.
    """
    try:
        logger.info(f"Adding df_core_pro_emp_horario_det to df_calendario. Not implemented yet.")
        return df_calendario
    except Exception as e:
        logger.error(f"Error in helper function add_folgas_ciclos: {str(e)}", exc_info=True)
        return pd.DataFrame()

def add_ciclos_90(df_calendario: pd.DataFrame, df_ciclos_90: pd.DataFrame) -> pd.DataFrame:
    """
    Add df_ciclos_90 to df_calendario.
    """
    try:
        logger.info(f"Adding df_ciclos_90 to df_calendario. Not implemented yet.")
        return df_calendario
    except Exception as e:
        logger.error(f"Error in helper function add_ciclos_90: {str(e)}", exc_info=True)
        return pd.DataFrame()

def add_days_off(df_calendario: pd.DataFrame, df_days_off: pd.DataFrame) -> pd.DataFrame:
    """
    Add df_days_off to df_calendario.
    """
    try:
        logger.info(f"Adding df_days_off to df_calendario. Not implemented yet.")
        return df_calendario
    except Exception as e:
        logger.error(f"Error in helper function add_days_off: {str(e)}", exc_info=True)
        return pd.DataFrame()