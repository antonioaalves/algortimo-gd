"""
File containing the data treatment/dataframe manipulation functions for the DescansosDataModel.
Data treatment functions:
- treat_valid_emp
- treat_df_closed_days
- treat_calendario_passado
- create_df_calendario

Dataframe manipulation functions:
- add_calendario_passado
- add_ausencias_ferias
- add_folgas_ciclos
- add_ciclos_90
- add_days_off
"""

# Dependencies
import pandas as pd
from typing import List, Tuple, Dict
from base_data_project.log_config import get_logger

# Local stuff
from src.config import PROJECT_NAME
from src.data_models.functions.helper_functions import convert_types_in

# Set up logger
logger = get_logger(PROJECT_NAME)

def treat_df_valid_emp(df_valid_emp: pd.DataFrame) -> pd.DataFrame:
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

def treat_df_calendario_passado(df_calendario_passado: pd.DataFrame) -> pd.DataFrame:
    """
    Treat df_calendario_passado dataframe.
    Convert types in df_calendario_passado.
    Filter df_calendario_passado by date logic.
    Arguments:
        df_calendario_passado: pd.DataFrame
    Returns:
        pd.DataFrame
    """
    try:
        logger.info(f"Treating df_calendario_passado")
        return df_calendario_passado
    except Exception as e:
        logger.error(f"Error in helper function treat_calendario_passado: {str(e)}", exc_info=True)
        return pd.DataFrame()

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