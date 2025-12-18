"""
File containing the data treatment/dataframe manipulation functions for the DescansosDataModel.
Data treatment functions:
- treat_valid_emp
- treat_df_feriados
- treat_df_closed_days
- treat_df_contratos
- treat_calendario_passado
- treat_employee_id_matriculas_map
- create_df_calendario
- adjust_estimativas_special_days
- filter_df_dates
- extract_tipos_turno
- process_special_shift_types
- add_date_related_columns
- define_dia_tipo
- merge_contract_data
- adjust_counters_for_contract_types
- handle_employee_edge_cases
- adjust_horario_for_admission_date
- calculate_and_merge_allocated_employees

Dataframe manipulation functions:
- add_calendario_passado
- add_ausencias_ferias
- add_folgas_ciclos
- add_ciclos_90
- add_days_off
"""

# Dependencies
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict
from base_data_project.log_config import get_logger

# Local stuff
from src.configuration_manager.instance import get_config
from src.data_models.functions.helper_functions import (
    convert_types_in,
    convert_ciclos_to_horario,
    adjusted_isoweek, 
    get_monday_of_previous_week, 
    get_sunday_of_next_week,
    get_week_pattern,
)

# Get configuration singleton
_config = get_config()
PROJECT_NAME = _config.project_name
from src.data_models.validations.load_process_data_validations import (
    validate_df_colaborador
)
from src.helpers import count_open_holidays

# Set up logger
logger = get_logger(PROJECT_NAME)

def separate_df_ciclos_completos_folgas_ciclos(df_ciclos_completos_folgas_ciclos: pd.DataFrame, employees_id_90_list: list[int]) -> Tuple[bool, pd.DataFrame, pd.DataFrame, str]:
    """
    Separate employee cycle data into complete cycles and fixed day-off cycles.
    
    This function partitions the combined cycles/day-offs dataframe into two distinct dataframes:
    - Complete cycles (CICLO COMPLETO): For employees with 90-day rotation schedules
    - Fixed day-off cycles: For all other employees with predefined rest days
    
    Business Context:
        Employees with 90-day complete cycles have their schedules defined differently
        than employees with fixed weekly patterns. This separation enables distinct
        processing logic for each group.
    
    Args:
        df_ciclos_completos_folgas_ciclos: Combined dataframe containing both cycle types
        employees_id_90_list: List of employee IDs who have 90-day complete cycles
        
    Returns:
        Tuple containing:
            - success (bool): True if separation succeeded, False otherwise
            - df_ciclos_completos (pd.DataFrame): Employees with complete 90-day cycles
            - df_folgas_ciclos (pd.DataFrame): Employees with fixed day-off patterns
            - error_message (str): Error description if operation failed, empty string otherwise
            
    Example:
        >>> success, df_complete, df_fixed, err = separate_df_ciclos_completos_folgas_ciclos(
        ...     df_combined, [101, 102, 103]
        ... )
    """
    try:
        # Convert employee_id to string for consistent comparison (comes as int from SQL, but list is strings)
        df_ciclos_completos_folgas_ciclos['employee_id'] = df_ciclos_completos_folgas_ciclos['employee_id'].astype(str)
        
        # mask to separate df_ciclos_completos and df_folgas_ciclos
        mask = df_ciclos_completos_folgas_ciclos['employee_id'].isin(employees_id_90_list)

        # df_ciclos_completos is the dataframe of the employees with CICLO COMPLETO
        df_ciclos_completos = df_ciclos_completos_folgas_ciclos[mask]
        
        # df_folgas_ciclos has the fixed days for all employees
        df_folgas_ciclos = df_ciclos_completos_folgas_ciclos[~mask]

        # Log the dataframe structures
        logger.info(f"df_ciclos_completos shape: {df_ciclos_completos.shape}")
        logger.info(f"df_folgas_ciclos shape: {df_folgas_ciclos.shape}")

        return True, df_ciclos_completos, df_folgas_ciclos, ""
    except Exception as e:
        logger.error(f"Error in get_df_ciclos_completos: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), pd.DataFrame(), str(e)

def treat_df_valid_emp(df_valid_emp: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Process and standardize employee priority data for day-off assignment logic.
    
    This function transforms numeric priority codes into categorical labels that
    drive day-off allocation decisions in the scheduling algorithm. Priority affects
    the order in which employees receive preferred rest days.
    
    Business Logic:
        - Priority 1 → 'manager': Highest priority for day-off preferences
        - Priority 2 → 'keyholder': Medium priority for key-holding staff
        - Priority 0 or null → 'normal': Standard priority for all other employees
    
    Data Quality:
        - Handles missing values by defaulting to priority 0
        - Converts numeric/float values to standardized string labels
        - Validates input structure before processing
    
    Args:
        df_valid_emp: DataFrame containing employee data with 'prioridade_folgas' column
        
    Returns:
        Tuple containing:
            - success (bool): True if treatment succeeded, False otherwise
            - df_valid_emp (pd.DataFrame): Processed dataframe with categorized priorities
            - error_message (str): Error description if operation failed, empty string otherwise
            
    Raises:
        Logs warnings if priority conversion fails, falling back to default values
    """
    try:
        # INPUT VALIDATION
        if df_valid_emp.empty:
            return False, pd.DataFrame(), "Input validation failed: empty DataFrame"
        
        if 'prioridade_folgas' not in df_valid_emp.columns:
            return False, pd.DataFrame(), "Input validation failed: missing prioridade_folgas column"
        
        # EARLY TYPE CONVERSION FOR IDENTIFIERS
        # Convert employee_id to string to match df_colaborador type for later merges
        df_valid_emp['employee_id'] = df_valid_emp['employee_id'].astype(str)
            
        # TREATMENT LOGIC
        try:
            df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].fillna(0.0)
            df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].astype(int)
            df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].astype(str)
        except (ValueError, TypeError) as e:
            logger.warning(f"Priority conversion failed, using defaults: {e}")
            df_valid_emp['prioridade_folgas'] = '0'
        
        # Convert prioridade_folgas values: '1' -> 'manager', '2' -> 'keyholder'
        df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].replace({
            '1': 'manager',
            '2': 'keyholder',
            '1.0': 'manager',
            '2.0': 'keyholder',
            '0': 'normal'
        })
        df_valid_emp['prioridade_folgas'] = df_valid_emp['prioridade_folgas'].fillna('')
        
        # OUTPUT VALIDATION
        if df_valid_emp.empty:
            return False, pd.DataFrame(), "Treatment resulted in empty DataFrame"
            
        logger.info(f"valid_emp:\n{df_valid_emp}")
        return True, df_valid_emp, ""
        
    except Exception as e:
        logger.error(f"Error in treat_df_valid_emp: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def treat_df_closed_days(df_closed_days: pd.DataFrame, start_date2: pd.Timestamp, end_date2: pd.Timestamp) -> Tuple[bool, pd.DataFrame, str]:
    """
    Filter and normalize closed business days within the scheduling period.
    
    This function processes store closure dates (holidays, special closures) to ensure
    they align with the scheduling period. It handles recurring annual events by
    adjusting their year to match the current planning period.
    
    Business Logic:
        - Filters dates to match the execution period [start_date2, end_date2]
        - Handles recurring holidays (dates before 2001) by updating to current year
        - Removes duplicate closure dates
        - Returns empty DataFrame if no closures exist (valid scenario)
    
    Use Cases:
        - Store-wide holidays (Christmas, New Year, etc.)
        - Regional closure days
        - Special business closure dates
    
    Args:
        df_closed_days: DataFrame with 'data' column containing closure dates
        start_date2: Beginning of the scheduling period
        end_date2: End of the scheduling period
        
    Returns:
        Tuple containing:
            - success (bool): True if treatment succeeded, False otherwise
            - df_closed_days (pd.DataFrame): Filtered closure dates within period
            - error_message (str): Error description if operation failed, empty string otherwise
    """
    try:
        # INPUT VALIDATION
        if start_date2 is None or end_date2 is None:
            return False, pd.DataFrame(), "Input validation failed: invalid date parameters"
            
        if not df_closed_days.empty and 'data' not in df_closed_days.columns:
            return False, pd.DataFrame(), "Input validation failed: missing data column"

        # TREATMENT LOGIC
        logger.info(f"Treating df_closed_days")
        if len(df_closed_days) > 0:
            logger.info(f"df_closed_days has more than 0 rows")
            try:
                df_closed_days = (df_closed_days
                        .assign(data=pd.to_datetime(df_closed_days['data'].dt.strftime('%Y-%m-%d')))
                        .query('(data >= @start_date2 and data <= @end_date2) or data < "2000-12-31"')
                        .assign(data=lambda x: x['data'].apply(lambda d: d.replace(year=start_date2.year)))
                        [['data']]
                        .drop_duplicates())
            except (ValueError, KeyError) as e:
                logger.warning(f"Date processing failed: {e}")
                return False, pd.DataFrame(), "Date processing failed"
                
        # OUTPUT VALIDATION - Allow empty DataFrame as valid result
        return True, df_closed_days, ""

    except Exception as e:
        logger.error(f"Error in treat_df_closed_days: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def treat_df_feriados(df_feriados: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Treat df_feriados dataframe by renaming columns and handling duplicates.
    
    Oracle table functions sometimes return 'database' instead of 'schedule_day' alias.
    Handles duplicate schedule_day by keeping rows with most specific location.
    Priority: fk_unidade > fk_cidade > fk_estado > fk_pais
    """
    try:
        # INPUT VALIDATION
        if df_feriados.empty:
            return False, pd.DataFrame(), "Input validation failed: empty DataFrame"
        
        # COLUMN RENAMING
        # Oracle may return 'database' instead of 'schedule_day' alias
        if 'database' in df_feriados.columns and 'schedule_day' not in df_feriados.columns:
            df_feriados = df_feriados.rename(columns={'database': 'schedule_day'})
        
        # Ensure schedule_day exists
        if 'schedule_day' not in df_feriados.columns:
            return False, pd.DataFrame(), "Input validation failed: missing date column (expected 'schedule_day' or 'database')"
        
        # Convert schedule_day to datetime
        df_feriados['schedule_day'] = pd.to_datetime(df_feriados['schedule_day'])
        
        # DUPLICATE HANDLING
        # Create priority column: lower number = higher priority
        # 1 = has fk_unidade, 2 = has fk_cidade, 3 = has fk_estado, 4 = has fk_pais, 5 = none
        df_feriados['_priority'] = 5
        df_feriados.loc[df_feriados['fk_pais'].notna(), '_priority'] = 4
        df_feriados.loc[df_feriados['fk_estado'].notna(), '_priority'] = 3
        df_feriados.loc[df_feriados['fk_cidade'].notna(), '_priority'] = 2
        df_feriados.loc[df_feriados['fk_unidade'].notna(), '_priority'] = 1
        
        # Sort by schedule_day and priority (ascending = most specific first)
        df_feriados = df_feriados.sort_values(
            by=['schedule_day', '_priority'],
            ascending=[True, True]
        )
        
        # Drop duplicates keeping the first occurrence (most specific location)
        df_feriados = df_feriados.drop_duplicates(
            subset=['schedule_day'],
            keep='first'
        )
        
        # Remove temporary priority column and reset index
        df_feriados = df_feriados.drop(columns=['_priority']).reset_index(drop=True)
        
        return True, df_feriados, ""
        
    except Exception as e:
        logger.error(f"Error in treat_df_feriados: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Error processing feriados: {str(e)}"

def treat_df_calendario_passado(df_calendario_passado: pd.DataFrame, case_type: int, wfm_proc_colab: str, first_date_passado: str, last_date_passado: str, start_date: str, end_date: str) -> Tuple[bool, pd.DataFrame, str]:
    """
    Process historical calendar data for carryover into current scheduling period.
    
    This function prepares past schedule data (already executed shifts) for integration
    into the current planning period. Historical data provides continuity for ongoing
    cycles and ensures accurate tracking of employee work patterns.
    
    Processing Steps:
        1. Validates date range parameters
        2. Converts data types to match algorithm requirements
        3. Renames 'schedule_day' to 'data' for consistency
        4. Removes temporary columns (data_dt, type, subtype)
        5. Returns structured data ready for merging with current calendar
    
    Business Context:
        Historical schedules are critical for:
        - Maintaining multi-week rotation cycles
        - Tracking accumulated rest day quotas
        - Ensuring continuity across planning periods
    
    Args:
        df_calendario_passado: Historical calendar DataFrame with past schedules
        case_type: Processing case identifier (reserved for future use)
        wfm_proc_colab: Workforce management process identifier
        first_date_passado: Start date of historical period (YYYY-MM-DD format)
        last_date_passado: End date of historical period (YYYY-MM-DD format)
        start_date: Start date of the current planning period (YYYY-MM-DD format)
        end_date: End date of the current planning period (YYYY-MM-DD format)
        
    Returns:
        Tuple containing:
            - success (bool): True if treatment succeeded, False otherwise
            - df_calendario_passado (pd.DataFrame): Processed historical calendar data
            - error_message (str): Detailed error description if operation failed
    """
    try:
        # INPUT VALIDATION
        if not first_date_passado or not last_date_passado:
            return False, pd.DataFrame(), "Input validation failed: missing date parameters"
            
        if not df_calendario_passado.empty and 'schedule_day' not in df_calendario_passado.columns:
            return False, pd.DataFrame(), "Input validation failed: missing schedule_day column"
            
        if not start_date or not end_date:
            return False, pd.DataFrame(), "Input validation failed: missing date parameters"
            
        # Treat dates for filtering purposes
        try:
            logger.info(f"Treating dates ({first_date_passado} and {last_date_passado})")
            first_date_passado_dt = pd.to_datetime(first_date_passado, format="%Y-%m-%d")
            last_date_passado_dt = pd.to_datetime(last_date_passado, format="%Y-%m-%d")
            start_date_dt = pd.to_datetime(start_date, format="%Y-%m-%d")
            end_date_dt = pd.to_datetime(end_date, format="%Y-%m-%d")
        except (ValueError, TypeError) as e:
            logger.error(f"Date parsing failed: {str(e)}")
            return False, pd.DataFrame(), "Date parsing failed"

        # TREATMENT LOGIC
        if df_calendario_passado.empty:
            # Even when empty, ensure proper column structure after treatment
            return True, df_calendario_passado, ""
            
        logger.info(f"Treating df_calendario_passado")
        try:
            # SQL now returns 'schedule_day' (standardized) - no renaming needed
            df_calendario_passado['schedule_day_dt'] = pd.to_datetime(df_calendario_passado['schedule_day'], format="%Y-%m-%d")
        except (ValueError, TypeError) as e:
            logger.warning(f"Date conversion failed: {e}")
            return False, pd.DataFrame(), "Date conversion failed"
            
        # Filter out not needed information
        try:
            # Ensure employee_id is string for consistent comparison
            if 'employee_id' in df_calendario_passado.columns:
                df_calendario_passado['employee_id'] = df_calendario_passado['employee_id'].astype(str)
            
            # Create mask to remove rows: start_date <= schedule_day <= end_date
            mask_to_remove = (
                (df_calendario_passado['schedule_day_dt'] >= start_date_dt) &
                (df_calendario_passado['schedule_day_dt'] <= end_date_dt)
            )
            
            # If wfm_proc_colab is not empty, add employee_id condition to mask
            if wfm_proc_colab and wfm_proc_colab != '':
                # Ensure wfm_proc_colab is string for comparison
                wfm_proc_colab_str = str(wfm_proc_colab)
                if 'employee_id' in df_calendario_passado.columns:
                    mask_to_remove = mask_to_remove & (df_calendario_passado['employee_id'] == wfm_proc_colab_str)
            
            # Remove rows that match the mask (keep rows that don't match)
            df_calendario_passado = df_calendario_passado[~mask_to_remove].copy()
            
            logger.info(f"Filtered out {mask_to_remove.sum()} rows from df_calendario_passado")
        except Exception as e:
            logger.warning(f"Filtering out not needed information failed: {e}")
            return False, pd.DataFrame(), f"Filtering out not needed information failed: {e}"

        # Convert types in
        try:
            df_calendario_passado = convert_types_in(df_calendario_passado)
        except Exception as e:
            logger.warning(f"Type conversion failed: {e}")
            return False, pd.DataFrame(), f"Type conversion failed: {e}"

        # Clean up columns
        try:
            columns_to_drop = ['schedule_day_dt']
            # Only drop columns that exist
            existing_columns_to_drop = [col for col in columns_to_drop if col in df_calendario_passado.columns]
            # SQL now returns 'tipo_turno' and 'subtype' (standardized) - keep them
            optional_columns_to_drop = []
            all_columns_to_drop = existing_columns_to_drop + optional_columns_to_drop
            
            if all_columns_to_drop:
                df_calendario_passado = df_calendario_passado.drop(labels=all_columns_to_drop, axis='columns')
        except Exception as e:
            logger.warning(f"Column cleanup failed: {e}")
            return False, pd.DataFrame(), f"Column cleanup failed: {e}"

        # OUTPUT VALIDATION - Allow empty DataFrame as valid result
        return True, df_calendario_passado, ""
        
    except Exception as e:
        error_msg = f"Error in treat_df_calendario_passado: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg

def treat_df_ausencias_ferias(df_ausencias_ferias: pd.DataFrame, start_date: str, end_date: str) -> Tuple[bool, pd.DataFrame, str]:
    """
    Process employee absences and vacation data for scheduling integration.
    
    This function transforms absence/vacation records (which may span multiple days)
    into individual daily entries suitable for calendar integration. It handles various
    absence types and ensures proper filtering for the scheduling period.
    
    Business Logic:
        - Expands date intervals into individual daily records
        - Maps specific absence reasons to vacation type 'V' (configurable via config)
        - Filters out overlapping absences that are already in historical calendar
        - Handles both single-day and multi-day absence periods
    
    Absence Types:
        - 'V': Vacation (férias) - includes mapped absence reason codes
        - 'A': General absence - filtered if already in historical data
        - Other types preserved as-is from source data
    
    Data Transformation:
        Input:  | employee_id | data_ini   | data_fim   | tipo_ausencia |
                | 101         | 2024-01-01 | 2024-01-03 | V            |
        
        Output: | employee_id | data       | tipo_ausencia |
                | 101         | 2024-01-01 | V            |
                | 101         | 2024-01-02 | V            |
                | 101         | 2024-01-03 | V            |
    
    Args:
        df_ausencias_ferias: DataFrame with absence records (data_ini, data_fim, tipo_ausencia)
        start_date: Beginning of scheduling period (YYYY-MM-DD format)
        end_date: End of scheduling period (YYYY-MM-DD format)
        
    Returns:
        Tuple containing:
            - success (bool): True if treatment succeeded, False otherwise
            - df_ausencias_ferias (pd.DataFrame): Expanded daily absence records
            - error_message (str): Error description if operation failed
    """
    try:
        # INPUT VALIDATION
        if df_ausencias_ferias.empty:
            return True, df_ausencias_ferias, ""  # Empty is valid for this function
            
        if not start_date or start_date == '' or not end_date or end_date == '':
            return False, pd.DataFrame(), "Input validation failed: missing date parameters"

        try:
            start_date_dt = pd.to_datetime(start_date, format="%Y-%m-%d")
            end_date_dt = pd.to_datetime(end_date, format="%Y-%m-%d")
        except (ValueError, TypeError) as e:
            return False, pd.DataFrame(), "Date conversion failed"

        if start_date_dt > end_date_dt:
            return False, pd.DataFrame(), "Input validation failed: start_date is greater than end_date"

        required_columns = ['data_ini', 'data_fim']
        missing_columns = [col for col in required_columns if col not in df_ausencias_ferias.columns]
        if missing_columns:
            error_msg = f"Input validation failed: missing columns {missing_columns}"
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        # TREATMENT LOGIC
        if not df_ausencias_ferias['data_ini'].equals(df_ausencias_ferias['data_fim']):
            logger.warning(f"There are date intervals in df_ausencias_ferias. Creating entries for each day in the interval")

            # Convert data_ini and data_fim to datetime objects to be able to do a date_range
            try:
                df_ausencias_ferias['data_ini'] = pd.to_datetime(df_ausencias_ferias['data_ini'], format='%Y-%m-%d')
                df_ausencias_ferias['data_fim'] = pd.to_datetime(df_ausencias_ferias['data_fim'], format='%Y-%m-%d')
            except (ValueError, TypeError) as e:
                logger.warning(f"Date conversion failed: {e}")
                return False, pd.DataFrame(), "Date conversion failed"
            
            # Create a new columns to identify where the different dates could exist
            df_ausencias_ferias['interval_entry'] = np.where(
                df_ausencias_ferias['data_ini'] != df_ausencias_ferias['data_fim'],
                True,
                False
            )

            # Filter the problematic rows and save them into memory
            problematic_rows = df_ausencias_ferias[df_ausencias_ferias['interval_entry'] == True]
            # Take those rows out of the original dataframe
            df_ausencias_ferias = df_ausencias_ferias[df_ausencias_ferias['interval_entry'] == False]
            # Create a new column data, it could be equal to data_ini since both data_ini and data_fim are the same
            df_ausencias_ferias['data'] = df_ausencias_ferias['data_ini']

            # Create a new dataframe where we are going to populate the values
            new_rows = pd.DataFrame(
                columns=list(df_ausencias_ferias.columns)
            )

            # Loop through the problematic rows to expand the interval to a single date on several rows
            for i, row in problematic_rows.iterrows():
                try:
                    date_range = pd.date_range(row['data_ini'], row['data_fim'], freq='D')
                    new_rows = pd.concat([
                        new_rows, 
                        pd.DataFrame({
                            'codigo': row['codigo'],
                            'employee_id': row['employee_id'],
                            'matricula': row['matricula'],
                            'data': date_range,
                            'data_ini': row['data_ini'],
                            'data_fim': row['data_fim'],
                            'tipo_ausencia': row['tipo_ausencia'],
                            'fk_motivo_ausencia': row['fk_motivo_ausencia'],
                        })
                    ])
                except Exception as e:
                    logger.warning(f"Failed to process row {i}: {e}")
                    continue

            # Add new treated rows to the original df
            df_ausencias_ferias = pd.concat([
                df_ausencias_ferias, new_rows
            ], ignore_index=True)
        else:
            # Simple case: data_ini equals data_fim
            df_ausencias_ferias['data'] = df_ausencias_ferias['data_ini']

        # Change column names
        if 'fk_colaborador' in df_ausencias_ferias.columns:
            df_ausencias_ferias = df_ausencias_ferias.rename(
                columns={
                    'fk_colaborador': 'employee_id'
                }
            )
            
        # Drop column names - only drop columns that exist
        columns_to_drop = ['interval_entry', 'data_ini', 'data_fim']
        existing_columns_to_drop = [col for col in columns_to_drop if col in df_ausencias_ferias.columns]
        if existing_columns_to_drop:
            df_ausencias_ferias = df_ausencias_ferias.drop(
                labels=existing_columns_to_drop,
                axis='columns'
            )

        # Convert tipo_ausencia to 'V' for specific vacation motivo_ausencia codes
        motivos_ausencia_list = _config.parameters.get_parameter_default('codigos_motivo_ausencia')
        if 'fk_motivo_ausencia' in df_ausencias_ferias.columns:
            before_v_count = (df_ausencias_ferias['tipo_ausencia'] == 'V').sum()
            
            # Normalize types: convert both to strings for comparison (data may be string or int)
            motivos_ausencia_list_str = [str(m) for m in motivos_ausencia_list]
            df_motivos_str = df_ausencias_ferias['fk_motivo_ausencia'].astype(str)
            matching_mask = df_motivos_str.isin(motivos_ausencia_list_str)
            df_ausencias_ferias.loc[matching_mask, 'tipo_ausencia'] = 'V'
            after_v_count = (df_ausencias_ferias['tipo_ausencia'] == 'V').sum()
            converted_count = after_v_count - before_v_count
            if converted_count > 0:
                logger.info(f"Converted {converted_count} absence rows to vacation (V) based on motivo codes")
        else:
            logger.warning("fk_motivo_ausencia column not found, cannot convert to V")

        # Filter out tipo_ausencia = 'A' for dates outside start_date and end_date (these are already present coming from df_calendario_passado)
        if 'tipo_ausencia' in df_ausencias_ferias.columns and 'data' in df_ausencias_ferias.columns:
            # Ensure data column is datetime
            df_ausencias_ferias['data'] = pd.to_datetime(df_ausencias_ferias['data'])
            # Remove rows where tipo_ausencia='A' and date is within the execution period
            df_ausencias_ferias = df_ausencias_ferias[
                ~((df_ausencias_ferias['tipo_ausencia'] == 'A') & 
                  (df_ausencias_ferias['data'] <= start_date_dt) & 
                  (df_ausencias_ferias['data'] >= end_date_dt))
            ]
            
        # OUTPUT VALIDATION
        if not df_ausencias_ferias.empty and 'data' not in df_ausencias_ferias.columns:
            return False, pd.DataFrame(), "Treatment failed: missing data column in result"
            
        return True, df_ausencias_ferias, ""

    except Exception as e:
        logger.error(f"Error in treat_df_ausencias_ferias: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def treat_df_ciclos_completos(df_ciclos_completos: pd.DataFrame, df_colaborador_limits: pd.DataFrame = None) -> Tuple[bool, pd.DataFrame, str]:
    """
    Treat df_ciclos_completos dataframe.
    Merges employee limit columns and converts WFM types to algorithm 'horario' codes.
    
    Args:
        df_ciclos_completos: DataFrame with ciclos completos data
        df_colaborador_limits: DataFrame with columns [matricula, limite_superior_manha, limite_inferior_tarde]
        
    Returns:
        Tuple of (success, treated_dataframe, error_message)
    """
    try:
        # INPUT VALIDATION - empty is valid for this function
        if df_ciclos_completos.empty:
            logger.info("df_ciclos_completos is empty - returning as-is")
            return True, df_ciclos_completos, ""
        
        # TREATMENT LOGIC
        logger.info(f"Treating df_ciclos_completos with {len(df_ciclos_completos)} rows")
        
        # Step 1: Merge employee limits if provided
        # These are here because these calculations might need limite_superior_manha and limite_inferior_tarde
        if df_colaborador_limits is not None and not df_colaborador_limits.empty:
            logger.info("Merging employee limit columns")
            # Normalize column names for merge
            df_ciclos_completos.columns = df_ciclos_completos.columns.str.lower()
            df_colaborador_limits.columns = df_colaborador_limits.columns.str.lower()
            
            # Merge on matricula
            df_ciclos_completos = df_ciclos_completos.merge(
                df_colaborador_limits,
                on='matricula',
                how='left'
            )
            logger.info(f"Merged limits - resulting shape: {df_ciclos_completos.shape}")
        else:
            logger.info("No employee limits provided - proceeding without merge")
            # Still need to normalize column names for consistency
            df_ciclos_completos.columns = df_ciclos_completos.columns.str.lower()
        
        # Step 2: Transform dia_semana from database standard to algorithm standard
        if 'dia_semana' in df_ciclos_completos.columns:
            logger.info("Converting dia_semana from database standard to algorithm standard")
            df_ciclos_completos['dia_semana'] = df_ciclos_completos['dia_semana'].apply(
                lambda x: x - 6 if x == 7 else (x if x == 8 else x + 1)
            )
            logger.info("dia_semana transformation complete")
        else:
            logger.warning("dia_semana column not found - skipping transformation")
        
        # Step 3: Convert WFM types to algorithm 'horario' codes
        logger.info("Converting WFM types to horario codes")
        l_dom_days = _config.parameters.get_parameter_default('l_dom_days')
        df_ciclos_completos = convert_ciclos_to_horario(df_ciclos_completos, l_dom_days)
        
        # OUTPUT VALIDATION
        if 'horario' not in df_ciclos_completos.columns:
            return False, pd.DataFrame(), "Conversion failed: missing 'horario' column in output"
        
        logger.info(f"Successfully treated df_ciclos_completos - {len(df_ciclos_completos)} rows with 'horario' column")
        return True, df_ciclos_completos, ""
        
    except Exception as e:
        logger.error(f"Error in treat_df_ciclos_completos: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), str(e)

def treat_df_folgas_ciclos(df_folgas_ciclos: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Process fixed day-off cycle data for calendar integration.
    
    This function standardizes day-off cycle records by:
    - Filtering to essential columns (employee_id, matricula, schedule_day, tipo_dia)
    - Converting day-off type codes from 'F' (feriado/folga) to 'L' (libranza/day-off)
    
    Business Context:
        Fixed cycles represent predefined rest day patterns for employees who don't
        follow 90-day rotation schedules. These are typically weekly or bi-weekly
        patterns that repeat consistently.
    
    Args:
        df_folgas_ciclos: DataFrame containing fixed day-off cycle data
        
    Returns:
        Tuple containing:
            - success (bool): True if treatment succeeded, False otherwise
            - df_folgas_ciclos (pd.DataFrame): Processed day-off data with 'L' codes
            - error_message (str): Error description if operation failed
    """
    try:
        # INPUT VALIDATION - empty is valid for this function
        if df_folgas_ciclos.empty:
            logger.info("df_folgas_ciclos is empty - returning as-is (all employees have 90-day cycles)")
            return True, df_folgas_ciclos, ""
        
        # Copy data frame
        df_folgas_ciclos = df_folgas_ciclos.copy()
        # Remove unwanted columns
        needed_columns = ['employee_id', 'matricula', 'schedule_day', 'tipo_dia']
        df_folgas_ciclos = df_folgas_ciclos[needed_columns]

        # convert the values of tipo_dia to "F"
        df_folgas_ciclos['tipo_dia'] = df_folgas_ciclos['tipo_dia'].replace('F', 'L')

        # convert the values of tipo_dia to "F"
        df_folgas_ciclos['tipo_dia'] = df_folgas_ciclos['tipo_dia'].replace('S', '-')
        
        return True, df_folgas_ciclos, ""
    except Exception as e:
        logger.error(f"Error in treat_df_folgas_ciclos: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def treat_df_colaborador(df_colaborador: pd.DataFrame, employees_id_list: List[str]) -> Tuple[bool, pd.DataFrame, str]:
    """
    Process and validate core employee contract and configuration data.
    
    This is a critical data preparation function that standardizes employee master data
    for use throughout the scheduling algorithm. It performs column mapping, type
    conversions, and validation of business-critical fields.
    
    Key Operations:
        1. Column Renaming: Maps database columns to algorithm field names
           - min_dias_trabalhados → min_dia_trab
           - max_dias_trabalhados → max_dia_trab
           - fds_cal_2d → c2d, fds_cal_3d → c3d, d_cal_xx → cxx
        
        2. Type Conversions: Ensures numeric and date fields have correct types
           - Contract limits (min_dia_trab, max_dia_trab)
           - Day-off quotas (dyf_max_t, c2d, c3d, cxx, lqs)
           - Date fields (data_admissao, data_demissao)
           - Identifiers to strings (employee_id, matricula - identifiers not numbers)
        
        3. Data Quality Validations:
           - seq_turno must not be 0 or null (shift sequence is required)
           - Contract limits must be valid (1-8 days, min ≤ max)
           - Date fields preserved as NaT/None (not filled with 0)
    
    Business Rules:
        - convenio: Uppercase standardization for agreement type
        - Missing numeric values: Filled with 0 (except dates and times)
        - Time columns: Preserved as-is to maintain shift boundary information
    
    Args:
        df_colaborador: Raw employee DataFrame from database/source
        employees_id_list: List of valid employee IDs to process (as strings)
        
    Returns:
        Tuple containing:
            - success (bool): True if all validations passed, False otherwise
            - df_colaborador (pd.DataFrame): Standardized employee data
            - error_message (str): Detailed validation error if any checks failed
            
    Raises:
        Returns False with error message if:
        - Required columns are missing
        - seq_turno contains invalid values
        - Contract limits are outside valid ranges
        - Validation function fails
    """
    try:
        # EARLY TYPE CONVERSION FOR IDENTIFIERS
        # Convert employee_id to string BEFORE validation to ensure type consistency
        # (employee_id comes as int from SQL but needs to be string for comparisons)
        df_colaborador['employee_id'] = df_colaborador['employee_id'].astype(str)
        employees_id_list = [str(x) for x in employees_id_list]
        
        # INPUT VALIDATION
        # TODO: add validations
        if not validate_df_colaborador(df_colaborador=df_colaborador, employees_id_list=employees_id_list):
            return False, pd.DataFrame(), "Input validation failed: empty DataFrame"
            
        # Rename columns LOGIC
        try:
            # SQL now returns standardized names, but handle legacy column names if they exist
            rename_mapping = {}
            
            # Map database column names to expected business logic names
            column_mapping = {
                'min_dias_trabalhados': 'min_dia_trab',
                'max_dias_trabalhados': 'max_dia_trab', 
                'lq': 'lqs'  # Rename lq to lqs for consistency
            }
            
            # Apply column renaming
            df_colaborador = df_colaborador.rename(columns=column_mapping)
                
        except Exception as e:
            logger.warning(f"Column renaming failed: {e}")
            # Continue with original column names
        
        # Convert data types logic
        try:
            df_colaborador['convenio'] = df_colaborador['convenio'].str.upper()
            df_colaborador['min_dia_trab'] = pd.to_numeric(df_colaborador['min_dia_trab'], errors='coerce')
            df_colaborador['max_dia_trab'] = pd.to_numeric(df_colaborador['max_dia_trab'], errors='coerce')
            df_colaborador['dyf_max_t'] = pd.to_numeric(df_colaborador['dyf_max_t'], errors='coerce')
            df_colaborador['c2d'] = pd.to_numeric(df_colaborador['c2d'], errors='coerce')
            df_colaborador['c3d'] = pd.to_numeric(df_colaborador['c3d'], errors='coerce')
            df_colaborador['cxx'] = pd.to_numeric(df_colaborador['cxx'], errors='coerce')
            df_colaborador['lqs'] = pd.to_numeric(df_colaborador['lqs'], errors='coerce')
            df_colaborador['data_admissao'] = pd.to_datetime(df_colaborador['data_admissao'], errors='coerce')
            df_colaborador['data_demissao'] = pd.to_datetime(df_colaborador['data_demissao'], errors='coerce')
            df_colaborador['seq_turno'] = df_colaborador['seq_turno'].fillna('').astype(str)
            df_colaborador['ciclo'] = df_colaborador['ciclo'].fillna('').astype(str)
            
            # Convert matricula to string for consistency (already did employee_id before validation)
            df_colaborador['matricula'] = df_colaborador['matricula'].astype(str)
        except Exception as e:
            error_msg = f"Error converting specific columns to numeric type: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, pd.DataFrame(), error_msg

        #logger.info(f"DEBUG df_colaborador:\n {df_colaborador}")

        # Initialize columns that will be created later in func_inicializa
        # These columns are calculated from calendar data (matriz2) in func_inicializa,
        # but need to exist earlier for functions like adjust_counters_for_contract_types
        if 'total_dom_fes' not in df_colaborador.columns:
            df_colaborador['total_dom_fes'] = 0
        if 'total_fes' not in df_colaborador.columns:
            df_colaborador['total_fes'] = 0
        if 'total_holidays' not in df_colaborador.columns:
            df_colaborador['total_holidays'] = 0

        # Fill missing values
        # Exclude date and time columns from fillna(0) - they should remain as NaT/None
        datetime_columns = ['data_admissao', 'data_demissao', "h_tm_in", "h_seg_in", "h_ter_in", "h_qua_in", "h_qui_in", "h_sex_in", "h_sab_in", "h_dom_in", "h_fer_in", "h_tt_out", "h_seg_out", "h_ter_out", "h_qua_out", "h_qui_out", "h_sex_out", "h_sab_out", "h_dom_out", "h_fer_out"]
        non_date_columns = [col for col in df_colaborador.columns if col not in datetime_columns]
        df_colaborador[non_date_columns] = df_colaborador[non_date_columns].fillna(0)

        # Validate seq_turno
        seq_turno_zeros = bool((df_colaborador['seq_turno'] == 0).any())
        seq_turno_nulls = bool(df_colaborador['seq_turno'].isna().any())
        if seq_turno_zeros or seq_turno_nulls:
            error_msg = f"seq_turno=0 or null - columna SEQ_TURNO mal parametrizada: {df_colaborador['seq_turno'] == 0}"
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        # Validate min_dia_trab and max_dia_trab instead of tipo_contrato (which is created later)
        min_zeros = bool((df_colaborador['min_dia_trab'] == 0).any())
        min_invalid = bool((df_colaborador['min_dia_trab'] > 8).any())
        min_nulls = bool(df_colaborador['min_dia_trab'].isna().any())
        max_zeros = bool((df_colaborador['max_dia_trab'] == 0).any())
        max_invalid = bool((df_colaborador['max_dia_trab'] > 8).any())
        max_nulls = bool(df_colaborador['max_dia_trab'].isna().any())
        invalid_values = bool((df_colaborador['max_dia_trab'] < df_colaborador['min_dia_trab']).any())
        
        if min_zeros or min_invalid or min_nulls or max_zeros or max_invalid or max_nulls or invalid_values:
            error_msg = f"min_dia_trab or max_dia_trab contains 0 or null values - contract data not valid"
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
            
        # OUTPUT VALIDATION
        if df_colaborador.empty:
            return False, pd.DataFrame(), "Treatment resulted in empty DataFrame"
            
        return True, df_colaborador, ""
        
    except Exception as e:
        logger.error(f"Error in treat_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def treat_df_contratos(df_contratos: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Process and transform contract data for scheduling calculations.
    
    This function converts contract time limits from timedelta format to hours and
    calculates the daily workload (carga_diaria) based on maximum weekly workload
    and maximum workday constraints.
    
    Business Logic:
        - Converts maximumworkload, maximumworkday, and maximumdaysperweek from
          timedelta to hours (total seconds / 3600)
        - Calculates carga_diaria (daily workload) as the minimum of:
          - maximumworkload / maximumdaysperweek (average daily workload)
          - maximumworkday (maximum allowed per day)
        - Ensures carga_diaria is non-negative (sets to 0 if negative)
    
    Args:
        df_contratos: DataFrame with contract information containing:
            - maximumworkload: Maximum weekly workload (timedelta)
            - maximumworkday: Maximum daily work hours (timedelta)
            - maximumdaysperweek: Maximum working days per week (timedelta)
    
    Returns:
        Tuple containing:
            - success (bool): True if treatment succeeded, False otherwise
            - df_contratos (pd.DataFrame): Processed contract data with carga_diaria
            - error_message (str): Error description if operation failed
    """
    try:
        # INPUT VALIDATION
        if df_contratos is None or df_contratos.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_contratos"
        
        required_cols = ['maximumworkload', 'maximumworkday', 'maximumdaysperweek']
        missing_cols = [col for col in required_cols if col not in df_contratos.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"Input validation failed: missing columns {missing_cols}"
        
        # TREATMENT LOGIC
        df_result = df_contratos.copy()
        
        # Convert timedelta columns to hours (total seconds / 3600)
        df_result['maximumworkload'] = pd.to_timedelta(df_result['maximumworkload']).dt.total_seconds() / 3600
        df_result['maximumworkday'] = pd.to_timedelta(df_result['maximumworkday']).dt.total_seconds() / 3600
        df_result['maximumdaysperweek'] = pd.to_timedelta(df_result['maximumdaysperweek']).dt.total_seconds() / 3600
        
        # Calculate carga_diaria (daily workload)
        # Use the minimum of average daily workload and maximum workday
        df_result['carga_diaria'] = np.where(
            np.trunc(df_result['maximumworkload'] / df_result['maximumdaysperweek']) < df_result['maximumworkday'],
            np.trunc(df_result['maximumworkload'] / df_result['maximumdaysperweek']),
            df_result['maximumworkday']
        )
        
        # Ensure carga_diaria is non-negative
        df_result['carga_diaria'] = np.where(
            df_result['carga_diaria'] < 0,
            0,
            df_result['carga_diaria']
        )
        
        # OUTPUT VALIDATION
        if 'carga_diaria' not in df_result.columns:
            return False, pd.DataFrame(), "Treatment failed: carga_diaria column not created"
        
        logger.info(f"Successfully treated df_contratos. Shape: {df_result.shape}")
        return True, df_result, ""
        
    except Exception as e:
        logger.error(f"Error in treat_df_contratos: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), str(e)

def add_lqs_to_df_colaborador(df_colaborador: pd.DataFrame, df_params_lq: pd.DataFrame, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    Add quality weekend leave quota (lqs) to employee data.
    
    Quality weekends refer to specific weekend patterns that must be allocated to
    employees based on their contract agreements. This function manages the lq/lqs
    quota assignment logic.
    
    Use Cases:
        - Case 0: Set all lq values to 0 (disabled)
        - Case 1: Apply configured lq treatment logic (to be implemented)
    
    Business Context:
        LQ (Libre Qualidad) represents quality rest weekends that must be distributed
        fairly among employees based on their contract type and work patterns.
    
    Args:
        df_colaborador: Employee DataFrame to update
        df_params_lq: Configuration parameters for lq calculations
        use_case: Processing mode (0=disabled, 1=apply logic)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated employee data with lq column
            - error_message (str): Error description if operation failed
            
    Note:
        Use case 1 logic is reserved for future implementation when business
        rules for lq calculation are fully defined.
    """
    try:
        if use_case == 0:
            df_colaborador['lq'] = 0
        elif use_case == 1:
            # TODO: develop lq treatment logic (import from the original models.py)
            pass
        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        return True, df_colaborador, ""
    except Exception as e:
        error_msg = f""
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg

def set_tipo_contrato_to_df_colaborador(df_colaborador: pd.DataFrame, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    Calculate and assign contract type based on minimum/maximum working days.
    
    Contract type is a derived field that categorizes employees based on their
    weekly working day requirements. This classification drives rest day allocation
    logic throughout the scheduling algorithm.
    
    Contract Type Classification:
        - Type 2: 2 working days (min=2, max=2)
        - Type 3: 2-3, 3, or 3-4 working days
        - Type 4: 2-4, 3-5, 3-6, 4, 4-5, or 4-6 working days
        - Type 5: 5 or 5-6 working days
        - Type 6: 6 working days (min=6, max=6)
    
    Business Logic:
        Contract type determines:
        - Number of mandatory rest days per week
        - Weekend work requirements
        - Quality weekend entitlements
        - Holiday work patterns
    
    Use Cases:
        - Case 0: Use existing tipo_contrato from source data
        - Case 1: Calculate tipo_contrato from min/max_dia_trab using lookup table
    
    Args:
        df_colaborador: Employee DataFrame with min_dia_trab and max_dia_trab columns
        use_case: Processing mode (0=preserve existing, 1=calculate)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated data with tipo_contrato column
            - error_message (str): Error description if operation failed
            
    Note:
        The min/max to tipo_contrato mapping table should ideally be stored in
        configuration/database rather than hardcoded.
    """
    try:
        # INPUT VALIDATION
        if df_colaborador.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_colaborador DataFrame"
            
        # TREATMENT LOGIC
        if use_case == 0:
            df_colaborador['tipo_contrato']
        if use_case == 1:
            # TODO: this shouldnt be defined here, it should be in database or csv
            params_contrato = pd.DataFrame({
                'min': [2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 6],
                'max': [2, 3, 4, 3, 4, 5, 6, 4, 5, 6, 5, 6, 6],
                'tipo_contrato': [2, 3, 4, 3, 4, 5, 4, 4, 5, 6, 5, 6, 6]
            })

            df_colaborador = pd.merge(
                df_colaborador, 
                params_contrato, 
                left_on=['min_dia_trab', 'max_dia_trab'], 
                right_on=['min', 'max'], 
                how='left'
            )
        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        # OUTPUT VALIDATION
        return True, df_colaborador, ""
        
    except Exception as e:
        logger.error(f"Error in set_tipo_contrato_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def add_prioridade_folgas_to_df_colaborador(df_colaborador: pd.DataFrame, df_valid_emp: pd.DataFrame, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    Merge employee day-off priority information into main employee dataframe.
    
    This function integrates priority levels from the validated employee list into
    the main employee master data. Priority affects the order in which employees
    are assigned preferred rest days during schedule generation.
    
    Priority Levels:
        - 'manager': Highest priority (managers, supervisors)
        - 'keyholder': Medium priority (key-holding staff)
        - 'normal' or '': Standard priority (all other employees)
    
    Business Context:
        Priority ensures that:
        - Management can maintain consistent rest day patterns
        - Key personnel availability is optimized
        - Fair distribution among standard employees
    
    Use Cases:
        - Case 0: Set empty priority for all employees (disabled)
        - Case 1: Merge priorities from df_valid_emp via left join on fk_colaborador
    
    Args:
        df_colaborador: Main employee DataFrame
        df_valid_emp: Validated employee list with prioridade_folgas column
        use_case: Processing mode (0=disabled, 1=merge priorities)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated data with prioridade_folgas
            - error_message (str): Error description if operation failed
    """
    try:
        if df_colaborador.empty or len(df_colaborador) == 0:
            return False, pd.DataFrame(), "Input validation failed: empty df_colaborador DataFrame"

        needed_columns = ['employee_id', 'prioridade_folgas']
        if not all(col in df_valid_emp.columns for col in needed_columns):
            return False, pd.DataFrame(), f"Needed columns not present in df_valid_emp: {needed_columns}"

        if use_case == 0:
            df_colaborador['prioridade_folgas'] = ''
        
        elif use_case == 1:
            # Merge with valid_emp to get PRIORIDADE_FOLGAS
            # Note: employee_id is already converted to string in both dataframes by their respective treatment functions
            df_colaborador = pd.merge(
                df_colaborador, 
                df_valid_emp[['employee_id', 'prioridade_folgas']], 
                on='employee_id', 
                how='left'
            )

        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        return True, df_colaborador, ""

    except Exception as e:
        logger.error(f"Error in add_prioridade_folgas_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def date_adjustments_to_df_colaborador(df_colaborador: pd.DataFrame, main_year: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    Apply date adjustments to df_colaborador using vectorized operations for annual counters.
    
    Adjusts l_d, l_dom, l_q, l_total, c2d, and c3d fields proportionally based on:
    - Admission date: when employee was hired after January 1st
    - Termination date: when employee left before December 31st
    - Both: when employee was hired after January 1st AND left before December 31st
    
    Args:
        df_colaborador: DataFrame with employee data
        year: The year for which to apply adjustments (annual counters)
        
    Returns:
        Tuple[bool, DataFrame, str]: Success flag, processed DataFrame, error message
    """
    try:
        # INPUT VALIDATION
        if df_colaborador is None or df_colaborador.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_colaborador"
            
        if not main_year or not isinstance(main_year, int) or main_year < 1900 or main_year > 2100:
            return False, pd.DataFrame(), "Input validation failed: invalid year parameter"
            
        # Construct annual dates from year
        start_date = f"{main_year}-01-01"
        end_date = f"{main_year}-12-31"
        
        logger.info(f"Applying date adjustments for year {main_year} ({start_date} to {end_date}) for {len(df_colaborador)} employees")
        
        # TREATMENT LOGIC
        df_result = df_colaborador.copy()
        
        # Convert date strings to datetime
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        total_days = (end_dt - start_dt).days + 1
        
        # Convert dates to datetime if they're not already
        df_result['data_admissao'] = pd.to_datetime(df_result['data_admissao'], errors='coerce')
        
        # Handle data_demissao - convert to datetime and handle null/empty values
        if 'data_demissao' in df_result.columns:
            # Replace empty strings with NaN
            df_result['data_demissao'] = df_result['data_demissao'].replace('', pd.NaT)
            df_result['data_demissao'] = pd.to_datetime(df_result['data_demissao'], errors='coerce')
        else:
            # If column doesn't exist, create it as NaT (no termination dates)
            df_result['data_demissao'] = pd.NaT
        
        # Define fields to adjust along with desired rounding strategy
        fields_to_adjust = ['ld', 'l_dom', 'lq', 'l_total', 'c2d', 'c3d', 'cxx']
        rounding_map = {
            'l_total': np.round,
            'l_dom': np.round,
            'c2d': np.floor,
            'c3d': np.floor,
            'ld': np.round,
            'lq': np.round,
            'cxx': np.round,
        }
        
        # SCENARIO A: Late hire only (hired after start_date, still active OR terminate after year end)
        late_hire_mask = (
            df_result['data_admissao'].notna() & 
            (start_dt < df_result['data_admissao']) &
            (
                df_result['data_demissao'].isna() | 
                (df_result['data_demissao'] > end_dt)
            )
        )
        
        if late_hire_mask.any():
            logger.info(f"DEBUG: {late_hire_mask.sum()} employees need late hire adjustments")
            
            # Calculate adjustment factors (from admission to year end, even if termination is in next year)
            days_worked = (end_dt - df_result.loc[late_hire_mask, 'data_admissao']).dt.days + 1
            div_factors = days_worked / total_days
            
            # Log debug info with detailed math for each employee
            for idx in df_result[late_hire_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                admission_date = df_result.loc[idx, 'data_admissao']
                termination_date = df_result.loc[idx, 'data_demissao']
                div = div_factors.loc[idx]
                days = days_worked.loc[idx]
                
                logger.info(f"=== SCENARIO A (Late Hire) - Employee {matricula} ===")
                logger.info(f"  Admission: {admission_date}, Termination: {termination_date if pd.notna(termination_date) else 'None'}")
                logger.info(f"  Days worked in {main_year}: {days}, Total days in year: {total_days}, Adjustment factor: {div:.6f}")
                
                # Log detailed math for each field
                for field in fields_to_adjust:
                    if field in df_result.columns:
                        before_val = df_result.loc[idx, field]
                        calculated_val = before_val * div
                        rounder = rounding_map.get(field, np.ceil)
                        after_val = rounder(calculated_val)
                        logger.info(f"  {field}: {before_val} * {div:.6f} = {calculated_val:.6f} -> (rounded) = {after_val}")
                        if after_val < 0:
                            logger.error(f"  WARNING: {field} became negative ({after_val}) for employee {matricula}!")
            
            # Apply adjustments vectorized
            for field in fields_to_adjust:
                if field in df_result.columns:
                    rounder = rounding_map.get(field, np.ceil)
                    original_values = df_result.loc[late_hire_mask, field].copy()
                    # Ensure div_factors Series has same index as the filtered DataFrame
                    # Get the indices that match the mask
                    mask_indices = df_result[late_hire_mask].index
                    div_factors_aligned = div_factors.loc[mask_indices]
                    
                    # Multiply and round - ensure indices align properly
                    adjusted_values = original_values * div_factors_aligned
                    rounded_values = rounder(adjusted_values)
                    
                    # Assign back using the same indices
                    df_result.loc[mask_indices, field] = rounded_values.values
                    
                    # Log verification after adjustment for ALL employees to ensure assignment worked
                    logger.info(f"VERIFY {field} adjustments applied to {len(mask_indices)} employees:")
                    for idx in mask_indices:
                        matricula = df_result.loc[idx, 'matricula']
                        final_val = df_result.loc[idx, field]
                        logger.info(f"  Employee {matricula}: original={original_values.loc[idx]}, factor={div_factors.loc[idx]:.6f}, final={final_val}")
                    
                    # Check for negative values after adjustment
                    negative_mask = df_result.loc[late_hire_mask, field] < 0
                    if negative_mask.any():
                        for neg_idx in df_result.loc[late_hire_mask & negative_mask].index:
                            matricula = df_result.loc[neg_idx, 'matricula']
                            logger.error(f"ERROR: {field} is negative ({df_result.loc[neg_idx, field]}) for employee {matricula} after adjustment!")
                            logger.error(f"  Original value: {original_values.loc[neg_idx]}, Factor: {div_factors.loc[neg_idx]:.6f}")
        
        # SCENARIO B: Early termination only (hired before/on start_date, left before end_date)
        early_term_mask = (
            df_result['data_admissao'].notna() & 
            (df_result['data_admissao'] <= start_dt) &
            df_result['data_demissao'].notna() &
            (df_result['data_demissao'] < end_dt)
        )
        
        if early_term_mask.any():
            logger.info(f"DEBUG: {early_term_mask.sum()} employees need early termination adjustments")
            
            # Calculate adjustment factors
            days_worked = (df_result.loc[early_term_mask, 'data_demissao'] - start_dt).dt.days + 1
            div_factors = days_worked / total_days
            
            # Log debug info with detailed math for each employee
            for idx in df_result[early_term_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                admission_date = df_result.loc[idx, 'data_admissao']
                termination_date = df_result.loc[idx, 'data_demissao']
                div = div_factors.loc[idx]
                days = days_worked.loc[idx]
                
                logger.info(f"=== SCENARIO B (Early Termination) - Employee {matricula} ===")
                logger.info(f"  Admission: {admission_date}, Termination: {termination_date}")
                logger.info(f"  Days worked in {main_year}: {days}, Total days in year: {total_days}, Adjustment factor: {div:.6f}")
                
                # Log detailed math for each field
                for field in fields_to_adjust:
                    if field in df_result.columns:
                        before_val = df_result.loc[idx, field]
                        calculated_val = before_val * div
                        rounder = rounding_map.get(field, np.ceil)
                        after_val = rounder(calculated_val)
                        logger.info(f"  {field}: {before_val} * {div:.6f} = {calculated_val:.6f} -> (rounded) = {after_val}")
                        if after_val < 0:
                            logger.error(f"  WARNING: {field} became negative ({after_val}) for employee {matricula}!")
            
            # Apply adjustments vectorized
            for field in fields_to_adjust:
                if field in df_result.columns:
                    rounder = rounding_map.get(field, np.ceil)
                    original_values = df_result.loc[early_term_mask, field].copy()
                    df_result.loc[early_term_mask, field] = rounder(
                        df_result.loc[early_term_mask, field] * div_factors
                    )
                    # Check for negative values after adjustment
                    negative_mask = df_result.loc[early_term_mask, field] < 0
                    if negative_mask.any():
                        for neg_idx in df_result.loc[early_term_mask & negative_mask].index:
                            matricula = df_result.loc[neg_idx, 'matricula']
                            logger.error(f"ERROR: {field} is negative ({df_result.loc[neg_idx, field]}) for employee {matricula} after adjustment!")
                            logger.error(f"  Original value: {original_values.loc[neg_idx]}, Factor: {div_factors.loc[neg_idx]:.6f}")
        
        # SCENARIO C: Both late hire AND early termination (terminate within the year)
        both_adjust_mask = (
            df_result['data_admissao'].notna() & 
            (start_dt < df_result['data_admissao']) &
            df_result['data_demissao'].notna() &
            (df_result['data_demissao'] <= end_dt)
        )
        
        if both_adjust_mask.any():
            logger.info(f"DEBUG: {both_adjust_mask.sum()} employees need both late hire and early termination adjustments")
            
            # Calculate adjustment factors (from admission to termination, capped at year end)
            days_worked = (df_result.loc[both_adjust_mask, 'data_demissao'] - df_result.loc[both_adjust_mask, 'data_admissao']).dt.days + 1
            div_factors = days_worked / total_days
            
            # Log debug info with detailed math for each employee
            for idx in df_result[both_adjust_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                admission_date = df_result.loc[idx, 'data_admissao']
                termination_date = df_result.loc[idx, 'data_demissao']
                div = div_factors.loc[idx]
                days = days_worked.loc[idx]
                
                logger.info(f"=== SCENARIO C (Both Late Hire & Early Term) - Employee {matricula} ===")
                logger.info(f"  Admission: {admission_date}, Termination: {termination_date}")
                logger.info(f"  Days worked in {main_year}: {days}, Total days in year: {total_days}, Adjustment factor: {div:.6f}")
                
                # Log detailed math for each field
                for field in fields_to_adjust:
                    if field in df_result.columns:
                        before_val = df_result.loc[idx, field]
                        calculated_val = before_val * div
                        rounder = rounding_map.get(field, np.ceil)
                        after_val = rounder(calculated_val)
                        logger.info(f"  {field}: {before_val} * {div:.6f} = {calculated_val:.6f} -> (rounded) = {after_val}")
                        if after_val < 0:
                            logger.error(f"  WARNING: {field} became negative ({after_val}) for employee {matricula}!")
            
            # Apply adjustments vectorized
            for field in fields_to_adjust:
                if field in df_result.columns:
                    rounder = rounding_map.get(field, np.ceil)
                    original_values = df_result.loc[both_adjust_mask, field].copy()
                    df_result.loc[both_adjust_mask, field] = rounder(
                        df_result.loc[both_adjust_mask, field] * div_factors
                    )
                    # Check for negative values after adjustment
                    negative_mask = df_result.loc[both_adjust_mask, field] < 0
                    if negative_mask.any():
                        for neg_idx in df_result.loc[both_adjust_mask & negative_mask].index:
                            matricula = df_result.loc[neg_idx, 'matricula']
                            logger.error(f"ERROR: {field} is negative ({df_result.loc[neg_idx, field]}) for employee {matricula} after adjustment!")
                            logger.error(f"  Original value: {original_values.loc[neg_idx]}, Factor: {div_factors.loc[neg_idx]:.6f}")
        
        # Handle L_Q recalculation for all adjusted employees (when c3d + c2d > l_q)
        all_adjusted_mask = late_hire_mask | early_term_mask | both_adjust_mask
        
        if all_adjusted_mask.any() and 'l_q' in df_result.columns and 'c2d' in df_result.columns and 'c3d' in df_result.columns:
            lq_adjustment_mask = all_adjusted_mask & (
                (df_result['c3d'] + df_result['c2d']) > df_result['l_q']
            )
            
            if lq_adjustment_mask.any():
                # Recalculate L_Q values vectorized
                df_result.loc[lq_adjustment_mask, 'l_q'] = (
                    df_result.loc[lq_adjustment_mask, 'c2d'] + 
                    df_result.loc[lq_adjustment_mask, 'c3d'] + 
                    df_result.loc[lq_adjustment_mask, 'c3d']
                )
                
                # Log error messages for employees with L_Q recalculation
                for idx in df_result[lq_adjustment_mask].index:
                    matricula = df_result.loc[idx, 'matricula']
                    logger.error(f"Empleado {matricula} sin suficiente L_Q para fines de semana de calidad. Recalculated l_q: {df_result.loc[idx, 'l_q']}")
        
        # Final validation: Check for negative values in adjusted fields
        negative_issues = []
        
        if all_adjusted_mask.any():
            for field in fields_to_adjust:
                if field in df_result.columns:
                    negative_mask = (df_result[field] < 0) & all_adjusted_mask
                    if negative_mask.any():
                        negative_count = negative_mask.sum()
                        negative_issues.append(f"{field}: {negative_count} employees")
                        for neg_idx in df_result[negative_mask].index:
                            matricula = df_result.loc[neg_idx, 'matricula']
                            logger.error(f"FINAL CHECK: {field} is negative ({df_result.loc[neg_idx, field]}) for employee {matricula}")
        
        if negative_issues:
            logger.error(f"VALIDATION FAILED: Found negative values after adjustments: {', '.join(negative_issues)}")
            logger.error("This indicates the original values were already negative or the calculation produced invalid results.")
        else:
            logger.info("VALIDATION PASSED: No negative values found in adjusted fields.")
        
        # Final verification: log all adjusted l_dom values to ensure they persisted
        if 'l_dom' in df_result.columns:
            adjusted_employees = df_result[late_hire_mask | early_term_mask | both_adjust_mask]
            if not adjusted_employees.empty:
                logger.info(f"FINAL VERIFICATION - l_dom values after all adjustments:")
                for idx, row in adjusted_employees.iterrows():
                    logger.info(f"  Employee {row['matricula']} (idx={idx}): l_dom={row['l_dom']}")
            else:
                logger.info("FINAL VERIFICATION - No employees needed adjustments")
        
        logger.info(f"Successfully applied date adjustments to {len(df_result)} employees")
        logger.info(f"Summary: {late_hire_mask.sum()} late hires, {early_term_mask.sum()} early terminations, {both_adjust_mask.sum()} both adjustments")
        
        return True, df_result, ""
        
    except Exception as e:
        logger.error(f"Error in date_adjustments_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing failed: {str(e)}"


def contract_adjustments_to_df_colaborador(df_colaborador: pd.DataFrame, 
                                               start_date: str, 
                                               end_date: str,
                                               convenio_bd: str,
                                               num_fer_dom: int,
                                               fer_fechados: int,
                                               num_sundays: int,
                                               use_case: int = 0,
                                               matriz_festivos: pd.DataFrame = None,
                                               count_open_holidays_func = None) -> Tuple[bool, pd.DataFrame, str]:
    """
    Apply contract type and convention business logic using vectorized operations.
    
    Args:
        df_colaborador: DataFrame with employee data
        start_date: Period start date as string (YYYY-MM-DD format)
        end_date: Period end date as string (YYYY-MM-DD format)
        convenio_bd: Database convention identifier
        num_fer_dom: Number of holiday/sunday periods
        fer_fechados: Number of closed holidays
        num_sundays: Number of sundays in period
        matriz_festivos: DataFrame with holiday data (optional)
        count_open_holidays_func: Function to count open holidays (optional)
        
    Returns:
        Tuple[bool, DataFrame, str]: Success flag, processed DataFrame, error message
    """
    try:
        # INPUT VALIDATION
        if df_colaborador is None or df_colaborador.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_colaborador"
            
        if not start_date or not end_date or not convenio_bd:
            return False, pd.DataFrame(), "Input validation failed: missing required parameters"
            
        logger.info(f"Applying contract type logic for {len(df_colaborador)} employees")

        # Define use_case for totals treatmentcalculations
        if use_case == 0:
            treat_l_d = False
            treat_l_dom = True
            treat_lq = False
            treat_lq_og = False
            treat_l_total = True
            treat_c2d = False
            treat_c3d = False
        if use_case == 1:
            treat_l_d = True
            treat_l_dom = True
            treat_lq = True
            treat_lq_og = True
            treat_l_total = True
            treat_c2d = True
            treat_c3d = True            
        
        # TREATMENT LOGIC
        df_result = df_colaborador.copy()
        
        # Convert date strings to datetime for div calculation
        start_dt = pd.to_datetime(start_date, format='%Y-%m-%d')
        end_dt = pd.to_datetime(end_date, format='%Y-%m-%d')
        
        # Calculate div factor for each employee based on admission date (reused from admission function)
        df_result['data_admissao'] = pd.to_datetime(df_result['data_admissao'], errors='coerce')
        needs_admission_adjustment = (
            df_result['data_admissao'].notna() & 
            (start_dt < df_result['data_admissao'])
        )
        
        # Calculate div factor for l_dom_salsa calculations
        div_factors = pd.Series(1.0, index=df_result.index)
        if needs_admission_adjustment.any():
            days_from_admission = (end_dt - df_result.loc[needs_admission_adjustment, 'data_admissao']).dt.days + 1
            total_days = (end_dt - start_dt).days + 1
            div_factors.loc[needs_admission_adjustment] = days_from_admission / total_days
        
        # Apply business logic: C2D = C2D + C3D (vectorized)
        df_result['c2d'] = df_result['c2d'] + df_result['c3d'] if treat_c2d else 0
        
        # Initialize new columns with default values
        new_columns = ['ld', 'l_dom', 'lq_og', 'l_total', 'l_dom_salsa']
        for col in new_columns:
            if col not in df_result.columns:
                df_result[col] = 0
        
        # Process contract type 6 with convenio_bd
        mask_6_bd = (df_result['tipo_contrato'] == 6) & (df_result['convenio'] == convenio_bd)
        if mask_6_bd.any():
            df_result.loc[mask_6_bd, 'ld'] = df_result.loc[mask_6_bd, 'dyf_max_t'] if treat_l_d else 0
            df_result.loc[mask_6_bd, 'l_dom'] = num_fer_dom - df_result.loc[mask_6_bd, 'dyf_max_t'] - fer_fechados if treat_l_dom else 0
            df_result.loc[mask_6_bd, 'lq_og'] = df_result.loc[mask_6_bd, 'lq'].copy() if treat_lq_og else 0
            df_result.loc[mask_6_bd, 'lq'] = df_result.loc[mask_6_bd, 'lq'] - (df_result.loc[mask_6_bd, 'c2d'] + df_result.loc[mask_6_bd, 'c3d']) if treat_lq else 0
            df_result.loc[mask_6_bd, 'l_total'] = num_fer_dom + df_result.loc[mask_6_bd, 'lq'] + df_result.loc[mask_6_bd, 'c2d'] + df_result.loc[mask_6_bd, 'c3d'] if treat_l_total else 0
            df_result.loc[mask_6_bd, 'l_dom_salsa'] = num_sundays * div_factors.loc[mask_6_bd] - df_result.loc[mask_6_bd, 'dyf_max_t'] if treat_l_dom else 0
            
            # Handle negative LQ for contract type 6
            negative_lq_mask = mask_6_bd & (df_result['lq'] < 0)
            if negative_lq_mask.any():
                df_result.loc[negative_lq_mask, 'c3d'] = df_result.loc[negative_lq_mask, 'c3d'] + df_result.loc[negative_lq_mask, 'lq']
                df_result.loc[negative_lq_mask, 'lq'] = 0
                
                # Log warnings for employees with negative LQ
                for idx in df_result[negative_lq_mask].index:
                    matricula = df_result.loc[idx, 'matricula']
                    logger.warning(f"Empleado {matricula} sin suficiente LQ para fines de semana de calidad. Recalculated l_total: {df_result.loc[idx, 'l_total']}")
        
        # Process contract types 5,4 with convenio_bd
        mask_54_bd = (df_result['tipo_contrato'].isin([5, 4])) & (df_result['convenio'] == convenio_bd)
        if mask_54_bd.any():
            df_result.loc[mask_54_bd, 'ld'] = df_result.loc[mask_54_bd, 'dyf_max_t']
            df_result.loc[mask_54_bd, 'l_dom'] = num_fer_dom - df_result.loc[mask_54_bd, 'dyf_max_t'] - fer_fechados
            df_result.loc[mask_54_bd, 'lq_og'] = 0
            df_result.loc[mask_54_bd, 'lq'] = 0
            # Calculate l_total using vectorized operations
            df_result.loc[mask_54_bd, 'l_total'] = num_sundays * (7 - df_result.loc[mask_54_bd, 'tipo_contrato'])
            df_result.loc[mask_54_bd, 'l_dom_salsa'] = num_sundays * div_factors.loc[mask_54_bd] - df_result.loc[mask_54_bd, 'dyf_max_t']
        
        # Process contract types 3,2 with convenio_bd
        mask_32_bd = (df_result['tipo_contrato'].isin([3, 2])) & (df_result['convenio'] == convenio_bd)
        if mask_32_bd.any():
            # Reset all fields for contract types 3,2
            fields_to_reset = ['dyf_max_t', 'q', 'lq_og', 'lq', 'c2d', 'c3d', 'cxx', 'ld']
            for field in fields_to_reset:
                if field in df_result.columns:
                    df_result.loc[mask_32_bd, field] = 0
            
            if matriz_festivos is not None and len(matriz_festivos) > 0 and count_open_holidays_func is not None:
                # Process each contract type separately for holiday calculations
                for tipo_contrato in [3, 2]:
                    tipo_mask = mask_32_bd & (df_result['tipo_contrato'] == tipo_contrato)
                    if tipo_mask.any():
                        coh = count_open_holidays_func(matriz_festivos, tipo_contrato)
                        df_result.loc[tipo_mask, 'l_dom'] = coh[0]
                        df_result.loc[tipo_mask, 'l_total'] = coh[1] - coh[0]
                        
                        # Log warnings for negative l_total
                        negative_total_mask = tipo_mask & (df_result['l_total'] < 0)
                        for idx in df_result[negative_total_mask].index:
                            matricula = df_result.loc[idx, 'matricula']
                            logger.warning(f"Employee {matricula}: negative l_total ({df_result.loc[idx, 'l_total']}) from holidays calc (coh[1]:{coh[1]} - coh[0]:{coh[0]})")
            else:
                df_result.loc[mask_32_bd, 'l_dom'] = 0
                df_result.loc[mask_32_bd, 'l_total'] = 0
        
        # Process contract type 6 with SABECO
        mask_6_sabeco = (df_result['tipo_contrato'] == 6) & (df_result['convenio'] == 'SABECO')
        if mask_6_sabeco.any():
            df_result.loc[mask_6_sabeco, 'ld'] = df_result.loc[mask_6_sabeco, 'dyf_max_t']
            df_result.loc[mask_6_sabeco, 'l_dom'] = num_fer_dom - df_result.loc[mask_6_sabeco, 'dyf_max_t'] - fer_fechados
            df_result.loc[mask_6_sabeco, 'c3d'] = 0
            df_result.loc[mask_6_sabeco, 'lq'] = 0
            df_result.loc[mask_6_sabeco, 'lq_og'] = 0
            df_result.loc[mask_6_sabeco, 'l_total'] = num_fer_dom + df_result.loc[mask_6_sabeco, 'c2d']
        
        # Process contract types 5,4 with SABECO
        mask_54_sabeco = (df_result['tipo_contrato'].isin([5, 4])) & (df_result['convenio'] == 'SABECO')
        if mask_54_sabeco.any():
            df_result.loc[mask_54_sabeco, 'ld'] = df_result.loc[mask_54_sabeco, 'dyf_max_t']
            df_result.loc[mask_54_sabeco, 'l_dom'] = num_fer_dom - df_result.loc[mask_54_sabeco, 'dyf_max_t'] - fer_fechados
            df_result.loc[mask_54_sabeco, 'c3d'] = 0
            df_result.loc[mask_54_sabeco, 'lq'] = 0
            df_result.loc[mask_54_sabeco, 'lq_og'] = 0
            df_result.loc[mask_54_sabeco, 'l_total'] = num_sundays * (7 - df_result.loc[mask_54_sabeco, 'tipo_contrato']) + 8  # 8 is hardcoded per business rule
        
        # Process contract types 3,2 with SABECO
        mask_32_sabeco = (df_result['tipo_contrato'].isin([3, 2])) & (df_result['convenio'] == 'SABECO')
        if mask_32_sabeco.any():
            # Reset fields for SABECO contract types 3,2
            fields_to_reset = ['dyf_max_t', 'q', 'lq', 'lq_og', 'c2d', 'c3d', 'cxx', 'ld']
            for field in fields_to_reset:
                if field in df_result.columns:
                    df_result.loc[mask_32_sabeco, field] = 0
            
            if count_open_holidays_func is not None:
                for tipo_contrato in [3, 2]:
                    tipo_mask = mask_32_sabeco & (df_result['tipo_contrato'] == tipo_contrato)
                    if tipo_mask.any():
                        coh = count_open_holidays_func(matriz_festivos, tipo_contrato)
                        df_result.loc[tipo_mask, 'l_dom'] = coh[0]
                        df_result.loc[tipo_mask, 'l_total'] = coh[1] - coh[0]
                        
                        # Log warnings for negative l_total in SABECO
                        negative_total_mask = tipo_mask & (df_result['l_total'] < 0)
                        for idx in df_result[negative_total_mask].index:
                            matricula = df_result.loc[idx, 'matricula']
                            logger.warning(f"Employee {matricula}: negative l_total ({df_result.loc[idx, 'l_total']}) from SABECO holidays calc (coh[1]:{coh[1]} - coh[0]:{coh[0]})")
            
            # Preserve dofhc value for SABECO (vectorized)
            if 'dofhc' in df_result.columns:
                # dofhc is already preserved, no action needed
                pass
        
        logger.info(f"Successfully applied contract type logic to {len(df_result)} employees")
        return True, df_result, ""
        
    except Exception as e:
        logger.error(f"Error in apply_contract_type_logic_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing failed: {str(e)}"


def add_l_d_to_df_colaborador(
    df_colaborador: pd.DataFrame,
    convenio_bd: str,
    use_case: int = 0,  
) -> Tuple[bool, pd.DataFrame, str]:
    """
    Calculate and assign daily rest quota (l_d/ld) for employees.
    
    L_D represents regular weekday day-off allowances - rest days that can be
    taken on standard working days (Monday-Saturday, excluding Sundays/holidays).
    
    Business Rules by Contract Type:
        - Types 6, 5, 4: ld = dyf_max_t (max Sunday/holiday work days)
        - Types 3, 2: ld = 0 (these contracts don't work weekends typically)
        - Same logic applies for both convenio_bd and SABECO agreements
    
    Quota Context:
        - ld: Regular weekday rest days
        - l_dom: Sunday/holiday rest days
        - l_total: Total rest days (sum of all types)
    
    Use Cases:
        - Case 0: Set l_d = 0 for all employees (disabled)
        - Case 1: Calculate l_d based on contract type and agreement (convenio)
    
    Args:
        df_colaborador: Employee DataFrame with tipo_contrato, convenio, dyf_max_t
        convenio_bd: Database convention identifier (e.g., 'ALCAMPO', 'SABECO')
        use_case: Processing mode (0=disabled, 1=calculate)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated data with ld column
            - error_message (str): Error description if operation failed
    """
    try:
        # Define use_case for totals treatment calculations
        logger.info(f"Adding ld column to df_colaborador.")

        df_result = df_colaborador.copy()

        if use_case == 0:
            df_result['ld'] = 0

        elif use_case == 1:
            # First mask
            mask_6_bd = (df_result['tipo_contrato'] == 6) & (df_result['convenio'] == convenio_bd)
            if mask_6_bd.any():
                df_result.loc[mask_6_bd, 'ld'] = df_result.loc[mask_6_bd, 'dyf_max_t']

            # Second mask
            mask_54_bd = (df_result['tipo_contrato'].isin([5, 4])) & (df_result['convenio'] == convenio_bd)
            if mask_54_bd.any():
                df_result.loc[mask_54_bd, 'ld'] = df_result.loc[mask_54_bd, 'dyf_max_t']

            # Third mask
            mask_32_bd = (df_result['tipo_contrato'].isin([3, 2])) & (df_result['convenio'] == convenio_bd)
            if mask_32_bd.any():
                df_result.loc[mask_32_bd, 'ld'] = 0

            mask_6_sabeco = (df_result['tipo_contrato'] == 6) & (df_result['convenio'] == 'SABECO')
            if mask_6_sabeco.any():
                df_result.loc[mask_6_sabeco, 'ld'] = df_result.loc[mask_6_sabeco, 'dyf_max_t']

            mask_54_sabeco = (df_result['tipo_contrato'].isin([5, 4])) & (df_result['convenio'] == 'SABECO')
            if mask_54_sabeco.any():
                df_result.loc[mask_54_sabeco, 'ld'] = df_result.loc[mask_54_sabeco, 'dyf_max_t']

            # Process contract types 3,2 with SABECO
            mask_32_sabeco = (df_result['tipo_contrato'].isin([3, 2])) & (df_result['convenio'] == 'SABECO')
            if mask_32_sabeco.any():
                df_result.loc[mask_32_sabeco, 'ld'] = 0

        return True, df_result, ""
        
    except Exception as e:
        logger.error(f"Error in add_l_d_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing failed: {str(e)}"                    

def add_l_dom_to_df_colaborador(
    df_colaborador: pd.DataFrame,
    df_feriados: pd.DataFrame,
    convenio_bd: str,
    start_date_str: str,
    end_date_str: str,
    num_sundays: int,
    num_feriados: int,
    num_feriados_fechados: int,
    num_fer_dom: int,
    use_case: int = 0,
) -> Tuple[bool, pd.DataFrame, str]:
    """
    Calculate and assign Sunday/holiday rest quota (l_dom) for employees.
    
    L_DOM represents mandatory rest days on Sundays and holidays. This is a critical
    quota ensuring employees receive appropriate rest on special days per labor law
    and contract agreements.
    
    Calculation Logic by Contract Type and Agreement:
    
        **For Types 6, 5, 4:**
        - Use Case 1 (Salsa): l_dom = (num_sundays × admission_factor) - dyf_max_t
        - Use Case 2 (Alcampo): l_dom = num_fer_dom - dyf_max_t - num_feriados_fechados
        
        **For Types 3, 2:**
        - Calculated via count_open_holidays() function
        - Considers which holidays fall on employee working days
        - Returns [l_dom, l_total] per contract type
        
        **Admission Factor:**
        - Prorates l_dom for employees hired mid-period
        - Factor = days_from_admission / total_days_in_period
    
    Business Context:
        - Ensures labor law compliance for rest days
        - Handles different agreement types (BD vs SABECO)
        - Accounts for store closure days (feriados_fechados)
        - Adjusts for late hires (admission date logic)
    
    Use Cases:
        - Case 0: Set l_dom = 0 for all employees (disabled)
        - Case 1: Salsa model - uses Sunday count with admission adjustment
        - Case 2: Alcampo model - uses combined fer_dom with closed holidays
    
    Args:
        df_colaborador: Employee DataFrame with contract and date information
        df_feriados: Holiday calendar for open holiday calculations
        convenio_bd: Database convention identifier
        start_date_str: Period start date (YYYY-MM-DD)
        end_date_str: Period end date (YYYY-MM-DD)
        num_sundays: Total Sundays in period
        num_feriados: Total holidays in period
        num_feriados_fechados: Holidays when store is closed
        num_fer_dom: Combined Sunday/holiday count
        use_case: Processing mode (0=disabled, 1=Salsa, 2=Alcampo)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated data with l_dom column
            - error_message (str): Error description if operation failed
    """

    try:
        logger.info(f"Adding l_dom column to df_colaborador (full year values only - date adjustments will be applied later).")

        df_result = df_colaborador.copy()

        # Case 0: 
        if use_case == 0:
            df_result['l_dom'] = 0

        # Used by Salsa
        elif use_case == 1:
            # Calculate full-year l_dom values (no date adjustments here - those are done in date_adjustments_to_df_colaborador)
            # First mask
            mask_6_bd = (df_result['tipo_contrato'] == 6) & (df_result['convenio'] == convenio_bd)
            if mask_6_bd.any():
                df_result.loc[mask_6_bd, 'l_dom'] = num_sundays - df_result.loc[mask_6_bd, 'dyf_max_t']

            # Second mask
            mask_54_bd = (df_result['tipo_contrato'].isin([5, 4])) & (df_result['convenio'] == convenio_bd)
            if mask_54_bd.any():
                df_result.loc[mask_54_bd, 'l_dom'] = num_sundays - df_result.loc[mask_54_bd, 'dyf_max_t']

            # Third mask
            mask_32_bd = (df_result['tipo_contrato'].isin([3, 2])) & (df_result['convenio'] == convenio_bd)
            if mask_32_bd.any():
                if df_feriados is not None and len(df_feriados) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:
                        tipo_mask = mask_32_bd & (df_result['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_feriados, tipo_contrato)
                            df_result.loc[tipo_mask, 'l_dom'] = coh[0]
                else:
                    df_result.loc[mask_32_bd, 'l_dom'] = 0
                        

        # Used by alcampo
        elif use_case == 2:
            mask_6_bd = (df_result['tipo_contrato'] == 6) & (df_result['convenio'] == convenio_bd)
            if mask_6_bd.any():
                df_result.loc[mask_6_bd, 'l_dom'] = num_fer_dom - df_result.loc[mask_6_bd, 'dyf_max_t'] - num_feriados_fechados

            mask_54_bd = (df_result['tipo_contrato'].isin([5, 4])) & (df_result['convenio'] == convenio_bd)
            if mask_54_bd.any():
                df_result.loc[mask_54_bd, 'l_dom'] = num_fer_dom - df_result.loc[mask_54_bd, 'dyf_max_t'] - num_feriados_fechados

            # Process contract types 3,2 with convenio_bd
            mask_32_bd = (df_result['tipo_contrato'].isin([3, 2])) & (df_result['convenio'] == convenio_bd)
            if mask_32_bd.any():
                if df_feriados is not None and len(df_feriados) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:
                        tipo_mask = mask_32_bd & (df_result['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_feriados, tipo_contrato)
                            df_result.loc[tipo_mask, 'l_dom'] = coh[0]
                else:
                    df_result.loc[mask_32_bd, 'l_dom'] = 0


            mask_6_sabeco = (df_result['tipo_contrato'] == 6) & (df_result['convenio'] == 'SABECO')
            if mask_6_sabeco.any():
                df_result.loc[mask_6_sabeco, 'l_dom'] = num_fer_dom - df_result.loc[mask_6_sabeco, 'dyf_max_t'] - num_feriados_fechados

            mask_54_bd = (df_result['tipo_contrato'].isin([5, 4])) & (df_result['convenio'] == convenio_bd)
            if mask_54_bd.any():
                df_result.loc[mask_54_bd, 'l_dom'] = num_fer_dom - df_result.loc[mask_54_bd, 'dyf_max_t'] - num_feriados_fechados

            # Process contract types 3,2 with SABECO
            mask_32_sabeco = (df_result['tipo_contrato'].isin([3, 2])) & (df_result['convenio'] == 'SABECO')
            if mask_32_sabeco.any():
                if df_feriados is not None and len(df_feriados) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:                
                        tipo_mask = mask_32_sabeco & (df_result['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_feriados, tipo_contrato)
                            df_result.loc[tipo_mask, 'l_dom'] = coh[0]
                else:
                    df_result.loc[mask_32_bd, 'l_dom'] = 0

        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        return True, df_result, ""

    except Exception as e:
        logger.error(f"Error in add_l_dom_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing l_dom for df_colaborador failed: {str(e)}"      

def set_c2d_to_df_colaborador(df_colaborador: pd.DataFrame, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    Consolidate 2-day and 3-day weekend quotas into c2d column.
    
    C2D (ciclos de 2 días) represents 2-consecutive-day weekend rest periods.
    This function consolidates c2d and c3d quotas into a single c2d field for
    simplified scheduling logic.
    
    Business Logic:
        - Original: c2d = two-day weekends, c3d = three-day weekends
        - After consolidation: c2d = c2d + c3d (combined quota)
        - Simplifies algorithm by treating all quality weekends uniformly
    
    Use Cases:
        - Case 0: Set c2d = 0 for all employees (disabled)
        - Case 1: Consolidate: c2d = c2d + c3d (Salsa & Alcampo standard)
        - Case 2: Preserve existing c2d values (no modification)
    
    Args:
        df_colaborador: Employee DataFrame with c2d and c3d columns
        use_case: Processing mode (0=zero, 1=consolidate, 2=preserve)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated data with consolidated c2d
            - error_message (str): Error description if operation failed
            
    Note:
        After consolidation, c3d is typically reset to 0 in subsequent functions
        to avoid double-counting quality weekend quotas.
    """
    try:
        # Case 0:
        if use_case == 0:
            df_colaborador['c2d'] = 0

        # Case 1: Salsa e Alcampo
        elif use_case == 1:
            df_colaborador['c2d'] = df_colaborador['c2d'] + df_colaborador['c3d']

        elif use_case == 2:
            pass

        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg        

        return True, df_colaborador, ""

    except Exception as e:
        logger.error(f"Error in add_l_dom_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing c2d for df_colaborador failed: {str(e)}"

def set_c3d_to_df_colaborador(df_colaborador: pd.DataFrame, convenio_bd: str, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    Reset or adjust 3-day weekend quota (c3d) after consolidation into c2d.
    
    After consolidating c2d and c3d quotas (via set_c2d_to_df_colaborador), this
    function handles the c3d column to prevent double-counting of quality weekend
    allocations.
    
    Business Rules by Contract Type:
        - Types 6, 5, 4 (convenio_bd): Preserve c3d value (no change)
        - Types 3, 2 (convenio_bd): Reset c3d = 0 (don't work weekends)
        - SABECO Types 6, 5, 4: Reset c3d = 0 (simplified model)
        - SABECO Types 3, 2: Reset c3d = 0 (don't work weekends)
    
    Use Cases:
        - Case 0: Set c3d = 0 for all employees (disabled/zero mode)
        - Case 1: Apply convenio_bd rules (standard business logic)
        - Case 2: Apply both convenio_bd and SABECO rules (full logic)
    
    Args:
        df_colaborador: Employee DataFrame with tipo_contrato, convenio, c3d
        convenio_bd: Database convention identifier
        use_case: Processing mode (0=zero, 1=BD rules, 2=BD+SABECO rules)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated data with adjusted c3d
            - error_message (str): Error description if operation failed
            
    Note:
        This function typically runs after set_c2d_to_df_colaborador which
        consolidates c3d into c2d, making c3d safe to zero out.
    """

    try:
        if use_case == 0:
            df_colaborador['c3d'] = 0
        
        elif use_case == 1:
            mask_6_bd = (df_colaborador['tipo_contrato'] == 6) & (df_colaborador['convenio'] == convenio_bd)
            if mask_6_bd.any():
                pass # no changes needed

            mask_54_bd = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_54_bd.any():
                pass # no changes defined

            mask_32_bd = (df_colaborador['tipo_contrato'].isin([3, 2])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_32_bd.any():
                df_colaborador.loc[mask_32_bd, 'c3d'] = 0

        elif use_case == 2:
            mask_6_bd = (df_colaborador['tipo_contrato'] == 6) & (df_colaborador['convenio'] == convenio_bd)
            if mask_6_bd.any():
                pass # no changes needed

            mask_54_bd = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_54_bd.any():
                pass # no changes defined

            mask_32_bd = (df_colaborador['tipo_contrato'].isin([3, 2])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_32_bd.any():
                df_colaborador.loc[mask_32_bd, 'c3d'] = 0

            mask_6_sabeco = (df_colaborador['tipo_contrato'] == 6) & (df_colaborador['convenio'] == 'SABECO')
            if mask_6_sabeco.any():
                df_colaborador.loc[mask_6_sabeco, 'c3d'] = 0

            mask_54_sabeco = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == 'SABECO')
            if mask_54_sabeco.any():
                df_colaborador.loc[mask_54_sabeco, 'c3d'] = 0

            # Process contract types 3,2 with SABECO
            mask_32_sabeco = (df_colaborador['tipo_contrato'].isin([3, 2])) & (df_colaborador['convenio'] == 'SABECO')
            if mask_32_sabeco.any():
                df_colaborador.loc[mask_32_sabeco, 'c3d'] = 0

        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        return True, df_colaborador, ""

    except Exception as e:
        logger.error(f"Error in add_l_dom_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing l_dom for df_colaborador failed: {str(e)}"

def add_l_q_to_df_colaborador(df_colaborador: pd.DataFrame, convenio_bd: str, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    Calculate quality rest quota (lq) after deducting quality weekend allocations.
    
    LQ (Libranza Quincenal) represents quincenal rest days that remain after subtracting
    quality weekend quotas (c2d, c3d). These are additional flexible rest days
    that can be allocated throughout the scheduling period.
    
    Calculation Formula by Contract Type:
        - Type 6: lq = lq_original - c2d - c3d
        - Type 5, 4: lq = 0 (no quality rest beyond weekends)
        - Type 3, 2: lq = 0 (part-time contracts, no quality rest)
        - SABECO all types: lq = 0 (simplified agreement model)
    
    Business Context:
        Quality rest days are:
        - Premium rest periods beyond mandatory minimums
        - Negotiated in collective bargaining agreements
        - Allocated strategically for work-life balance
        - Subject to operational constraints
    
    Use Cases:
        - Case 0: Set lq = 0 for all employees (disabled)
        - Case 1: Calculate for Salsa model (standard formula)
        - Case 2: Calculate for Alcampo + SABECO models (full logic)
    
    Args:
        df_colaborador: Employee DataFrame with lq, c2d, c3d, tipo_contrato
        convenio_bd: Database convention identifier
        use_case: Processing mode (0=zero, 1=Salsa, 2=Alcampo+SABECO)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated data with calculated lq
            - error_message (str): Error description if operation failed
            
    Warning:
        If lq becomes negative after deduction, it indicates insufficient
        quality rest allocation. Downstream functions should handle this
        by adjusting c3d or flagging the employee for review.
    """
    try:
        if 'lq' in df_colaborador.columns:
            df_colaborador.drop('lq', axis='columns')
        if use_case == 0:
            df_colaborador['lq'] = 0

        # Salsa use_case
        elif use_case == 1:
            mask_6_bd = (df_colaborador['tipo_contrato'] == 6) & (df_colaborador['convenio'] == convenio_bd)
            if mask_6_bd.any():
                df_colaborador.loc[mask_6_bd, 'lq'] = df_colaborador.loc[mask_6_bd, 'lq'] - df_colaborador.loc[mask_6_bd, 'c2d'] - df_colaborador.loc[mask_6_bd, 'c3d']

            mask_54_bd = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_54_bd.any():
                df_colaborador.loc[mask_54_bd, 'lq'] = 0

            mask_32_bd = (df_colaborador['tipo_contrato'].isin([3, 2])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_32_bd.any():
                df_colaborador.loc[mask_32_bd, 'lq'] = 0

        # Alcampo use_case
        elif use_case == 2:
            mask_6_bd = (df_colaborador['tipo_contrato'] == 6) & (df_colaborador['convenio'] == convenio_bd)
            if mask_6_bd.any():
                df_colaborador.loc[mask_6_bd, 'lq'] = df_colaborador.loc[mask_6_bd, 'lq'] - df_colaborador.loc[mask_6_bd, 'c2d'] - df_colaborador.loc[mask_6_bd, 'c3d']

            mask_54_bd = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_54_bd.any():
                df_colaborador.loc[mask_54_bd, 'lq'] = 0

            mask_32_bd = (df_colaborador['tipo_contrato'].isin([3, 2])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_32_bd.any():
                df_colaborador.loc[mask_32_bd, 'lq'] = 0

            mask_6_sabeco = (df_colaborador['tipo_contrato'] == 6) & (df_colaborador['convenio'] == 'SABECO')
            if mask_6_sabeco.any():
                df_colaborador.loc[mask_6_sabeco, 'lq'] = 0

            mask_54_sabeco = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == 'SABECO')
            if mask_54_sabeco.any():
                df_colaborador.loc[mask_54_sabeco, 'lq'] = 0

            # Process contract types 3,2 with SABECO
            mask_32_sabeco = (df_colaborador['tipo_contrato'].isin([3, 2])) & (df_colaborador['convenio'] == 'SABECO')
            if mask_32_sabeco.any():
                df_colaborador.loc[mask_32_sabeco, 'lq'] = 0

        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        return True, df_colaborador, ""


    except Exception as e:
        logger.error(f"Error in add_l_dom_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing l_dom for df_colaborador failed: {str(e)}"    

def add_l_total_to_df_colaborador(df_colaborador: pd.DataFrame, df_feriados: pd.DataFrame, convenio_bd: str, num_sundays: int, num_fer_dom: int, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
    """
    Calculate total rest day quota (l_total) combining all rest day types.
    
    L_TOTAL represents the aggregate annual/period rest day entitlement for each
    employee. It combines mandatory Sunday/holiday rest, quality weekends, and
    regular weekday rest days into a single quota for tracking.
    
    Calculation Formulas by Contract Type:
    
        **Type 6:**
        - l_total = num_fer_dom + lq + c2d + c3d
        - Includes all rest types: sundays, holidays, quality rest, weekends
        
        **Types 5, 4:**
        - l_total = num_sundays × (7 - tipo_contrato)
        - Proportional to contract working days
        - SABECO adds +8 days: num_sundays × (7 - tipo_contrato) + 8
        
        **Types 3, 2:**
        - l_total = count_open_holidays(df_feriados, tipo_contrato)[1]
        - Based on which holidays fall on working days
        - Calculated from holiday matrix analysis
    
    Business Context:
        - Ensures labor law compliance (minimum rest days)
        - Tracks quota consumption throughout year
        - Validates against contract agreements
        - Drives day-off allocation algorithm
    
    Use Cases:
        - Case 0: Set l_total = 0 for all employees (disabled)
        - Case 1: Calculate full l_total for all contract types and agreements
    
    Args:
        df_colaborador: Employee DataFrame with all rest quota components
        df_feriados: Holiday calendar for open holiday calculations (types 3,2)
        convenio_bd: Database convention identifier
        num_sundays: Total Sundays in period
        num_fer_dom: Combined Sunday/holiday count
        use_case: Processing mode (0=zero, 1=calculate)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_colaborador (pd.DataFrame): Updated data with l_total column
            - error_message (str): Error description if operation failed
            
    Note:
        L_total is a critical validation metric. If actual rest days allocated
        don't match l_total, it indicates scheduling conflicts or quota errors.
    """

    try:
        if 'l_total' in df_colaborador.columns:
            df_colaborador.drop('l_total', axis='columns')

        if use_case == 0:
            df_colaborador['l_total'] = 0

        elif use_case == 1:
            mask_6_bd = (df_colaborador['tipo_contrato'] == 6) & (df_colaborador['convenio'] == convenio_bd)
            if mask_6_bd.any():
                df_colaborador.loc[mask_6_bd, 'l_total'] = num_fer_dom + df_colaborador.loc[mask_6_bd, 'lq'] + df_colaborador.loc[mask_6_bd, 'c2d'] + df_colaborador.loc[mask_6_bd, 'c3d']

            mask_54_bd = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_54_bd.any():
                # FIX: Apply vectorized operation - (7 - tipo_contrato) for each row, ensuring integer type
                df_colaborador.loc[mask_54_bd, 'l_total'] = num_sundays * (7 - df_colaborador.loc[mask_54_bd, 'tipo_contrato'].astype(int))

            mask_32_bd = (df_colaborador['tipo_contrato'].isin([3, 2])) & (df_colaborador['convenio'] == convenio_bd)
            if mask_32_bd.any():
                if df_feriados is not None and len(df_feriados) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:
                        tipo_mask = mask_32_bd & (df_colaborador['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_feriados, tipo_contrato)
                            df_colaborador.loc[tipo_mask, 'l_total'] = coh[1]
                else:
                    df_colaborador.loc[mask_32_bd, 'l_total'] = 0

            mask_6_sabeco = (df_colaborador['tipo_contrato'] == 6) & (df_colaborador['convenio'] == 'SABECO')
            if mask_6_sabeco.any():
                # FIX: Use .loc for proper indexing
                df_colaborador.loc[mask_6_sabeco, 'l_total'] = num_fer_dom + df_colaborador.loc[mask_6_sabeco, 'c2d']

            mask_54_sabeco = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == 'SABECO')
            if mask_54_sabeco.any():
                # FIX: Apply vectorized operation - (7 - tipo_contrato) for each row, then add 8, ensuring integer type
                df_colaborador.loc[mask_54_sabeco, 'l_total'] = num_sundays * (7 - df_colaborador.loc[mask_54_sabeco, 'tipo_contrato'].astype(int)) + 8

            # Process contract types 3,2 with SABECO
            mask_32_sabeco = (df_colaborador['tipo_contrato'].isin([3, 2])) & (df_colaborador['convenio'] == 'SABECO')
            if mask_32_sabeco.any():
                if df_feriados is not None and len(df_feriados) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:                
                        tipo_mask = mask_32_sabeco & (df_colaborador['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_feriados, tipo_contrato)
                            df_colaborador.loc[tipo_mask, 'l_total'] = coh[1]
                else:
                    df_colaborador.loc[mask_32_bd, 'l_total'] = 0

        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg            

        return True, df_colaborador, ""

    except Exception as e:
        logger.error(f"Error in add_l_total_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing l_total for df_colaborador failed: {str(e)}"  



def create_df_calendario(
    start_date: str,
    end_date: str,
    main_year: int,
    employee_id_matriculas_map: Dict[str, str],
    past_employees_id_list: List[str],
    df_feriados: pd.DataFrame = None,
) -> Tuple[bool, pd.DataFrame, str]:
    """
    Create df_calendario dataframe with employee schedules for the specified date range using vectorized operations.
    
    Args:
        start_date: Start date as string (YYYY-MM-DD format)
        end_date: End date as string (YYYY-MM-DD format)
        employee_id_matriculas_map: Dictionary mapping employee_ids to matriculas
        past_employees_id_list: List of past employees ids
        df_feriados: Holiday DataFrame used to pre-fill closed days (tipo_feriado='F')
    Returns:
        Tuple[bool, DataFrame, str] with columns: employee_id, data, tipo_turno, horario, wday, dia_tipo, matricula, data_admissao, data_demissao
    """
    try:
        # INPUT VALIDATION
        if not start_date or not end_date:
            return False, pd.DataFrame(), "Input validation failed: missing date parameters"
            
        if not employee_id_matriculas_map or len(employee_id_matriculas_map) == 0:
            return False, pd.DataFrame(), "Input validation failed: empty employee mapping"
            
        logger.info(f"Creating df_calendario from {start_date} to {end_date} for {len(employee_id_matriculas_map)} employees")
        
        # TREATMENT LOGIC
        try:
            # Convert date strings to date format
            previous_monday = get_monday_of_previous_week(f"{main_year}-01-01")
            start_dt = pd.to_datetime(previous_monday, format='%Y-%m-%d')
            next_sunday = get_sunday_of_next_week(f"{main_year}-12-31")
            end_dt = pd.to_datetime(next_sunday, format='%Y-%m-%d')
        except (ValueError, TypeError) as e:
            return False, pd.DataFrame(), "Date parsing failed"
        
        # Generate sequence of dates
        date_range = pd.date_range(start=start_dt, end=end_dt, freq='D')
        
        # Create employee DataFrame
        employees_df = pd.DataFrame(list(employee_id_matriculas_map.items()), 
                                  columns=['employee_id', 'matricula'])
        
        # Create dates DataFrame with weekday calculation
        dates_df = pd.DataFrame({
            'schedule_day': date_range,
            'wd': date_range.weekday + 1  # Convert to 1-7 (Monday-Sunday)
        })
        
        # Create shifts DataFrame
        shifts_df = pd.DataFrame({'tipo_turno': ['M', 'T']})
        
        # Create cartesian product using cross merge
        try:
            # First: employees × dates
            emp_dates = employees_df.assign(key=1).merge(dates_df.assign(key=1), on='key').drop('key', axis=1)
            
            # Second: (employees × dates) × shifts
            df_calendario = emp_dates.assign(key=1).merge(shifts_df.assign(key=1), on='key').drop('key', axis=1)
        except Exception as e:
            logger.warning(f"Cartesian product creation failed: {e}")
            return False, pd.DataFrame(), "Calendar creation failed"
        
        # Vectorized operations for final formatting
        try:
            df_calendario['employee_id'] = df_calendario['employee_id'].astype(str)
            df_calendario['matricula'] = df_calendario['matricula'].astype(str)
            df_calendario['schedule_day'] = df_calendario['schedule_day'].dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Data formatting failed: {e}")
            return False, pd.DataFrame(), "Data formatting failed"
        
        # Add empty columns
        df_calendario['horario'] = ''
        df_calendario['dia_tipo'] = ''

        # Flag closed holidays (tipo_feriado == 'F') upfront so later steps preserve them
        try:
            if df_feriados is not None and not df_feriados.empty:
                if {'schedule_day', 'tipo_feriado'}.issubset(df_feriados.columns):
                    df_closed = df_feriados.copy()
                    df_closed['tipo_feriado'] = df_closed['tipo_feriado'].astype(str).str.upper()
                    df_closed = df_closed[df_closed['tipo_feriado'] == 'F']
                    if not df_closed.empty:
                        df_closed['schedule_day'] = pd.to_datetime(df_closed['schedule_day']).dt.strftime('%Y-%m-%d')
                        closed_dates = df_closed['schedule_day'].unique()
                        mask_closed = df_calendario['schedule_day'].isin(closed_dates)
                        if mask_closed.any():
                            df_calendario.loc[mask_closed, 'horario'] = 'F'
                else:
                    logger.warning("df_feriados missing schedule_day or tipo_feriado columns; skipping closed-day flagging")
        except Exception as e:
            logger.warning(f"Failed to pre-fill closed holidays in df_calendario: {e}")
        
        # Reorder columns
        column_order = ['employee_id', 'schedule_day', 'tipo_turno', 'horario', 'wd', 'dia_tipo', 'matricula']
        df_calendario = df_calendario[column_order]
        
        # Sort by employee_id, date, and shift type for consistent ordering
        df_calendario = df_calendario.sort_values(['employee_id', 'schedule_day', 'tipo_turno']).reset_index(drop=True)
        
        # OUTPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Calendar creation resulted in empty DataFrame"
            
        logger.info(f"Created df_calendario with {len(df_calendario)} rows ({len(employee_id_matriculas_map)} employees × {len(date_range)} days × 2 shifts)")
        
        return True, df_calendario, ""
        
    except Exception as e:
        logger.error(f"Error in create_df_calendario: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""


def add_seq_turno(df_calendario: pd.DataFrame, df_colaborador: pd.DataFrame):
    """
    Populate df_calendario's horario column based on employee shift sequences using vectorized operations.
    
    The horario column indicates which shift type the employee works that week:
    - If weekly pattern is 'M': only tipo_turno='M' rows get horario='M', tipo_turno='T' rows get '0'
    - If weekly pattern is 'T': only tipo_turno='T' rows get horario='T', tipo_turno='M' rows get '0'
    - If pattern is 'MoT', 'P', or 'CICLO': both tipo_turno='M' and tipo_turno='T' rows get that value
    
    Args:
        df_calendario: Calendar dataframe with columns [employee_id, data, tipo_turno, ww, ...]
        df_colaborador: Employee dataframe with columns [employee_id, seq_turno, semana1, ...]
        
    Returns:
        Tuple[bool, DataFrame, str]: (success, updated_calendario, error_message)
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty or len(df_calendario) < 1:
            return False, pd.DataFrame(), "df_calendario is empty"

        if df_colaborador.empty or len(df_colaborador) < 1:
            return False, pd.DataFrame(), "df_colaborador is empty"
        
        # Check required columns in calendario
        required_cal_cols = ['employee_id', 'ww', 'tipo_turno']
        missing_cols = [col for col in required_cal_cols if col not in df_calendario.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"df_calendario missing required columns: {missing_cols}"
        
        # Check required columns in colaborador
        required_colab_cols = ['employee_id', 'seq_turno']
        missing_cols = [col for col in required_colab_cols if col not in df_colaborador.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"df_colaborador missing required columns: {missing_cols}"
        
        logger.info(f"Starting add_seq_turno for {len(df_calendario)} calendario rows and {len(df_colaborador)} employees")
        
        # Create copy to avoid modifying original
        df_result = df_calendario.copy()
        
        # TREATMENT LOGIC - FULLY VECTORIZED
        
        # Get max week number
        max_week = int(df_result['ww'].max())
        
        # Create week range dataframe
        weeks_df = pd.DataFrame({'ww': range(1, max_week + 1)})
        
        # Create employee-week combinations (cross join)
        df_emp_weeks = df_colaborador[['employee_id', 'seq_turno']].copy()
        if 'semana1' in df_colaborador.columns:
            df_emp_weeks['semana1'] = df_colaborador['semana1']
        else:
            df_emp_weeks['semana1'] = 'M'  # Default
        
        df_emp_weeks['key'] = 1
        weeks_df['key'] = 1
        df_emp_weeks = df_emp_weeks.merge(weeks_df, on='key').drop('key', axis=1)
        
        # Handle null values
        df_emp_weeks['seq_turno'] = df_emp_weeks['seq_turno'].fillna('T')
        df_emp_weeks['semana1'] = df_emp_weeks['semana1'].fillna('M')
        
        # Calculate cycle length and position in cycle
        # TODO: This definition shouldnt be done here
        cycle_length_map = {
            'M': 1, 'T': 1, 'MoT': 1, 'P': 1, 'CICLO': 1,
            'MT': 2, 
            'MMT': 3, 'MTT': 3
        }
        df_emp_weeks['cycle_len'] = df_emp_weeks['seq_turno'].map(cycle_length_map).fillna(1).astype(int)
        df_emp_weeks['week_in_cycle'] = (df_emp_weeks['ww'] - 1) % df_emp_weeks['cycle_len']
        
        # Apply pattern logic vectorized using apply with the helper function
        df_emp_weeks['pattern'] = df_emp_weeks.apply(
            lambda row: get_week_pattern(row['seq_turno'], row['semana1'], row['week_in_cycle']),
            axis=1
        )
        
        # Keep only needed columns for merge
        df_emp_weeks = df_emp_weeks[['employee_id', 'ww', 'pattern']]
        
        logger.info(f"Generated {len(df_emp_weeks)} employee-week pattern combinations")
        
        # Merge pattern information back to calendario
        df_result = df_result.merge(df_emp_weeks, on=['employee_id', 'ww'], how='left')
        
        # Fill any missing patterns with '0'
        df_result['pattern'] = df_result['pattern'].fillna('0')
        
        # Preserve any horario values already filled (e.g., closed holidays)
        prefilled_mask = df_result['horario'].notna() & (df_result['horario'] != '')

        # Normalize pattern to uppercase for consistent matching
        df_result['pattern_upper'] = df_result['pattern'].str.upper()
        
        # Vectorized horario assignment using np.select
        conditions = [
            # Special patterns that work both shifts
            df_result['pattern_upper'].isin(['MOT', 'P']),
            # CICLO employees get 'MoT' as default (flexible - overridden by add_ciclos_completos where data exists)
            df_result['pattern_upper'] == 'CICLO',
            # Morning pattern matches morning shift
            (df_result['pattern_upper'] == 'M') & (df_result['tipo_turno'] == 'M'),
            # Afternoon pattern matches afternoon shift
            (df_result['pattern_upper'] == 'T') & (df_result['tipo_turno'] == 'T'),
        ]
        
        choices = [
            df_result['pattern'],  # MoT/P: use the pattern value
            'MoT',                 # CICLO: default to MoT (flexible scheduling)
            'M',                   # Morning shift on morning week
            'T',                   # Afternoon shift on afternoon week
        ]
        
        # Apply conditions and set default to '0' for non-matches
        calculated_horario = np.select(conditions, choices, default='0')
        update_mask = ~prefilled_mask
        if update_mask.any():
            df_result.loc[update_mask, 'horario'] = calculated_horario[update_mask.to_numpy()]
        
        # Drop temporary helper columns
        df_result = df_result.drop(['pattern', 'pattern_upper'], axis=1, errors='ignore')
        
        # OUTPUT VALIDATION
        if 'horario' not in df_result.columns:
            return False, pd.DataFrame(), "Failed to create horario column"
        
        horario_counts = df_result['horario'].value_counts()
        logger.info(f"Successfully populated horario column. Value counts: {horario_counts.to_dict()}")
        logger.info(f"Completed add_seq_turno for {len(df_result)} rows")
        
        return True, df_result, ""
        
    except Exception as e:
        logger.error(f"Error in add_seq_turno: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Failed to add seq_turno: {str(e)}"

def add_calendario_passado(df_calendario: pd.DataFrame, df_calendario_passado: pd.DataFrame, use_case: int = 1) -> Tuple[bool, pd.DataFrame, str]:
    """
    Populate calendar schedule gaps using historical shift data.
    
    This function fills schedule entries (horario) in the current calendar by
    looking up corresponding dates in historical calendar data. It ensures continuity
    for employees whose schedules carry over from previous periods.
    
    Matching Logic:
        - Matches by: (employee_id, schedule_day) - exact employee and date combination
        - Overrides any value EXCEPT F's (closed holidays) and V's (vacations)
        - Only fills with valid historical values (not empty/null/'-')
        - Uses vectorized MultiIndex lookup for performance
        - Priority: Preserves F's and V's from earlier steps
    
    Business Context:
        Historical data is critical for:
        - Maintaining ongoing multi-week rotation cycles
        - Preserving established shift patterns
        - Ensuring accurate rest day quota tracking
        - Continuity across planning period boundaries
    
    Use Cases:
        - Case 0: No processing (return calendar as-is)
        - Case 1: Override gaps using historical data (standard mode)
    
    Args:
        df_calendario: Current calendar DataFrame with schedule data
        df_calendario_passado: Historical calendar DataFrame with past schedules
        use_case: Processing mode (0=disabled, 1=fill gaps)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_calendario (pd.DataFrame): Calendar with filled historical data
            - error_message (str): Success message with fill count or error details
            
    Example:
        Current:    | employee_id | data       | horario |
                    | 101         | 2024-01-15 |         |
        
        Historical: | employee_id | data       | horario |
                    | 101         | 2024-01-15 | M       |
        
        Result:     | employee_id | data       | horario |
                    | 101         | 2024-01-15 | M       | (override from historical)
                    
        Note: If horario was 'F' or 'V', it would be preserved (not overridden)
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # USE CASE LOGIC
        if use_case == 0:
            logger.info("use_case == 0: returning df_calendario as is")
            return True, df_calendario, "No processing applied (use_case=0)"
        elif use_case == 1:
            # TREATMENT LOGIC FOR USE CASE 1
            if df_calendario_passado.empty:
                logger.info("df_calendario_passado is empty, returning original df_calendario")
                return True, df_calendario, ""
                
            # Check required columns exist
            required_cols_calendario = ['employee_id', 'schedule_day', 'horario']
            required_cols_passado = ['employee_id', 'schedule_day', 'horario']
            
            for col in required_cols_calendario:
                if col not in df_calendario.columns:
                    return False, pd.DataFrame(), f"Missing required column '{col}' in df_calendario"
            for col in required_cols_passado:
                if col not in df_calendario_passado.columns:
                    return False, pd.DataFrame(), f"Missing required column '{col}' in df_calendario_passado"
            
            df_result = df_calendario.copy()
            
            # Ensure employee_id types match (convert to string for consistent matching)
            df_result['employee_id'] = df_result['employee_id'].astype(str)
            df_calendario_passado_temp = df_calendario_passado.copy()
            df_calendario_passado_temp['employee_id'] = df_calendario_passado_temp['employee_id'].astype(str)
            
            # Ensure schedule_day is string format in both
            if df_result['schedule_day'].dtype != 'object':
                df_result['schedule_day'] = pd.to_datetime(df_result['schedule_day']).dt.strftime('%Y-%m-%d')
            if df_calendario_passado_temp['schedule_day'].dtype != 'object':
                df_calendario_passado_temp['schedule_day'] = pd.to_datetime(df_calendario_passado_temp['schedule_day']).dt.strftime('%Y-%m-%d')
            
            # Create lookup Series from df_calendario_passado using MultiIndex
            passado_lookup = df_calendario_passado_temp.set_index(['employee_id', 'schedule_day'])['horario']
            
            # Create MultiIndex for df_result to enable vectorized lookup
            result_index = df_result.set_index(['employee_id', 'schedule_day']).index
            
            # Vectorized lookup: map passado values to result positions
            mapped_values = result_index.map(passado_lookup)
            matches_found = mapped_values.notna().sum()
            logger.info(f"Found {matches_found} matches from df_calendario_passado")
            
            # Create mask for valid values from passado (not empty/null)
            # Note: Allow '-' values (from type='N' conversion) to override default '0' values
            valid_passado_mask = mapped_values.notna() & (mapped_values != '')
            
            # Create mask to preserve F's (closed holidays) and V's (vacations)
            preserve_f_mask = df_result['horario'] == 'F'
            preserve_v_mask = df_result['horario'] == 'V'
            preserve_mask = preserve_f_mask | preserve_v_mask
            
            # Combine masks: override everything except F's and V's where passado has valid data
            fill_mask = valid_passado_mask & ~preserve_mask
            
            # Vectorized assignment
            df_result.loc[fill_mask, 'horario'] = mapped_values[fill_mask]
            
            filled_count = fill_mask.sum()
            
            # Log NL value statistics
            nl_values_from_passado = (mapped_values == 'NL').sum() if mapped_values is not None else 0
            nl_values_in_result = (df_result['horario'] == 'NL').sum()
            horario_counts_after = df_result['horario'].value_counts()
            
            logger.info(f"Overridden {filled_count} horario values from df_calendario_passado (preserved {preserve_mask.sum()} F/V values)")
            logger.info(f"add_calendario_passado: NL values from passado lookup: {nl_values_from_passado}, NL values in result after merge: {nl_values_in_result}")
            logger.info(f"add_calendario_passado: horario value counts after merge: {horario_counts_after.to_dict()}")
            
            # OUTPUT VALIDATION
            if df_result.empty:
                return False, pd.DataFrame(), "Result DataFrame is empty after processing"
                
            return True, df_result, f"Successfully filled {filled_count} horario values from historical data"
        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        logger.error(f"Error in add_calendario_passado: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Error processing calendario data: {str(e)}"

def add_ausencias_ferias(df_calendario: pd.DataFrame, df_ausencias_ferias: pd.DataFrame, use_case: int = 1) -> Tuple[bool, pd.DataFrame, str]:
    """
    Integrate employee absence and vacation records into calendar schedules.
    
    This function maps absence/vacation types from the absence tracking system into
    the calendar's horario field. It ensures that pre-approved absences, vacations,
    and other leave types are properly reflected in the work schedule.
    
    Absence Type Mapping:
        - 'V': Vacation (férias) - scheduled vacation periods
        - 'A': General absence - sick leave, personal days, etc.
        - Other codes: Preserved as-is from absence tracking system
    
    Matching Logic:
        - Matches by: (employee_id/fk_colaborador, data)
        - OVERRIDES existing horario values (except 'F' for closed holidays which are preserved)
        - Only fills with valid absence codes (not empty/null/'-')
        - Uses vectorized MultiIndex lookup for efficient processing
        - Note: Runs after add_seq_turno, so it overrides shift assignments with absences
    
    Business Context:
        - Prevents scheduling employees during approved absences
        - Maintains accurate attendance tracking
        - Ensures vacation entitlements are respected
        - Supports labor law compliance for leave tracking
    
    Use Cases:
        - Case 0: No processing (return calendar as-is)
        - Case 1: Integrate absence data into schedule (standard mode)
    
    Args:
        df_calendario: Calendar DataFrame with employee schedules
        df_ausencias_ferias: Absence/vacation DataFrame with tipo_ausencia codes
        use_case: Processing mode (0=disabled, 1=integrate absences)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_calendario (pd.DataFrame): Calendar with integrated absence data
            - error_message (str): Success message with fill count or error details
            
    Example:
        Calendar:  | employee_id | data       | horario |
                   | 101         | 2024-01-15 | M       |
                   | 101         | 2024-01-16 |         |
        
        Absences:  | employee_id | data       | tipo_ausencia |
                   | 101         | 2024-01-16 | V            |
        
        Result:    | employee_id | data       | horario |
                   | 101         | 2024-01-15 | M       |
                   | 101         | 2024-01-16 | V       |
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # USE CASE LOGIC
        if use_case == 0:
            logger.info("use_case == 0: returning df_calendario as is")
            return True, df_calendario, "No processing applied (use_case=0)"
        elif use_case == 1:
            # TREATMENT LOGIC FOR USE CASE 1
            if df_ausencias_ferias.empty:
                logger.info("df_ausencias_ferias is empty, returning original df_calendario")
                return True, df_calendario, ""
            
            df_result = df_calendario.copy()
            
            # Create lookup Series from df_ausencias_ferias using MultiIndex
            employee_col = 'employee_id' if 'employee_id' in df_ausencias_ferias.columns else 'fk_colaborador'
            
            # Create a temporary mapping column in ausencias to match with schedule_day
            df_ausencias_temp = df_ausencias_ferias.copy()
            df_ausencias_temp[employee_col] = df_ausencias_temp[employee_col].astype(str)
            df_ausencias_temp['_schedule_day_lookup'] = pd.to_datetime(df_ausencias_temp['data']).dt.strftime('%Y-%m-%d')
            
            ausencias_lookup = df_ausencias_temp.set_index([employee_col, '_schedule_day_lookup'])['tipo_ausencia']
            
            # Create MultiIndex for df_result to enable vectorized lookup
            result_employee_col = 'employee_id' if 'employee_id' in df_result.columns else 'fk_colaborador'
            cal_employee = df_result[result_employee_col].astype(str)
            cal_schedule = pd.to_datetime(df_result['schedule_day']).dt.strftime('%Y-%m-%d')
            result_index = pd.MultiIndex.from_arrays([cal_employee, cal_schedule])
            
            # Vectorized lookup: map ausencias values to result positions
            mapped_values = result_index.map(ausencias_lookup)
            matches_found = mapped_values.notna().sum()
            logger.info(f"Found {matches_found} matches from df_ausencias_ferias")
            
            # Create mask for valid values from ausencias (not empty/null)
            valid_ausencias_mask = mapped_values.notna() & (mapped_values != '') & (mapped_values != '-')
            
            # OVERRIDE LOGIC: Absences should override shift assignments (except F=closed holidays)
            preserve_f_mask = df_result['horario'] == 'F'
            fill_mask = valid_ausencias_mask & ~preserve_f_mask
            
            # Vectorized assignment
            df_result.loc[fill_mask, 'horario'] = mapped_values[fill_mask]
            
            filled_count = fill_mask.sum()
            logger.info(f"Filled {filled_count} empty horario values from df_ausencias_ferias")
            
            return True, df_result, f"Successfully filled {filled_count} horario values from ausencias data"
        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        error_msg = f"Error in add_ausencias_ferias: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg


def add_folgas_ciclos(df_calendario: pd.DataFrame, df_core_pro_emp_horario_det: pd.DataFrame, use_case: int = 1) -> Tuple[bool, pd.DataFrame, str]:
    """
    Apply fixed day-off cycles to calendar schedules (override mode).
    
    This function enforces predefined rest day patterns by overriding existing schedule
    entries with day-off codes. Unlike fill operations, this OVERRIDES any existing
    horario values to ensure fixed rest days are respected.
    
    Day-Off Logic:
        - Source: tipo_dia = 'F' (folga/day-off) or 'S' (no-work) in cycle definition
        - After treatment: 'F' → 'L', 'S' → '-'
        - For 'L' values: Target horario = 'L' (libre/rest day) - OVERRIDE mode
          * Preserves F's, A's and V's (does not override these)
        - For '-' values (no-work days): Special handling:
          * If current horario is 'A' → sets to 'A-'
          * If current horario is 'V' → sets to 'V-'
          * If current horario is 'F' → preserved as 'F'
          * Otherwise → sets to '-'
        - Mode: OVERRIDE - replaces existing horario values (except F's, A's, V's)
    
    Business Context:
        Fixed cycles represent:
        - Negotiated rest day patterns from labor agreements
        - Mandatory rest days that cannot be moved
        - Predictable schedules for work-life balance
        - Recurring weekly or bi-weekly patterns
    
    Matching Logic:
        - Matches by: (employee_id, schedule_day converted to data)
        - Filters source for tipo_dia = 'L' (day-off) or '-' (no-work) after treatment
        - Uses vectorized MultiIndex for efficient bulk updates
        - Preservation rules:
          * F's (closed holidays): Always preserved, never overridden
          * A's (absences): Preserved from 'L' overrides, but '-' converts to 'A-'
          * V's (vacations): Preserved from 'L' overrides, but '-' converts to 'V-'
        - Special handling for '-' values (no-work days):
          * If current horario is 'A' → sets to 'A-'
          * If current horario is 'V' → sets to 'V-'
          * If current horario is 'F' → preserved as 'F'
          * Otherwise → sets to '-'
    
    Use Cases:
        - Case 0: No processing (return calendar as-is)
        - Case 1: Apply fixed day-off overrides (standard mode)
    
    Args:
        df_calendario: Calendar DataFrame with employee schedules
        df_core_pro_emp_horario_det: Cycle details with tipo_dia markers
        use_case: Processing mode (0=disabled, 1=apply overrides)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_calendario (pd.DataFrame): Calendar with day-off overrides applied
            - error_message (str): Success message with override count or error details
            
    Example:
        Calendar: | employee_id | data       | horario |
                  | 101         | 2024-01-15 | M       |
        
        Cycles:   | employee_id | schedule_day | tipo_dia |
                  | 101         | 2024-01-15   | L        |
        
        Result:   | employee_id | data       | horario |
                  | 101         | 2024-01-15 | L       |  (overridden from cycles)
                  
        Note: If horario was 'F' (closed holiday), it would be preserved (not overridden)
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # USE CASE LOGIC
        if use_case == 0:
            logger.info("use_case == 0: returning df_calendario as is")
            return True, df_calendario, "No processing applied (use_case=0)"
        elif use_case == 1:
            # TREATMENT LOGIC FOR USE CASE 1
            if df_core_pro_emp_horario_det.empty:
                logger.info("df_core_pro_emp_horario_det is empty, returning original df_calendario")
                return True, df_calendario, ""
            
            df_result = df_calendario.copy()
            
            # Filter for both day-offs (tipo_dia = 'L' after treatment) and no-work days (tipo_dia = '-' after treatment)
            # Note: treat_df_folgas_ciclos converts 'F' → 'L' and 'S' → '-'
            df_dayoffs = df_core_pro_emp_horario_det[
                df_core_pro_emp_horario_det['tipo_dia'].isin(['L', '-'])
            ].copy()
            
            if df_dayoffs.empty:
                logger.info("No day-off or no-work records found, returning original df_calendario")
                return True, df_result, ""
            
            # Ensure schedule_day is in string format for matching
            if df_dayoffs['schedule_day'].dtype != 'object':
                df_dayoffs['schedule_day'] = pd.to_datetime(df_dayoffs['schedule_day']).dt.strftime('%Y-%m-%d')
            
            # Create lookup Series from day-offs using MultiIndex
            dayoffs_lookup = df_dayoffs.set_index(['employee_id', 'schedule_day'])['tipo_dia']
            
            # Create MultiIndex for df_result to enable vectorized lookup
            result_index = df_result.set_index(['employee_id', 'schedule_day']).index
            
            # Vectorized lookup: map day-off values to result positions
            mapped_values = result_index.map(dayoffs_lookup)
            
            # Create mask to preserve F's (closed holidays) - do not override closed holidays
            preserve_f_mask = df_result['horario'] == 'F'
            
            # Create mask for A's and V's - these should only be modified by '-' (becoming A- or V-)
            preserve_av_mask = df_result['horario'].isin(['A', 'V'])
            
            # Process 'L' values (day-offs): override with 'L', but preserve F's, A's and V's
            dayoff_l_mask = (mapped_values == 'L') & ~preserve_f_mask & ~preserve_av_mask
            if dayoff_l_mask.any():
                df_result.loc[dayoff_l_mask, 'horario'] = 'L'
            
            # Process '-' values (no-work days): check if current horario is 'A' or 'V'
            # If inserting '-' and current is 'A' → 'A-', if current is 'V' → 'V-', otherwise '-'
            # Note: F's should never be overridden, even by '-'
            dash_mask = (mapped_values == '-') & ~preserve_f_mask
            if dash_mask.any():
                # Get current horario values for rows where we're inserting '-'
                current_horario = df_result.loc[dash_mask, 'horario']
                
                # Vectorized conditional assignment: A → A-, V → V-, otherwise -
                df_result.loc[dash_mask, 'horario'] = np.where(
                    current_horario == 'A',
                    'A-',
                    np.where(current_horario == 'V', 'V-', '-')
                )
            
            # Count overrides
            filled_count = dayoff_l_mask.sum() + dash_mask.sum()
            preserved_f_count = ((mapped_values == 'L') & preserve_f_mask).sum() + ((mapped_values == '-') & preserve_f_mask).sum()
            preserved_av_count = ((mapped_values == 'L') & preserve_av_mask).sum()
            logger.info(f"Applied {filled_count} day-off/no-work overrides from df_core_pro_emp_horario_det ({dayoff_l_mask.sum()} L, {dash_mask.sum()} -) (preserved {preserved_f_count} F values, {preserved_av_count} A/V values)")
            
            return True, df_result, f"Successfully applied {filled_count} day-off overrides"
        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        error_msg = f"Error in add_folgas_ciclos: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg

def add_ciclos_completos(df_calendario: pd.DataFrame, df_ciclos_completos: pd.DataFrame, use_case: int = 1) -> Tuple[bool, pd.DataFrame, str]:
    """
    Integrate complete 90-day rotation cycle schedules into calendar.
    
    This function fills calendar gaps with shift assignments from employees who follow
    complete 90-day rotation cycles (CICLO COMPLETO). These are comprehensive schedules
    that define every working day and rest day over a 90-day period.
    
    Schedule Source Priority:
        1. codigo_trads: Translated/standardized shift codes (preferred)
        2. horario_ind: Individual schedule codes (fallback)
    
    Business Context:
        90-day complete cycles:
        - Provide long-term schedule predictability
        - Balance work patterns over extended periods
        - Ensure fair distribution of shifts and rest days
        - Common in retail/hospitality for complex staffing needs
    
    Matching Logic:
        - Matches by: (employee_id, schedule_day converted to data)
        - Only fills with valid cycle codes (not empty/null)
        - Uses vectorized MultiIndex for performance
        - Preservation rules:
          * F's (closed holidays): Always preserved, never overridden
          * A's (absences): Preserved from regular shift codes, but 'L' and '-' can override
          * V's (vacations): Preserved from regular shift codes, but 'L' and '-' can override
        - Special handling for 'L' values (day-offs):
          * Can override A's and V's (but not F's)
        - Special handling for '-' values (no-work days from tipo_dia='S'):
          * If current horario is 'A' → sets to 'A-'
          * If current horario is 'V' → sets to 'V-'
          * If current horario is 'F' → preserved as 'F'
          * Otherwise → sets to '-'
    
    Use Cases:
        - Case 0: No processing (return calendar as-is)
        - Case 1: Override with complete cycle data (standard mode)
    
    Args:
        df_calendario: Calendar DataFrame with employee schedules
        df_ciclos_completos: Complete cycle DataFrame with 90-day schedules
        use_case: Processing mode (0=disabled, 1=fill with cycles)
        
    Returns:
        Tuple containing:
            - success (bool): True if operation succeeded
            - df_calendario (pd.DataFrame): Calendar with cycle data integrated
            - error_message (str): Success message with fill count or error details
            
    Example:
        Calendar:  | employee_id | data       | horario |
                   | 101         | 2024-01-15 |         |
        
        Cycles:    | employee_id | schedule_day | codigo_trads |
                   | 101         | 2024-01-15   | M           |
        
        Result:    | employee_id | data       | horario |
                   | 101         | 2024-01-15 | M       |
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # USE CASE LOGIC
        if use_case == 0:
            logger.info("use_case == 0: returning df_calendario as is")
            return True, df_calendario, "No processing applied (use_case=0)"

        elif use_case == 1:
            # TREATMENT LOGIC FOR USE CASE 1
            if df_ciclos_completos.empty:
                logger.info("df_ciclos_completos is empty, returning original df_calendario")
                return True, df_calendario, ""
            
            df_result = df_calendario.copy()
            
            # Ensure schedule_day is in string format for matching
            df_ciclos_mapped = df_ciclos_completos.copy()
            
            # Ensure employee_id types match (convert to string for consistent matching)
            df_result['employee_id'] = df_result['employee_id'].astype(str)
            df_ciclos_mapped['employee_id'] = df_ciclos_mapped['employee_id'].astype(str)
            
            if df_result['schedule_day'].dtype != 'object':
                df_result['schedule_day'] = pd.to_datetime(df_result['schedule_day']).dt.strftime('%Y-%m-%d')
            if df_ciclos_mapped['schedule_day'].dtype != 'object':
                df_ciclos_mapped['schedule_day'] = pd.to_datetime(df_ciclos_mapped['schedule_day']).dt.strftime('%Y-%m-%d')
            
            # Use horario (from convert_ciclos_to_horario) if available, then codigo_trads, otherwise horario_ind as fallback
            if 'horario' in df_ciclos_mapped.columns:
                horario_col = 'horario'
            elif 'codigo_trads' in df_ciclos_mapped.columns:
                horario_col = 'codigo_trads'
            else:
                horario_col = 'horario_ind'
            
            # Create lookup Series from df_ciclos_90 using MultiIndex
            ciclos_lookup = df_ciclos_mapped.set_index(['employee_id', 'schedule_day'])[horario_col]
            
            # Create MultiIndex for df_result to enable vectorized lookup
            result_index = df_result.set_index(['employee_id', 'schedule_day']).index
            
            # Vectorized lookup: map ciclos values to result positions
            mapped_values = result_index.map(ciclos_lookup)
            matches_found = mapped_values.notna().sum()
            logger.info(f"Found {matches_found} matches from df_ciclos_completos")
            
            # Create mask for valid values from ciclos (not empty/null)
            # Note: Allow '-' values (skip days from tipo_dia='S') to override default '0' values
            valid_ciclos_mask = mapped_values.notna() & (mapped_values != '')
            
            # Create mask to preserve F's (closed holidays) - F's should never be overridden
            preserve_f_mask = df_result['horario'] == 'F'
            
            # Create mask for A's and V's - these should only be modified by '-' (becoming A- or V-)
            preserve_av_mask = df_result['horario'].isin(['A', 'V'])
            
            # Special handling for '-' values: check if current horario is 'A' or 'V'
            # If inserting '-' and current is 'A' → 'A-', if current is 'V' → 'V-', otherwise '-'
            # Note: F's should never be overridden, even by '-'
            dash_mask = valid_ciclos_mask & ~preserve_f_mask & (mapped_values == '-')
            if dash_mask.any():
                # Get current horario values for rows where we're inserting '-'
                current_horario = df_result.loc[dash_mask, 'horario']
                
                # Vectorized conditional assignment: A → A-, V → V-, otherwise -
                df_result.loc[dash_mask, 'horario'] = np.where(
                    current_horario == 'A',
                    'A-',
                    np.where(current_horario == 'V', 'V-', '-')
                )
            
            # Process 'L' values: L can override A's and V's (but not F's)
            l_mask = valid_ciclos_mask & ~preserve_f_mask & (mapped_values == 'L')
            if l_mask.any():
                df_result.loc[l_mask, 'horario'] = 'L'
            
            # For other non-dash, non-L values, assign normally but preserve F's, A's and V's
            # A's and V's should NOT be overridden by regular shift codes (only by '-' or 'L')
            other_mask = valid_ciclos_mask & ~preserve_f_mask & ~preserve_av_mask & (mapped_values != '-') & (mapped_values != 'L')
            if other_mask.any():
                df_result.loc[other_mask, 'horario'] = mapped_values[other_mask]
            
            # Calculate fill_mask for logging purposes (combines dash, L, and other updates)
            fill_mask = dash_mask | l_mask | other_mask
            
            filled_count = fill_mask.sum()
            
            # Log NL value statistics
            nl_values_from_ciclos = (mapped_values == 'NL').sum() if mapped_values is not None else 0
            nl_values_in_result = (df_result['horario'] == 'NL').sum()
            horario_counts_after = df_result['horario'].value_counts()
            
            # Count A/V values that were preserved (not overridden by regular shift codes, but may have been overridden by L or -)
            preserved_av_from_shifts = (preserve_av_mask & valid_ciclos_mask & (mapped_values != '-') & (mapped_values != 'L')).sum()
            logger.info(f"Overridden {filled_count} horario values from df_ciclos_completos ({dash_mask.sum()} -, {l_mask.sum()} L, {other_mask.sum()} other) (preserved {preserve_f_mask.sum()} F values, {preserved_av_from_shifts} A/V from shift codes)")
            logger.info(f"add_ciclos_completos: NL values from ciclos lookup: {nl_values_from_ciclos}, NL values in result after merge: {nl_values_in_result}")
            logger.info(f"add_ciclos_completos: horario value counts after merge: {horario_counts_after.to_dict()}")
            
            return True, df_result, f"Successfully filled {filled_count} horario values from completos cycles"
        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        error_msg = f"Error in add_ciclos_completos: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg

def add_days_off(df_calendario: pd.DataFrame, df_days_off: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Integrate additional day-off allocations into calendar schedules.
    
    This function is a placeholder for future day-off integration logic. It will handle
    supplementary rest day assignments that don't fit into the standard categories
    (historical, absences, cycles).
    
    Potential Use Cases (To Be Implemented):
        - Manual day-off assignments from management
        - Compensatory rest days for overtime
        - Special accommodation days
        - Emergency schedule adjustments
    
    Current Status:
        Function signature exists but business logic is not yet implemented.
        Returns input calendar unchanged.
    
    Args:
        df_calendario: Calendar DataFrame with employee schedules
        df_days_off: Additional day-off assignments (structure TBD)
        
    Returns:
        Tuple containing:
            - success (bool): True (always succeeds currently)
            - df_calendario (pd.DataFrame): Unmodified calendar
            - error_message (str): Empty string (no errors)
            
    Note:
        Implementation pending business requirements definition.
        TODO: Add implementation when business logic is defined.
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # TREATMENT LOGIC
        logger.info(f"Adding df_days_off to df_calendario. Not implemented yet.")
        # TODO: Add implementation when business logic is defined
        
        # OUTPUT VALIDATION
        return True, df_calendario, ""
        
    except Exception as e:
        logger.error(f"Error in add_days_off: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""


def adjust_estimativas_special_days(df_estimativas: pd.DataFrame, special_days_list: List[str] = None, use_case: int = 0) -> Tuple[bool, pd.DataFrame, str]:
    """
    Adjust min_turno for special dates (Christmas, New Year season) to match max_turno.
    
    Business Logic:
    - Peak shopping season dates (Dec 23, 24, 30, 31) require maximum staffing (no flexibility)
    - Specific Friday mornings (Dec 22, 29) also require maximum morning shift staffing
    
    Args:
        df_estimativas: Workload estimates dataframe with columns: data, turno, min_turno, max_turno
        special_days_list: Optional list of special dates. If empty/None, uses default December dates based on year in data
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, modified dataframe, error message)
    """
    try:
        # INPUT VALIDATION
        if df_estimativas.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_estimativas"
        
        required_cols = ['data', 'turno', 'min_turno', 'max_turno']
        missing_cols = [col for col in required_cols if col not in df_estimativas.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"Input validation failed: missing columns {missing_cols}"

        if use_case == 0:
            logger.info("use_case == 0: returning df_estimativas as is")
            return True, df_estimativas, "No processing applied (use_case=0)"

        elif use_case == 1:
        
            # TREATMENT LOGIC
            df_estimativas_adjusted = df_estimativas.copy()
            
            # If no special_days_list provided, create default list based on year from data
            # TODO: Remove this when special dates added to settings
            if special_days_list is None or len(special_days_list) == 0:
                try:
                    # Get year from the minimum date in df_estimativas
                    ano = pd.to_datetime(df_estimativas_adjusted['data'].min()).year
                    logger.info(f"Using year {ano} from df_estimativas for special dates")
                    
                    # Define special dates (Christmas and New Year season)
                    special_dates = [
                        f'{ano}-12-23',  # December 23
                        f'{ano}-12-24',  # Christmas Eve
                        f'{ano}-12-30',  # December 30
                        f'{ano}-12-31'   # New Year's Eve
                    ]
                    
                    # Define special Friday dates (morning shifts only)
                    friday_dates = [
                        f'{ano}-12-22',  # Friday before Christmas
                        f'{ano}-12-29'   # Friday before New Year
                    ]
                except Exception as e:
                    logger.warning(f"Could not extract year from data, using empty special dates: {e}")
                    special_dates = []
                    friday_dates = []
            else:
                # Use provided special_days_list
                # Assume it contains both types of dates (you can enhance this later)
                special_dates = special_days_list
                friday_dates = []
                logger.info(f"Using provided special_days_list with {len(special_dates)} dates")
            
            # Adjust min_turno = max_turno for all special dates (both M and T shifts)
            if len(special_dates) > 0:
                mask_special = df_estimativas_adjusted['data'].isin(special_dates)
                df_estimativas_adjusted.loc[mask_special, 'min_turno'] = df_estimativas_adjusted.loc[mask_special, 'max_turno']
                logger.info(f"Adjusted {mask_special.sum()} rows for special dates: {special_dates}")
            
            # Adjust min_turno = max_turno for Friday morning shifts only
            if len(friday_dates) > 0:
                mask_friday = (
                    (df_estimativas_adjusted['data'].isin(friday_dates)) & 
                    (df_estimativas_adjusted['turno'] == 'M')
                )
                df_estimativas_adjusted.loc[mask_friday, 'min_turno'] = df_estimativas_adjusted.loc[mask_friday, 'max_turno']
                logger.info(f"Adjusted {mask_friday.sum()} rows for Friday morning shifts: {friday_dates}")
            
            # OUTPUT VALIDATION
            if df_estimativas_adjusted.empty:
                return False, pd.DataFrame(), "Treatment resulted in empty DataFrame"
            
            logger.info(f"Successfully adjusted estimativas for special days. Shape: {df_estimativas_adjusted.shape}")
            return True, df_estimativas_adjusted, ""

        else: 
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg            

    except Exception as e:
        logger.error(f"Error in adjust_estimativas_special_days: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), str(e)

def filter_df_dates(df: pd.DataFrame, first_date_str: str, last_date_str: str, date_col_name: str = 'data', use_case: int = 0) -> Tuple[bool, pd.DataFrame, str]:
    """
    Filter dataframe by date range. Works for any dataframe with a date column.
    
    This function is agnostic to the dataframe type - works for df_calendario, 
    df_estimativas, or any dataframe with a date column.
    
    Args:
        df: Input dataframe with a date column
        first_date_str: Start date in 'YYYY-MM-DD' format
        last_date_str: End date in 'YYYY-MM-DD' format
        date_col_name: Name of the date column (default: 'data', can be 'DATA' for calendario)
        use_case: Use case for the function. 0: no processing, 1: processing
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, filtered dataframe, error message)
    """
    try:
        # INPUT VALIDATION
        if df.empty or len(df) == 0:
            return False, pd.DataFrame(), "Input validation failed: empty dataframe"
        
        if date_col_name not in df.columns:
            # Try alternate column names
            alternate_cols = ['data', 'DATA', 'date', 'DATE']
            date_col_found = None
            for col in alternate_cols:
                if col in df.columns:
                    date_col_found = col
                    logger.info(f"Date column '{date_col_name}' not found, using '{col}' instead")
                    break
            
            if date_col_found is None:
                return False, pd.DataFrame(), f"Input validation failed: no date column found. Searched for: {alternate_cols}"
            
            date_col_name = date_col_found
        
        if not first_date_str or not last_date_str:
            return False, pd.DataFrame(), "Input validation failed: invalid date parameters"

        if use_case == 0:
            logger.info("use_case == 0: returning df as is")
            return True, df, "No processing applied (use_case=0)"

        elif use_case == 1:
        
            # Validate date format
            try:
                pd.to_datetime(first_date_str, format="%Y-%m-%d")
                pd.to_datetime(last_date_str, format="%Y-%m-%d")
            except Exception as e:
                return False, pd.DataFrame(), f"Input validation failed: invalid date format. Expected 'YYYY-MM-DD': {e}"
            
            # TREATMENT LOGIC
            df_filtered = df.copy()
            
            # Convert date column to datetime if it isn't already
            if not pd.api.types.is_datetime64_any_dtype(df_filtered[date_col_name]):
                try:
                    # Clean DATA column (remove everything after underscore if present)
                    if df_filtered[date_col_name].dtype == 'object':
                        df_filtered[date_col_name] = df_filtered[date_col_name].str.replace(r'_.*$', '', regex=True)
                    
                    # Convert to datetime
                    df_filtered[date_col_name] = pd.to_datetime(df_filtered[date_col_name])
                    logger.info(f"Converted {date_col_name} column to datetime")
                except Exception as e:
                    return False, pd.DataFrame(), f"Error converting {date_col_name} to datetime: {e}"
            
            # Convert filter dates to datetime
            first_date = pd.to_datetime(first_date_str, format="%Y-%m-%d")
            last_date = pd.to_datetime(last_date_str, format="%Y-%m-%d")
            
            # Apply date filter
            original_count = len(df_filtered)
            df_filtered = df_filtered[
                (df_filtered[date_col_name] >= first_date) & 
                (df_filtered[date_col_name] <= last_date)
            ]
            filtered_count = len(df_filtered)
            
            logger.info(f"Filtered dataframe by dates: {first_date_str} to {last_date_str}")
            logger.info(f"Rows before: {original_count}, after: {filtered_count}, removed: {original_count - filtered_count}")
            
            # OUTPUT VALIDATION
            if df_filtered.empty:
                return False, pd.DataFrame(), f"Filtering resulted in empty dataframe. Date range {first_date_str} to {last_date_str} has no data."
            
            return True, df_filtered, ""

        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        logger.error(f"Error in filter_df_dates: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), str(e)


def extract_tipos_turno(df_calendario: pd.DataFrame, tipo_turno_col: str = 'TIPO_TURNO') -> Tuple[bool, List[str], str]:
    """
    Extract unique shift types (tipos de turno) from calendario dataframe.
    
    This is a simple extraction function that identifies all shift types present in the schedule.
    The list is used for:
    - Determining if special shift processing is needed (MoT, P)
    - Validation and logging
    - Algorithm decision-making
    
    Common shift types:
    - 'M': Morning (Mañana)
    - 'T': Afternoon (Tarde)
    - 'MoT': Morning or Afternoon (ambiguous - needs processing)
    - 'P': Split shift (Partido - works both M and T)
    - 'F': Holiday (Feriado)
    - 'L': Day off (Livre)
    - 'V': Vacation (Férias)
    - 'A', 'DFS', 'OUT', 'NL': Other statuses
    
    Args:
        df_calendario: Calendar dataframe with shift assignments
        tipo_turno_col: Name of the shift type column (default: 'TIPO_TURNO')
        
    Returns:
        Tuple[bool, List[str], str]: (success, list of shift types, error message)
    """
    try:
        # INPUT VALIDATION
        if df_calendario is None or df_calendario.empty:
            return False, [], "Input validation failed: empty or None dataframe"
        
        if tipo_turno_col not in df_calendario.columns:
            return False, [], f"Input validation failed: column '{tipo_turno_col}' not found in dataframe"
        
        # EXTRACTION LOGIC
        tipos_turno_list = df_calendario[tipo_turno_col].unique().tolist()
        
        # Remove None/NaN values if present
        tipos_turno_list = [t for t in tipos_turno_list if pd.notna(t)]
        
        # Sort for consistency (optional but helpful for logging)
        tipos_turno_list = sorted(tipos_turno_list)
        
        # OUTPUT VALIDATION
        if len(tipos_turno_list) == 0:
            return False, [], "No shift types found in dataframe"
        
        logger.info(f"Extracted {len(tipos_turno_list)} unique shift types: {tipos_turno_list}")
        
        # Log if special shift types are present
        special_types = [t for t in tipos_turno_list if t in ['MoT', 'P']]
        if special_types:
            logger.info(f"Special shift types detected (will need processing): {special_types}")
        
        return True, tipos_turno_list, ""
        
    except Exception as e:
        error_msg = f"Error extracting tipos_turno: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, [], error_msg


def process_special_shift_types(df_calendario: pd.DataFrame, shift_type: str, employee_col: str = 'COLABORADOR', date_col: str = 'DATA', shift_col: str = 'TIPO_TURNO', use_case: int = 0) -> Tuple[bool, pd.DataFrame, str]:
    """
    Process ambiguous shift types (MoT, P) by converting them to concrete M/T assignments.
    
    This function handles two special shift types:
    
    1. **'MoT'** (Morning or Tarde): Employee can work either morning OR afternoon
       - Ambiguous assignment that needs to be resolved
       - First occurrence → 'M' (morning)
       - Second occurrence → 'T' (afternoon)
    
    2. **'P'** (Partida/Split shift): Employee works BOTH morning AND afternoon
       - Creates two separate shift entries
       - First occurrence → 'M' (morning shift)
       - Second occurrence → 'T' (afternoon shift)
    
    Algorithm:
    - Groups rows by employee and date
    - For each group with the special shift type:
      - 1st row → converts to 'M'
      - 2nd row → converts to 'T'
    - Combines processed rows back with unprocessed rows
    
    Args:
        df_calendario: Calendar dataframe with shift assignments
        shift_type: The special shift type to process ('MoT' or 'P')
        employee_col: Column name for employee identifier (default: 'COLABORADOR')
        date_col: Column name for date (default: 'DATA')
        shift_col: Column name for shift type (default: 'TIPO_TURNO')
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, processed dataframe, error message)
    
    Example:
        Input:
            COLABORADOR | DATA       | TIPO_TURNO
            123         | 2024-01-15 | P
            123         | 2024-01-15 | P
        
        Output:
            COLABORADOR | DATA       | TIPO_TURNO
            123         | 2024-01-15 | M
            123         | 2024-01-15 | T
    """
    try:
        # INPUT VALIDATION
        if df_calendario is None or df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty or None dataframe"
        
        required_cols = [employee_col, date_col, shift_col]
        missing_cols = [col for col in required_cols if col not in df_calendario.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"Input validation failed: missing columns {missing_cols}"
        
        if not shift_type or shift_type not in ['MoT', 'P']:
            return False, pd.DataFrame(), f"Input validation failed: shift_type must be 'MoT' or 'P', got '{shift_type}'"
        
        if use_case == 0:
            logger.info("use_case == 0: returning df_estimativas as is")
            return True, df_calendario, "No processing applied (use_case=0)"

        elif use_case == 1:
            # TREATMENT LOGIC
            df_result = df_calendario.copy()
            
            # Filter rows that match the special shift type
            mask_special = df_result[shift_col] == shift_type
            df_filtered = df_result[mask_special].copy()
            
            if len(df_filtered) > 0:
                logger.info(f"Processing {len(df_filtered)} rows with shift type '{shift_type}'")
                
                # Group by employee and date, then assign M/T to HORARIO based on row position
                def assign_shift_type(group):
                    """Assign M to first occurrence, T to second occurrence in HORARIO column."""
                    group = group.copy()
                    
                    if len(group) >= 1:
                        # First occurrence becomes Morning shift in HORARIO
                        group.loc[group.index[0], 'HORARIO'] = 'M'
                    
                    if len(group) >= 2:
                        # Second occurrence becomes Tarde (afternoon) shift in HORARIO
                        group.loc[group.index[1], 'HORARIO'] = 'T'
                    
                    if len(group) > 2:
                        logger.warning(f"Employee {group[employee_col].iloc[0]} has {len(group)} '{shift_type}' shifts on {group[date_col].iloc[0]} - only first 2 processed")
                    
                    return group
                
                # Apply the transformation
                df_filtered = (df_filtered
                            .groupby([employee_col, date_col], group_keys=False)
                            .apply(assign_shift_type)
                            .reset_index(drop=True))
                
                # Get rows that don't have the special shift type
                df_rest = df_result[~mask_special].copy()
                
                # Combine processed and unprocessed rows
                df_result = pd.concat([df_rest, df_filtered], ignore_index=True)
                
                # Count how many shifts were converted in HORARIO
                converted_m = (df_filtered['HORARIO'] == 'M').sum()
                converted_t = (df_filtered['HORARIO'] == 'T').sum()
                logger.info(f"Converted '{shift_type}' HORARIO: {converted_m} to 'M', {converted_t} to 'T'")
            else:
                logger.info(f"No '{shift_type}' shift types found in dataframe - skipping processing")
            
            # OUTPUT VALIDATION
            if df_result.empty:
                return False, pd.DataFrame(), "Processing resulted in empty dataframe"
            
            # Verify the special shift type was removed
            remaining_special = (df_result[shift_col] == shift_type).sum()
            if remaining_special > 0:
                logger.warning(f"After processing, {remaining_special} '{shift_type}' shifts still remain (might be >2 occurrences per employee-date)")
            
            logger.info(f"Successfully processed special shift type '{shift_type}'")
            return True, df_result, ""

        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        error_msg = f"Error processing special shift type '{shift_type}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg


def add_date_related_columns(df: pd.DataFrame, date_col: str = 'data', add_id_col: bool = False, use_case: int = 0, main_year: int = None, first_date: str = None, last_date: str = None) -> Tuple[bool, pd.DataFrame, str]:
    """
    Add date-related columns to dataframe (index, WDAY, WW, WD).
    
    Agnostic function that works for df_calendario, df_estimativas and df_feriados.
    
    Args:
        df: Input dataframe with date column
        date_col: Name of date column ('data' for estimativas, 'schedule_day' for calendario)
        add_id_col: Whether to add index column (row index) - usually only for calendario
        use_case: Processing mode (0=dynamic indexing, 1=fixed indexing for calendario)
        main_year: Main year for fixed indexing (required when use_case=1)
        first_date: Reference start date for consistent indexing (use_case=0)
        last_date: Reference end date for consistent indexing (use_case=0)
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, dataframe with new columns, error message)
        
    Note:
        The 'index' column assigns sequential IDs (1, 2, 3, ...) to each unique date
        in chronological order, identifying the position of each day in the calendar range.
        When first_date and last_date are provided, ensures consistent indexing across
        multiple dataframes (df_calendario, df_estimativas, df_feriados).
    """
    try:
        # INPUT VALIDATION
        if df is None or df.empty:
            return False, pd.DataFrame(), "Input validation failed: empty or None dataframe"
        
        if date_col not in df.columns:
            return False, pd.DataFrame(), f"Input validation failed: column '{date_col}' not found"
        
        # TREATMENT LOGIC
        df_result = df.copy()
        
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df_result[date_col]):
            df_result[date_col] = pd.to_datetime(df_result[date_col])
        
        # Add index column: sequential ID for each unique date (1, 2, 3, ...)
        if use_case == 0:
            if first_date is not None and last_date is not None:
                # Create mapping from reference date range (for consistent indexing across dataframes)
                reference_range = pd.date_range(start=first_date, end=last_date, freq='D')
                date_to_index = {date: idx + 1 for idx, date in enumerate(reference_range)}
                df_result['index'] = df_result[date_col].map(date_to_index)
                
                # Handle dates outside the reference range
                if df_result['index'].isna().any():
                    max_index = df_result['index'].max()
                    if pd.isna(max_index):
                        max_index = 0
                    df_result['index'] = df_result['index'].fillna(max_index + 1)
                df_result['index'] = df_result['index'].astype(int)
            else:
                # Original behavior: dynamic indexing from dataframe's own dates
                unique_dates = sorted(df_result[date_col].unique())
                date_to_index = {date: idx + 1 for idx, date in enumerate(unique_dates)}
                df_result['index'] = df_result[date_col].map(date_to_index).astype(int)
        elif use_case == 1:
            # Fixed indexing: 22-12-[year-1] to 04-01-[year+1]
            # This ensures index matches between df_estimativas (01-01 to 31-12) and df_calendario (23-12 to 04-01)
            if main_year is None:
                return False, pd.DataFrame(), "Input validation failed: main_year is required when use_case=1"
            
            # Create fixed date range: 22-12-[year-1] to 04-01-[year+1]
            start_date = get_monday_of_previous_week(f"{main_year}-01-01")
            end_date = get_sunday_of_next_week(f"{main_year}-12-31")
            fixed_date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Create mapping: fixed range dates to sequential IDs (23-12 always = 1, 04-01 always = last)
            date_to_index = {date: idx + 1 for idx, date in enumerate(fixed_date_range)}
            df_result['index'] = df_result[date_col].map(date_to_index)
            
            # Fill any missing indices (dates outside the fixed range) with max+1
            if df_result['index'].isna().any():
                max_index = df_result['index'].max()
                if pd.isna(max_index):
                    max_index = 0
                df_result['index'] = df_result['index'].fillna(max_index + 1)
            
            # Convert to int to ensure integer values, not floats
            df_result['index'] = df_result['index'].astype(int)
        
        # Add WDAY (1=Monday, 7=Sunday)
        df_result['wday'] = df_result[date_col].dt.dayofweek + 1
        
        # Add WW (adjusted ISO week)
        df_result['ww'] = df_result[date_col].apply(adjusted_isoweek)
        
        # Add WD (3-letter weekday name)
        df_result['wd'] = df_result[date_col].dt.day_name().str[:3]

        # Add year column
        df_result['year'] = df_result[date_col].dt.year
        
        # Add ID column if requested (usually for calendario)
        if add_id_col:
            df_result['ID'] = range(len(df_result))
        
        logger.info(f"Added date-related columns: index, wday, ww, wd" + (" and ID" if add_id_col else ""))
        return True, df_result, ""
        
    except Exception as e:
        error_msg = f"Error adding date-related columns: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg


def define_dia_tipo(df: pd.DataFrame, df_feriados: pd.DataFrame = None, date_col: str = 'data', tipo_turno_col: str = 'TIPO_TURNO', horario_col: str = 'HORARIO', wd_col: str = 'WD') -> Tuple[bool, pd.DataFrame, str]:
    """
    Define dia_tipo (day type) column - identifies Sundays and holidays.
    
    Business Logic:
    - 'domYf' (domingo y feriado): Sundays or holidays requiring special rest day handling
    - Regular weekday name (Mon, Tue, etc.): Normal working days
    
    A date is marked as 'domYf' if:
    - The date is present in df_feriados['schedule_day'] (holiday), OR
    - The date is a Sunday (WD == 'Sun')
    - AND the specific row's HORARIO != 'F' (not the holiday row itself)
    
    Requires: WD column must exist (run add_date_related_columns first)
    
    Args:
        df: Input dataframe
        df_feriados: Holiday dataframe with 'schedule_day' column containing holiday dates
        date_col: Date column name (default: 'data')
        tipo_turno_col: Shift type column (default: 'TIPO_TURNO') - kept for backward compatibility
        horario_col: Work status column (default: 'HORARIO')
        wd_col: Weekday name column (default: 'WD')
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, dataframe with dia_tipo column, error message)
    """
    try:
        # INPUT VALIDATION
        if df is None or df.empty:
            return False, pd.DataFrame(), "Input validation failed: empty or None dataframe"
        
        required_cols = [date_col, horario_col, wd_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"Input validation failed: missing columns {missing_cols}. Run add_date_related_columns first."
        
        # Prepare holiday dates set from df_feriados
        holiday_dates = set()
        if df_feriados is not None and not df_feriados.empty:
            if 'schedule_day' not in df_feriados.columns:
                logger.warning("df_feriados provided but missing 'schedule_day' column, using only Sunday check")
            else:
                # Convert schedule_day to date format matching df's date_col format
                try:
                    # Normalize dates to string format for comparison
                    df_feriados_dates = pd.to_datetime(df_feriados['schedule_day']).dt.strftime('%Y-%m-%d')
                    holiday_dates = set(df_feriados_dates.astype(str))
                    logger.info(f"Loaded {len(holiday_dates)} holiday dates from df_feriados")
                except Exception as e:
                    logger.warning(f"Error processing df_feriados dates: {e}, using only Sunday check")
        
        # TREATMENT LOGIC
        df_result = df.copy()
        
        # Drop any existing uppercase DIA_TIPO column to avoid duplicates
        if 'DIA_TIPO' in df_result.columns:
            logger.info("Removing existing uppercase DIA_TIPO column to avoid duplicates")
            df_result = df_result.drop(columns=['DIA_TIPO'], errors='ignore')
        
        # Normalize date column to string format for comparison
        if not pd.api.types.is_datetime64_any_dtype(df_result[date_col]):
            df_result[date_col] = pd.to_datetime(df_result[date_col])
        df_result['_date_str'] = df_result[date_col].dt.strftime('%Y-%m-%d')
        
        def assign_dia_tipo(group):
            """Assign dia_tipo for each date group."""
            # Get the date string for this group
            date_str = group['_date_str'].iloc[0]
            
            # Check if this date is a holiday (in df_feriados) OR is a Sunday
            is_holiday = date_str in holiday_dates
            is_sunday = group[wd_col].iloc[0] == 'Sun'
            
            # Apply logic row by row within the date group
            group['dia_tipo'] = group.apply(
                lambda row: 'domYf' if ((is_holiday or is_sunday) and row[horario_col] != 'F') else row[wd_col],
                axis=1
            )
            return group
        
        # Group by date and apply the logic
        df_result = df_result.groupby(date_col, group_keys=False).apply(assign_dia_tipo)
        
        # Clean up temporary column
        df_result = df_result.drop(columns=['_date_str'], errors='ignore')
        
        # Count results
        domyf_count = (df_result['dia_tipo'] == 'domYf').sum()
        logger.info(f"Defined dia_tipo column: {domyf_count} rows marked as 'domYf' (Sundays/holidays)")
        
        return True, df_result, ""
        
    except Exception as e:
        error_msg = f"Error defining dia_tipo: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg


def merge_contract_data(df_calendario: pd.DataFrame, df_contratos: pd.DataFrame, employee_col: str = 'COLABORADOR', date_col: str = 'DATA') -> Tuple[bool, pd.DataFrame, str]:
    """
    Merge contract information into calendario dataframe.
    
    Cross-dataframe operation that adds contract details (contract_id, carga_diaria) 
    to each employee-date row in the calendar.
    
    Args:
        df_calendario: Calendar dataframe with employee schedules
        df_contratos: Contracts dataframe with employee contract information
        employee_col: Employee identifier column in calendario (default: 'COLABORADOR')
        date_col: Date column name (default: 'DATA')
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, calendario with contract data, error message)
    """
    try:
        # INPUT VALIDATION
        if df_calendario is None or df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_calendario"
        
        if df_contratos is None or df_contratos.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_contratos"
        
        if employee_col not in df_calendario.columns:
            return False, pd.DataFrame(), f"Input validation failed: {employee_col} not in df_calendario"
        
        if date_col not in df_calendario.columns:
            return False, pd.DataFrame(), f"Input validation failed: {date_col} not in df_calendario"
        
        # TREATMENT LOGIC
        df_result = df_calendario.copy()
        df_contratos_work = df_contratos.copy()
        
        # Ensure required columns exist in df_contratos
        required_contract_cols = ['employee_id', 'matricula', 'contract_id', 'carga_diaria']
        if 'schedule_day' in df_contratos_work.columns:
            required_contract_cols.append('schedule_day')
        missing_cols = [col for col in required_contract_cols if col not in df_contratos_work.columns]
        if missing_cols:
            logger.warning(f"df_contratos missing columns: {missing_cols}. Merge may have limited data.")
        
        # Normalize types for merge - convert employee identifiers to string for consistent matching
        if employee_col in df_result.columns:
            df_result[employee_col] = df_result[employee_col].astype(str)
        if 'matricula' in df_result.columns:
            df_result['matricula'] = df_result['matricula'].astype(str)
        if 'matricula' in df_contratos_work.columns:
            df_contratos_work['matricula'] = df_contratos_work['matricula'].astype(str)
        
        # Normalize date column in both dataframes to string format '%Y-%m-%d' for consistent merging
        # Ensure both sides of the merge have the same date format (string)
        if date_col in df_result.columns:
            try:
                # Convert to datetime first, then to string format
                df_result[date_col] = pd.to_datetime(df_result[date_col], errors='coerce')
                df_result[date_col] = df_result[date_col].dt.strftime('%Y-%m-%d')
                logger.info(f"Normalized {date_col} in df_calendario to string format '%Y-%m-%d'")
            except Exception as e:
                logger.warning(f"Failed to normalize {date_col} in df_calendario: {e}")
        
        if date_col in df_contratos_work.columns:
            try:
                # Convert to datetime first, then to string format to match calendario
                df_contratos_work[date_col] = pd.to_datetime(df_contratos_work[date_col], errors='coerce')
                df_contratos_work[date_col] = df_contratos_work[date_col].dt.strftime('%Y-%m-%d')
                logger.info(f"Normalized {date_col} in df_contratos to string format '%Y-%m-%d'")
            except Exception as e:
                logger.warning(f"Failed to normalize {date_col} in df_contratos: {e}")
        
        # Determine merge keys - use matricula on both sides if available (common identifier)
        if 'matricula' in df_result.columns and 'matricula' in df_contratos_work.columns:
            # Use matricula on both sides (more reliable than employee_id = matricula)
            merge_left_on = ['matricula', date_col]
            merge_right_on = ['matricula', date_col]
            logger.info(f"Merging on matricula and {date_col}")
        else:
            # Fallback to original logic if matricula not available
            merge_left_on = [employee_col, date_col]
            merge_right_on = ['matricula', date_col]
            logger.info(f"Merging on {employee_col} (left) = matricula (right) and {date_col}")
        
        # Merge contract information
        original_count = len(df_result)
        merge_cols = ['matricula', date_col, 'contract_id', 'carga_diaria']
        if 'employee_id' in df_contratos_work.columns:
            merge_cols.append('employee_id')
        
        df_result = df_result.merge(
            df_contratos_work[merge_cols],
            left_on=merge_left_on,
            right_on=merge_right_on,
            how='left'
        )
        
        # Handle duplicate columns from merge
        # When merging, if right side has 'employee_id' but we merge on different columns,
        # pandas may create 'employee_id_y' for the right side version
        # Preserve the original employee_col column (which is 'employee_id' in this case)
        if 'employee_id_y' in df_result.columns:
            df_result = df_result.drop(columns=['employee_id_y'], errors='ignore')
        if 'employee_id_x' in df_result.columns:
            df_result = df_result.rename(columns={'employee_id_x': 'employee_id'})
        
        # Preserve employee_col if it's employee_id (needed for next step)
        # Only drop matricula from right side if it exists as a duplicate
        if 'matricula_y' in df_result.columns:
            df_result = df_result.drop(columns=['matricula_y'], errors='ignore')
        if 'matricula_x' in df_result.columns:
            df_result = df_result.rename(columns={'matricula_x': 'matricula'})
        
        # IMPORTANT: Do not drop employee_id if employee_col == 'employee_id' (needed by next function)
        # The original employee_id column must be preserved
        
        # OUTPUT VALIDATION
        if len(df_result) != original_count:
            logger.warning(f"Row count changed after merge: {original_count} -> {len(df_result)}")
        
        # Check if contract data was added
        if 'contract_id' in df_result.columns:
            null_contracts = df_result['contract_id'].isnull().sum()
            if null_contracts > 0:
                logger.warning(f"{null_contracts} rows have null contract_id after merge")
            logger.info(f"Successfully merged contract data. Added columns: contract_id, carga_diaria")
        else:
            logger.warning("contract_id column not found after merge")
        
        return True, df_result, ""
        
    except Exception as e:
        error_msg = f"Error merging contract data: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg


def adjust_counters_for_contract_types(df_colaborador: pd.DataFrame, tipo_contrato_col: str = 'tipo_contrato', use_case: int = 0) -> Tuple[bool, pd.DataFrame, str]:
    """
    Adjust Sunday/holiday counters for contract type 3 employees.
    
    Contract type 3 employees don't work weekends, so their total_dom_fes (Sundays/holidays 
    worked) and dyf_max_t (max Sundays/holidays allowed) should be set to 0.
    
    Note: This function expects total_dom_fes and dyf_max_t columns to exist (initialized
    in treat_df_colaborador). It will only adjust values, not create columns.
    
    Args:
        df_colaborador: Employee dataframe with contract information
        tipo_contrato_col: Contract type column name (default: 'tipo_contrato')
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, adjusted colaborador df, error message)
    """
    try:
        # INPUT VALIDATION
        if df_colaborador is None or df_colaborador.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_colaborador"
        
        if tipo_contrato_col not in df_colaborador.columns:
            return False, pd.DataFrame(), f"Input validation failed: {tipo_contrato_col} not in df_colaborador"
        
        required_cols = ['total_dom_fes', 'dyf_max_t']
        missing_cols = [col for col in required_cols if col not in df_colaborador.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"Input validation failed: missing columns {missing_cols}. These should be initialized in treat_df_colaborador."
        
        if use_case == 0:
            logger.info("use_case == 0: returning df_estimativas as is")
            return True, df_colaborador, "No processing applied (use_case=0)"

        elif use_case == 1: 
            # TREATMENT LOGIC
            df_result = df_colaborador.copy()
            
            # Count contract type 3 employees before adjustment
            type_3_mask = df_result[tipo_contrato_col] == 3
            type_3_count = type_3_mask.sum()
            
            if type_3_count == 0:
                logger.info("No contract type 3 employees found, skipping adjustment")
                return True, df_result, ""
            
            # Set Sunday/holiday counters to 0 for type 3 contracts
            df_result.loc[type_3_mask, 'total_dom_fes'] = 0
            df_result.loc[type_3_mask, 'dyf_max_t'] = 0
            
            # OUTPUT VALIDATION
            # Verify adjustments were applied
            type_3_total_dom_fes = df_result.loc[type_3_mask, 'total_dom_fes'].sum()
            type_3_dyf_max_t = df_result.loc[type_3_mask, 'dyf_max_t'].sum()
            
            if type_3_total_dom_fes != 0 or type_3_dyf_max_t != 0:
                logger.warning(f"Adjustment may have failed: type 3 employees have non-zero values")
            
            logger.info(f"Adjusted {type_3_count} contract type 3 employees: set total_dom_fes=0, dyf_max_t=0")
            
            return True, df_result, ""

        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        error_msg = f"Error adjusting contract type 3: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg


def handle_employee_edge_cases(df_colaborador: pd.DataFrame, df_calendario: pd.DataFrame, employee_col: str = 'COLABORADOR', matricula_col: str = 'matricula') -> Tuple[bool, pd.DataFrame, pd.DataFrame, str]:
    """
    Handle special edge cases for employee day-off calculations.
    
    Cross-dataframe operation that applies business rules for:
    - Employees with dyf_max_t=0 (cannot work Sundays/holidays)
    - Employees with COMPLETO cycle (all quotas reset to 0)
    - CXX adjustments for contract types 4/5
    
    Args:
        df_colaborador: Employee dataframe with quotas and cycle info
        df_calendario: Calendar dataframe with employee schedules
        employee_col: Employee identifier in calendario (default: 'COLABORADOR')
        matricula_col: Employee identifier in colaborador (default: 'matricula')
        
    Returns:
        Tuple[bool, pd.DataFrame, pd.DataFrame, str]: (success, updated colaborador, updated calendario, error)
    """
    try:
        # INPUT VALIDATION
        if df_colaborador is None or df_colaborador.empty:
            return False, pd.DataFrame(), pd.DataFrame(), "Input validation failed: empty df_colaborador"
        
        if df_calendario is None or df_calendario.empty:
            return False, pd.DataFrame(), pd.DataFrame(), "Input validation failed: empty df_calendario"
        
        required_cols_colab = [matricula_col, 'dyf_max_t', 'ciclo', 'l_dom', 'l_total', 'tipo_contrato', 'cxx']
        missing_cols = [col for col in required_cols_colab if col not in df_colaborador.columns]
        if missing_cols:
            return False, pd.DataFrame(), pd.DataFrame(), f"df_colaborador missing columns: {missing_cols}"
        
        # Use lowercase column names to match actual df_calendario columns
        required_cols_cal = [employee_col, 'dia_tipo', 'horario']
        missing_cols = [col for col in required_cols_cal if col not in df_calendario.columns]
        if missing_cols:
            return False, pd.DataFrame(), pd.DataFrame(), f"df_calendario missing columns: {missing_cols}"
        
        # TREATMENT LOGIC
        df_colab_result = df_colaborador.copy()
        df_cal_result = df_calendario.copy()
        
        # 5H-1: Handle dyf_max_t = 0 (cannot work Sundays/holidays)
        dyf_zero_mask = (df_colab_result['dyf_max_t'] == 0) & (df_colab_result['ciclo'] != 'COMPLETO')
        dyf_zero_count = dyf_zero_mask.sum()
        
        if dyf_zero_count > 0:
            # Adjust l_total: subtract l_dom from l_total
            df_colab_result.loc[dyf_zero_mask, 'l_total'] = (
                df_colab_result.loc[dyf_zero_mask, 'l_total'] - 
                df_colab_result.loc[dyf_zero_mask, 'l_dom']
            )
            
            # Set l_dom to 0
            df_colab_result.loc[dyf_zero_mask, 'l_dom'] = 0
            
            # Get list of employees with dyf_max_t=0
            dyf_zero_employees = df_colab_result.loc[dyf_zero_mask, matricula_col].tolist()
            
            # Mark domYf days as 'L_DOM' in calendario for these employees
            cal_mask = (
                df_cal_result[employee_col].isin(dyf_zero_employees) &
                (df_cal_result['dia_tipo'] == 'domYf') &
                (df_cal_result['horario'] != 'V')
            )
            df_cal_result.loc[cal_mask, 'horario'] = 'L_DOM'
            
            logger.info(f"5H-1: Processed {dyf_zero_count} employees with dyf_max_t=0")
        
        # 5H-2: Handle COMPLETO cycle (reset all quotas to 0)
        completo_mask = df_colab_result['ciclo'] == 'COMPLETO'
        completo_count = completo_mask.sum()
        
        if completo_count > 0:
            cols_to_reset = ['l_total', 'l_dom', 'l_d', 'l_q', 'l_qs', 'c2d', 'c3d', 'cxx', 'descansos_atrb']
            # Only reset columns that exist
            cols_to_reset = [col for col in cols_to_reset if col in df_colab_result.columns]
            df_colab_result.loc[completo_mask, cols_to_reset] = 0
            
            logger.info(f"5H-2: Reset quotas for {completo_count} employees with COMPLETO cycle")
        
        # 5H-3: Handle CXX for contract types 4/5
        if 'l_res' in df_colab_result.columns:
            cxx_mask = (
                df_colab_result['tipo_contrato'].isin([4, 5]) & 
                (df_colab_result['cxx'] > 0) &
                (~df_colab_result['ciclo'].isin(['SIN DYF', 'COMPLETO']))
            )
            cxx_count = cxx_mask.sum()
            
            if cxx_count > 0:
                df_colab_result.loc[cxx_mask, 'l_res2'] = (
                    df_colab_result.loc[cxx_mask, 'l_res'] - 
                    df_colab_result.loc[cxx_mask, 'cxx']
                )
                logger.info(f"5H-3: Calculated l_res2 for {cxx_count} employees with contract types 4/5 and CXX>0")
        else:
            logger.info("5H-3: Skipping CXX handling - l_res column not found")
        
        # 5H-4: Handle 'SIN DYF' cycle
        sin_dyf_mask = df_colab_result['ciclo'] == 'SIN DYF'
        sin_dyf_count = sin_dyf_mask.sum()
        
        if sin_dyf_count > 0:
            # Adjust c2d: subtract c3d from c2d
            df_colab_result.loc[sin_dyf_mask, 'c2d'] = (
                df_colab_result.loc[sin_dyf_mask, 'c2d'] - 
                df_colab_result.loc[sin_dyf_mask, 'c3d']
            )
            
            # Recalculate l_total based on specific quotas
            df_colab_result.loc[sin_dyf_mask, 'l_total'] = (
                df_colab_result.loc[sin_dyf_mask, 'l_dom'] + 
                df_colab_result.loc[sin_dyf_mask, 'l_d'] + 
                df_colab_result.loc[sin_dyf_mask, 'c2d'] + 
                df_colab_result.loc[sin_dyf_mask, 'cxx']
            )
            
            # Reset specific quotas to 0
            reset_cols_sin_dyf = ['l_q', 'l_qs', 'c3d', 'vz']
            if 'l_res' in df_colab_result.columns:
                reset_cols_sin_dyf.append('l_res')
            
            # Only reset columns that exist
            reset_cols_sin_dyf = [col for col in reset_cols_sin_dyf if col in df_colab_result.columns]
            df_colab_result.loc[sin_dyf_mask, reset_cols_sin_dyf] = 0
            
            logger.info(f"5H-4: Adjusted {sin_dyf_count} employees with 'SIN DYF' cycle")
        
        # OUTPUT VALIDATION
        logger.info(f"Successfully handled employee edge cases: {dyf_zero_count} dyf_max_t=0, {completo_count} COMPLETO, {sin_dyf_count} SIN DYF")
        
        return True, df_colab_result, df_cal_result, ""
        
    except Exception as e:
        error_msg = f"Error handling employee edge cases: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), pd.DataFrame(), error_msg


def adjust_horario_for_admission_date(df_calendario: pd.DataFrame, df_colaborador: pd.DataFrame, employee_col: str = 'COLABORADOR', date_col: str = 'DATA', horario_col: str = 'HORARIO', dia_tipo_col: str = 'dia_tipo') -> Tuple[bool, pd.DataFrame, str]:
    """
    Adjust HORARIO based on employee admission dates.
    
    For any date before admission OR after demission, set HORARIO to '-'.
    
    Args:
        df_calendario: Calendar dataframe with employee schedules
        df_colaborador: Employee dataframe with admission dates
        employee_col: Employee identifier column (default: 'COLABORADOR')
        date_col: Date column name (default: 'DATA')
        horario_col: Schedule column (default: 'HORARIO')
        dia_tipo_col: Day type column (default: 'dia_tipo')
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, adjusted calendario, error message)
    """
    try:
        # INPUT VALIDATION
        if df_calendario is None or df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_calendario"
        
        if df_colaborador is None or df_colaborador.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_colaborador"
        
        required_cols_cal = [employee_col, date_col, horario_col, dia_tipo_col]
        missing_cols = [col for col in required_cols_cal if col not in df_calendario.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"df_calendario missing columns: {missing_cols}"
        
        if 'data_admissao' not in df_colaborador.columns:
            logger.warning("data_admissao column not found in df_colaborador, skipping admission/demission adjustment")
            return True, df_calendario.copy(), ""
        
        demission_available = 'data_demissao' in df_colaborador.columns
        
        # TREATMENT LOGIC
        df_result = df_calendario.copy()
        
        # Ensure both sides have matching data types (string) for merge
        df_result[employee_col] = df_result[employee_col].astype(str).str.strip()
        merge_cols = [employee_col, 'data_admissao']
        if demission_available:
            merge_cols.append('data_demissao')
        admission_data = df_colaborador[merge_cols].copy()
        admission_data[employee_col] = admission_data[employee_col].astype(str).str.strip()
        
        # Merge admission date into calendario
        df_result = df_result.merge(admission_data, on=employee_col, how='left')
        
        # Verify merge succeeded - data_admissao should exist after merge
        if 'data_admissao' not in df_result.columns:
            logger.error(f"Merge failed: data_admissao column not found after merge. Check employee identifier matching.")
            logger.error(f"df_result[{employee_col}] type: {df_result[employee_col].dtype}, unique count: {df_result[employee_col].nunique()}")
            logger.error(f"admission_data[{employee_col}] type: {admission_data[employee_col].dtype}, unique count: {admission_data[employee_col].nunique()}")
            return False, df_calendario.copy(), ""  # Return original if merge fails
        
        # Convert dates to datetime if needed
        df_result[date_col] = pd.to_datetime(df_result[date_col])
        df_result['data_admissao'] = pd.to_datetime(df_result['data_admissao'])
        if demission_available:
            df_result['data_demissao'] = pd.to_datetime(df_result['data_demissao'])
        
        # Fill missing admission dates with very old date (assume they were always employed)
        df_result['data_admissao'] = df_result['data_admissao'].fillna(pd.Timestamp('1900-01-01'))
        if demission_available:
            df_result['data_demissao'] = df_result['data_demissao'].fillna(pd.Timestamp('2099-12-31'))
        
        # Vectorized adjustment
        # Mask for dates before admission / after demission
        before_admission = df_result[date_col] < df_result['data_admissao']
        if demission_available:
            after_demission = df_result[date_col] > df_result['data_demissao']
        else:
            after_demission = pd.Series(False, index=df_result.index)
        
        outside_contract = before_admission | after_demission
        df_result.loc[outside_contract, horario_col] = '-'
        
        # Drop temporary date columns
        cols_to_drop = ['data_admissao']
        if demission_available:
            cols_to_drop.append('data_demissao')
        df_result = df_result.drop(columns=cols_to_drop)
        
        # OUTPUT VALIDATION
        logger.info(f"Set HORARIO to '-' for {outside_contract.sum()} rows outside admission/demission window")
        
        return True, df_result, ""
        
    except Exception as e:
        error_msg = f"Error adjusting HORARIO for admission dates: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg


def calculate_and_merge_allocated_employees(df_estimativas: pd.DataFrame, df_calendario: pd.DataFrame, date_col_est: str = 'data', date_col_cal: str = 'DATA', shift_col_est: str = 'turno', shift_col_cal: str = 'TIPO_TURNO', employee_col: str = 'COLABORADOR', horario_col: str = 'HORARIO', param_pess_obj: float = 0.5) -> Tuple[bool, pd.DataFrame, str]:
    """
    Calculate actual employee headcount (+H) for each shift and merge with demand estimates.
    
    Cross-dataframe operation that:
    1. Counts how many employees work M/T shifts from calendario
    2. Handles split shifts (0.5 weighting for M+T same day)
    3. Merges +H with estimativas
    4. Calculates staffing objective (pess_obj) and diff
    
    Args:
        df_estimativas: Demand estimates with min/max/mean/sd per shift
        df_calendario: Calendar with employee schedules
        date_col_est: Date column in estimativas (default: 'data')
        date_col_cal: Date column in calendario (default: 'DATA')
        shift_col_est: Shift column in estimativas (default: 'turno')
        shift_col_cal: Shift column in calendario (default: 'TIPO_TURNO')
        employee_col: Employee identifier (default: 'COLABORADOR')
        horario_col: Schedule column (default: 'HORARIO')
        param_pess_obj: Volatility threshold for staffing objective (default: 0.5)
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, estimativas with +H/pess_obj/diff, error)
    """
    try:
        # INPUT VALIDATION
        if df_estimativas is None or df_estimativas.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_estimativas"
        
        if df_calendario is None or df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty df_calendario"
        
        required_cols_est = [date_col_est, shift_col_est, 'max_turno', 'min_turno', 'media_turno', 'sd_turno']
        missing_cols = [col for col in required_cols_est if col not in df_estimativas.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"df_estimativas missing columns: {missing_cols}"
        
        required_cols_cal = [date_col_cal, shift_col_cal, employee_col, horario_col]
        missing_cols = [col for col in required_cols_cal if col not in df_calendario.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"df_calendario missing columns: {missing_cols}"
        
        # TREATMENT LOGIC
        df_est_result = df_estimativas.copy()
        df_cal_work = df_calendario.copy()
        
        # Step 6B-1: Data type conversions
        df_est_result['max_turno'] = pd.to_numeric(df_est_result['max_turno'], errors='coerce')
        df_est_result['min_turno'] = pd.to_numeric(df_est_result['min_turno'], errors='coerce')
        df_est_result['sd_turno'] = pd.to_numeric(df_est_result['sd_turno'], errors='coerce')
        df_est_result['media_turno'] = pd.to_numeric(df_est_result['media_turno'], errors='coerce')
        df_est_result[shift_col_est] = df_est_result[shift_col_est].str.upper()
        
        # Filter out TIPO_DIA metadata rows
        df_cal_work = df_cal_work[df_cal_work[employee_col] != 'TIPO_DIA'].copy()
        
        # Convert dates to datetime for consistency
        df_est_result[date_col_est] = pd.to_datetime(df_est_result[date_col_est]).dt.date
        df_cal_work[date_col_cal] = pd.to_datetime(df_cal_work[date_col_cal]).dt.date
        
        # Step 6B-2 & 6B-3: Calculate +H for M and T shifts (vectorized)
        # Identify working employees (H or NL in HORARIO)
        df_cal_work['is_working'] = df_cal_work[horario_col].str.contains('H|NL', case=False, na=False)
        
        # Create pivot: for each employee-date, which shifts they work
        employee_shifts = df_cal_work[df_cal_work['is_working']].groupby(
            [employee_col, date_col_cal, shift_col_cal]
        ).size().reset_index(name='count')
        
        # Count shifts per employee-date
        shifts_per_emp_date = employee_shifts.groupby([employee_col, date_col_cal])[shift_col_cal].agg(set).reset_index()
        shifts_per_emp_date.columns = [employee_col, date_col_cal, 'shifts_worked']
        
        # Apply 0.5 weighting for employees working both M and T
        def calc_shift_weight(shifts, target_shift):
            if target_shift in shifts:
                return 0.5 if ('M' in shifts and 'T' in shifts) else 1.0
            return 0.0
        
        # Calculate +H for Morning
        shifts_per_emp_date['M_weight'] = shifts_per_emp_date['shifts_worked'].apply(
            lambda shifts: calc_shift_weight(shifts, 'M')
        )
        plus_h_m = shifts_per_emp_date.groupby(date_col_cal)['M_weight'].sum().reset_index()
        plus_h_m.columns = [date_col_cal, '+H']
        plus_h_m[shift_col_est] = 'M'
        
        # Calculate +H for Afternoon
        shifts_per_emp_date['T_weight'] = shifts_per_emp_date['shifts_worked'].apply(
            lambda shifts: calc_shift_weight(shifts, 'T')
        )
        plus_h_t = shifts_per_emp_date.groupby(date_col_cal)['T_weight'].sum().reset_index()
        plus_h_t.columns = [date_col_cal, '+H']
        plus_h_t[shift_col_est] = 'T'
        
        # Combine M and T
        plus_h_combined = pd.concat([plus_h_m, plus_h_t], ignore_index=True)
        
        # Step 6C: Merge +H with estimativas
        df_est_result = df_est_result.merge(
            plus_h_combined,
            left_on=[date_col_est, shift_col_est],
            right_on=[date_col_cal, shift_col_est],
            how='left'
        )
        
        # Drop duplicate date column if it exists
        if date_col_cal in df_est_result.columns and date_col_cal != date_col_est:
            df_est_result = df_est_result.drop(columns=[date_col_cal])
        
        df_est_result['+H'] = pd.to_numeric(df_est_result['+H'], errors='coerce').fillna(0)
        
        # Step 6D: Calculate staffing objective
        # aux = coefficient of variation (sd/mean)
        df_est_result['aux'] = np.where(
            df_est_result['media_turno'] != 0,
            df_est_result['sd_turno'] / df_est_result['media_turno'],
            0
        )
        
        # pess_obj: ceil if high volatility (aux >= threshold), else round
        df_est_result['pess_obj'] = np.where(
            df_est_result['aux'] >= param_pess_obj,
            np.ceil(df_est_result['media_turno']),
            np.round(df_est_result['media_turno'])
        )
        
        # diff: gap between current staffing and objective
        df_est_result['diff'] = df_est_result['+H'] - df_est_result['pess_obj']
        
        # Ensure min_turno is at least 1
        df_est_result['min_turno'] = np.where(
            df_est_result['min_turno'] == 0, 
            1, 
            df_est_result['min_turno']
        )
        
        # Drop temporary aux column
        df_est_result = df_est_result.drop(columns=['aux'], errors='ignore')
        
        # OUTPUT VALIDATION
        plus_h_count = (~df_est_result['+H'].isna()).sum()
        logger.info(f"Calculated +H for {plus_h_count} shift-date combinations")
        logger.info(f"Staffing objective (pess_obj) calculated using param_pess_obj={param_pess_obj}")
        
        return True, df_est_result, ""
        
    except Exception as e:
        error_msg = f"Error calculating and merging +H: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg
