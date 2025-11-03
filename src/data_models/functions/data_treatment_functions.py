"""
File containing the data treatment/dataframe manipulation functions for the DescansosDataModel.
Data treatment functions:
- treat_valid_emp
- treat_df_closed_days
- treat_calendario_passado
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
from src.data_models.functions.helper_functions import convert_types_in, adjusted_isoweek

# Get configuration singleton
_config = get_config()
PROJECT_NAME = _config.project_name
from src.data_models.validations.load_process_data_validations import (
    validate_df_colaborador
)
from src.helpers import count_open_holidays

# Set up logger
logger = get_logger(PROJECT_NAME)

def treat_df_valid_emp(df_valid_emp: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Treat valid_emp dataframe.
    """
    try:
        # INPUT VALIDATION
        if df_valid_emp.empty:
            return False, pd.DataFrame(), "Input validation failed: empty DataFrame"
        
        if 'prioridade_folgas' not in df_valid_emp.columns:
            return False, pd.DataFrame(), "Input validation failed: missing prioridade_folgas column"
            
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
    Treat df_closed_days dataframe.
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

def treat_df_calendario_passado(df_calendario_passado: pd.DataFrame, employees_id_list: List[int], past_employee_id_list: List[int], case_type: int, wfm_proc_colab: str, start_date: str, end_date: str) -> Tuple[bool, pd.DataFrame, str]:
    """
    Treat df_calendario_passado dataframe.
    Convert types in df_calendario_passado.
    Filter df_calendario_passado by date logic.
    Arguments:
        df_calendario_passado: pd.DataFrame
    Returns:
        Tuple[bool, pd.DataFrame, str]
    """
    try:
        # INPUT VALIDATION
        if not start_date or not end_date:
            return False, pd.DataFrame(), "Input validation failed: missing date parameters"
            
        if not df_calendario_passado.empty and 'schedule_day' not in df_calendario_passado.columns:
            return False, pd.DataFrame(), "Input validation failed: missing schedule_day column"
            
        # Treat dates for filtering purposes
        try:
            logger.info(f"Treating dates ({start_date} and {end_date})")
            start_date_dt = pd.to_datetime(start_date, format="%Y-%m-%d")
            end_date_dt = pd.to_datetime(end_date, format="%Y-%m-%d")
        except (ValueError, TypeError) as e:
            logger.error(f"Date parsing failed: {str(e)}")
            return False, pd.DataFrame(), "Date parsing failed"

        # TREATMENT LOGIC
        if df_calendario_passado.empty:
            # Even when empty, ensure proper column structure after treatment
            # Rename 'schedule_day' to 'data' to match expected structure
            if 'schedule_day' in df_calendario_passado.columns:
                df_calendario_passado = df_calendario_passado.rename(columns={'schedule_day': 'data'})
            return True, df_calendario_passado, ""
            
        logger.info(f"Treating df_calendario_passado")
        try:
            df_calendario_passado['data_dt'] = pd.to_datetime(df_calendario_passado['schedule_day'], format="%Y-%m-%d")
        except (ValueError, TypeError) as e:
            logger.warning(f"Date conversion failed: {e}")
            return False, pd.DataFrame(), "Date conversion failed"
            
        if case_type in [3, 4, 6, 7]:
            # Filter df_calendario_passado to have values outside the date range (most efficient pandas way)
            mask = ~df_calendario_passado['data_dt'].between(start_date_dt, end_date_dt, inclusive='both')
            df_calendario_passado = df_calendario_passado[mask]

        elif case_type == 8:
            # Filter df to exclude wfm_proc_colab employee within the date range (most efficient pandas way)
            mask = ~((df_calendario_passado['employee_id'] == wfm_proc_colab) & 
                     df_calendario_passado['data_dt'].between(start_date_dt, end_date_dt, inclusive='both'))
            df_calendario_passado = df_calendario_passado[mask]

        # Convert types in
        try:
            df_calendario_passado = convert_types_in(df_calendario_passado)
        except Exception as e:
            logger.warning(f"Type conversion failed: {e}")
            return False, pd.DataFrame(), f"Type conversion failed: {e}"

        # Clean up columns
        try:
            columns_to_drop = ['data_dt']
            # Only drop columns that exist
            existing_columns_to_drop = [col for col in columns_to_drop if col in df_calendario_passado.columns]
            optional_columns_to_drop = [col for col in ['type', 'subtype'] if col in df_calendario_passado.columns]
            all_columns_to_drop = existing_columns_to_drop + optional_columns_to_drop
            
            if all_columns_to_drop:
                df_calendario_passado = df_calendario_passado.drop(labels=all_columns_to_drop, axis='columns')
                
            df_calendario_passado = df_calendario_passado.rename(columns={'schedule_day': 'data'})
        except Exception as e:
            logger.warning(f"Column cleanup failed: {e}")
            return False, pd.DataFrame(), f"Column cleanup failed: {e}"

        # OUTPUT VALIDATION - Allow empty DataFrame as valid result
        return True, df_calendario_passado, ""
        
    except Exception as e:
        error_msg = f"Error in treat_df_calendario_passado: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg

def treat_df_ausencias_ferias(df_ausencias_ferias: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Treat df_ausencias_ferias dataframe.
    """
    try:
        # INPUT VALIDATION
        if df_ausencias_ferias.empty:
            return True, df_ausencias_ferias, ""  # Empty is valid for this function
            
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
            
        # OUTPUT VALIDATION
        if not df_ausencias_ferias.empty and 'data' not in df_ausencias_ferias.columns:
            return False, pd.DataFrame(), "Treatment failed: missing data column in result"
            
        return True, df_ausencias_ferias, ""

    except Exception as e:
        logger.error(f"Error in treat_df_ausencias_ferias: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def treat_df_ciclos_90(df_ciclos_90: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Treat df_ciclos_90 dataframe.
    """
    try:
        # INPUT VALIDATION - empty is valid for this function
        # TODO: Add treatment logic when implemented
        
        # TREATMENT LOGIC - placeholder for future implementation
        logger.info(f"treat_df_ciclos_90 not implemented yet - returning input as-is")
        
        # OUTPUT VALIDATION
        return True, df_ciclos_90, ""
        
    except Exception as e:
        logger.error(f"Error in treat_df_ciclos_90: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def treat_df_colaborador(df_colaborador: pd.DataFrame, employees_id_list: List[int]) -> Tuple[bool, pd.DataFrame, str]:
    """
    Treat df_colaborador dataframe.
    """
    try:
        # INPUT VALIDATION
        # TODO: add validations
        if not validate_df_colaborador(df_colaborador=df_colaborador, employees_id_list=employees_id_list):
            return False, pd.DataFrame(), "Input validation failed: empty DataFrame"
            
        # Rename columns LOGIC
        try:
            # Rename columns if they exist
            rename_mapping = {}
            if 'ec.codigo' in df_colaborador.columns:
                rename_mapping['ec.codigo'] = 'fk_colaborador'
            elif 'codigo' in df_colaborador.columns:
                rename_mapping['codigo'] = 'fk_colaborador'
            elif 'ec.matricula' in df_colaborador.columns:
                rename_mapping['ec.matricula'] = 'matricula'
            elif 'ec.data_admissao' in df_colaborador.columns:
                rename_mapping['ec.data_admissao'] = 'data_admissao'
            elif 'ec.data_demissao' in df_colaborador.columns:
                rename_mapping['ec.data_demissao'] = 'data_demissao'
                
            if rename_mapping:
                df_colaborador = df_colaborador.rename(columns=rename_mapping)
                
        except Exception as e:
            logger.warning(f"Column renaming failed: {e}")
            # Continue with original column names

        # Map database column names to expected business logic names
        column_mapping = {
            'min_dias_trabalhados': 'min_dia_trab',
            'max_dias_trabalhados': 'max_dia_trab', 
            'fds_cal_2d': 'c2d',
            'fds_cal_3d': 'c3d',
            'd_cal_xx': 'cxx',
            'lq': 'lqs'  # if needed, otherwise keep as 'lq'
        }
        
        # Apply column renaming
        df_colaborador = df_colaborador.rename(columns=column_mapping)
        
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
        except Exception as e:
            error_msg = f"Error converting specific columns to numeric type: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, pd.DataFrame(), error_msg

        #logger.info(f"DEBUG df_colaborador:\n {df_colaborador}")

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

def add_lqs_to_df_colaborador(df_colaborador: pd.DataFrame, df_params_lq: pd.DataFrame, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
    """
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
    Set tipo_contrato to df_colaborador.
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
    """
    try:
        if df_colaborador.empty or len(df_colaborador) == 0:
            return False, pd.DataFrame(), "Input validation failed: empty df_colaborador DataFrame"

        needed_columns = ['fk_colaborador', 'prioridade_folgas']
        if not all(col in df_valid_emp.columns for col in needed_columns):
            return False, pd.DataFrame(), f"Needed columns not present in df_valid_emp: {needed_columns}"

        if use_case == 0:
            df_colaborador['prioridade_folgas'] = ''
        
        elif use_case == 1:
            # Merge with valid_emp to get PRIORIDADE_FOLGAS
            df_colaborador = pd.merge(
                df_colaborador, 
                df_valid_emp[['fk_colaborador', 'prioridade_folgas']], 
                on='fk_colaborador', 
                how='left'
            )

        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        return True, df_colaborador, ""

    except Exception as e:
        logger.error(f"Error in set_tipo_contrato_to_df_colaborador: {str(e)}", exc_info=True)
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
        
        # Define fields to adjust
        fields_to_adjust = ['l_d', 'l_dom', 'l_q', 'l_total', 'c2d', 'c3d']
        
        # SCENARIO A: Late hire only (hired after start_date, still active)
        late_hire_mask = (
            df_result['data_admissao'].notna() & 
            (start_dt < df_result['data_admissao']) &
            df_result['data_demissao'].isna()
        )
        
        if late_hire_mask.any():
            logger.info(f"DEBUG: {late_hire_mask.sum()} employees need late hire adjustments")
            
            # Calculate adjustment factors
            days_worked = (end_dt - df_result.loc[late_hire_mask, 'data_admissao']).dt.days + 1
            div_factors = days_worked / total_days
            
            # Log debug info and apply adjustments
            for idx in df_result[late_hire_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                div = div_factors.loc[idx]
                logger.info(f"DEBUG Late Hire - Employee {matricula}: days_worked: {days_worked.loc[idx]}, total_days: {total_days}, div: {div}")
                
                # Log before values
                before_values = {field: df_result.loc[idx, field] for field in fields_to_adjust if field in df_result.columns}
                logger.info(f"DEBUG before: {before_values}")
            
            # Apply adjustments vectorized
            for field in fields_to_adjust:
                if field in df_result.columns:
                    df_result.loc[late_hire_mask, field] = np.ceil(
                        df_result.loc[late_hire_mask, field] * div_factors
                    )
            
            # Log after values
            for idx in df_result[late_hire_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                after_values = {field: df_result.loc[idx, field] for field in fields_to_adjust if field in df_result.columns}
                logger.info(f"DEBUG after: {after_values}")
        
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
            
            # Log debug info and apply adjustments
            for idx in df_result[early_term_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                div = div_factors.loc[idx]
                logger.info(f"DEBUG Early Term - Employee {matricula}: days_worked: {days_worked.loc[idx]}, total_days: {total_days}, div: {div}")
                
                # Log before values
                before_values = {field: df_result.loc[idx, field] for field in fields_to_adjust if field in df_result.columns}
                logger.info(f"DEBUG before: {before_values}")
            
            # Apply adjustments vectorized
            for field in fields_to_adjust:
                if field in df_result.columns:
                    df_result.loc[early_term_mask, field] = np.ceil(
                        df_result.loc[early_term_mask, field] * div_factors
                    )
            
            # Log after values
            for idx in df_result[early_term_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                after_values = {field: df_result.loc[idx, field] for field in fields_to_adjust if field in df_result.columns}
                logger.info(f"DEBUG after: {after_values}")
        
        # SCENARIO C: Both late hire AND early termination
        both_adjust_mask = (
            df_result['data_admissao'].notna() & 
            (start_dt < df_result['data_admissao']) &
            df_result['data_demissao'].notna() &
            (df_result['data_demissao'] < end_dt)
        )
        
        if both_adjust_mask.any():
            logger.info(f"DEBUG: {both_adjust_mask.sum()} employees need both late hire and early termination adjustments")
            
            # Calculate adjustment factors
            days_worked = (df_result.loc[both_adjust_mask, 'data_demissao'] - df_result.loc[both_adjust_mask, 'data_admissao']).dt.days + 1
            div_factors = days_worked / total_days
            
            # Log debug info and apply adjustments
            for idx in df_result[both_adjust_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                div = div_factors.loc[idx]
                logger.info(f"DEBUG Both - Employee {matricula}: days_worked: {days_worked.loc[idx]}, total_days: {total_days}, div: {div}")
                
                # Log before values
                before_values = {field: df_result.loc[idx, field] for field in fields_to_adjust if field in df_result.columns}
                logger.info(f"DEBUG before: {before_values}")
            
            # Apply adjustments vectorized
            for field in fields_to_adjust:
                if field in df_result.columns:
                    df_result.loc[both_adjust_mask, field] = np.ceil(
                        df_result.loc[both_adjust_mask, field] * div_factors
                    )
            
            # Log after values
            for idx in df_result[both_adjust_mask].index:
                matricula = df_result.loc[idx, 'matricula']
                after_values = {field: df_result.loc[idx, field] for field in fields_to_adjust if field in df_result.columns}
                logger.info(f"DEBUG after: {after_values}")
        
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
        
        logger.info(f"Successfully applied date adjustments to {len(df_result)} employees")
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
    """
    try:
        # Define use_case for totals treatment calculations
        logger.info(f"Adding l_d column to df_colaborador.")

        df_result = df_colaborador.copy()

        if use_case == 0:
            df_result['l_d'] = 0

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
    """

    try:
        logger.info(f"Adding l_dom column to df_colaborador.")

        df_result = df_colaborador.copy()

        start_date_dt = pd.to_datetime(start_date_str, format='%Y-%m-%d')
        end_date_dt = pd.to_datetime(end_date_str, format='%Y-%m-%d')

        # Calculate div factor for each employee based on admission date (reused from admission function)
        df_result['data_admissao'] = pd.to_datetime(df_result['data_admissao'], errors='coerce')
        needs_admission_adjustment = (
            df_result['data_admissao'].notna() & 
            (start_date_dt < df_result['data_admissao'])
        )
        
        # Calculate div factor for l_dom_salsa calculations
        div_factors = pd.Series(1.0, index=df_result.index)
        if needs_admission_adjustment.any():
            days_from_admission = (end_date_dt - df_result.loc[needs_admission_adjustment, 'data_admissao']).dt.days + 1
            total_days = (end_date_dt - start_date_dt).days + 1
            div_factors.loc[needs_admission_adjustment] = days_from_admission / total_days

        # Case 0: 
        if use_case == 0:
            df_result['l_dom'] = 0

        # Used by Salsa
        elif use_case == 1:
            # First mask
            mask_6_bd = (df_result['tipo_contrato'] == 6) & (df_result['convenio'] == convenio_bd)
            if mask_6_bd.any():
                df_result.loc[mask_6_bd, 'l_dom'] = num_sundays * div_factors.loc[mask_6_bd] - df_result.loc[mask_6_bd, 'dyf_max_t']

            # Second mask
            mask_54_bd = (df_result['tipo_contrato'].isin([5, 4])) & (df_result['convenio'] == convenio_bd)
            if mask_54_bd.any():
                df_result.loc[mask_54_bd, 'l_dom'] = num_sundays * div_factors.loc[mask_54_bd] - df_result.loc[mask_54_bd, 'dyf_max_t']

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
                df_colaborador.loc[mask_54_bd, 'l_total'] = num_sundays * (7 - int(df_colaborador.loc[mask_54_bd, 'tipo_contrato']))

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
                df_colaborador.loc[mask_6_sabeco, 'l_total'] = num_fer_dom + df_colaborador[mask_6_sabeco, 'c2d']

            mask_54_sabeco = (df_colaborador['tipo_contrato'].isin([5, 4])) & (df_colaborador['convenio'] == 'SABECO')
            if mask_54_sabeco.any():
                df_colaborador.loc[mask_54_sabeco, 'l_total'] = num_sundays * (7 - int(df_colaborador.loc[mask_54_sabeco, 'tipo_contrato'])) + 8

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
        logger.error(f"Error in add_l_dom_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing l_dom for df_colaborador failed: {str(e)}"  



def create_df_calendario(start_date: str, end_date: str, employee_id_matriculas_map: Dict[str, str]) -> Tuple[bool, pd.DataFrame, str]:
    """
    Create df_calendario dataframe with employee schedules for the specified date range using vectorized operations.
    
    Args:
        start_date: Start date as string (YYYY-MM-DD format)
        end_date: End date as string (YYYY-MM-DD format)
        employee_id_matriculas_map: Dictionary mapping employee_ids to matriculas
        
    Returns:
        Tuple[bool, DataFrame, str] with columns: employee_id, data, tipo_turno, horario, wday, dia_tipo, matricula, data_admissao, data_demissao
    """
    try:
        # INPUT VALIDATION
        if not start_date or not end_date:
            return False, pd.DataFrame(), "Input validation failed: missing date parameters"
            
        if employee_id_matriculas_map.empty or len(employee_id_matriculas_map) == 0:
            return False, pd.DataFrame(), "Input validation failed: empty employee mapping"
            
        logger.info(f"Creating df_calendario from {start_date} to {end_date} for {len(employee_id_matriculas_map)} employees")
        
        # TREATMENT LOGIC
        try:
            # Convert input strings to date format
            start_dt = pd.to_datetime(start_date, format='%Y-%m-%d')
            end_dt = pd.to_datetime(end_date, format='%Y-%m-%d')
        except (ValueError, TypeError) as e:
            return False, pd.DataFrame(), "Date parsing failed"
        
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
            df_calendario['data'] = df_calendario['data'].dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Data formatting failed: {e}")
            return False, pd.DataFrame(), "Data formatting failed"
        
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
        
        # OUTPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Calendar creation resulted in empty DataFrame"
            
        logger.info(f"Created df_calendario with {len(df_calendario)} rows ({len(employee_id_matriculas_map)} employees × {len(date_range)} days × 2 shifts)")
        
        return True, df_calendario, ""
        
    except Exception as e:
        logger.error(f"Error in create_df_calendario: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def add_calendario_passado(df_calendario: pd.DataFrame, df_calendario_passado: pd.DataFrame, use_case: int = 1) -> Tuple[bool, pd.DataFrame, str]:
    """
    Fill missing horario values in df_calendario using historical data from df_calendario_passado.
    Matches records by employee_id and data fields.
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
            required_cols = ['employee_id', 'data', 'horario']
            for col in required_cols:
                if col not in df_calendario.columns:
                    return False, pd.DataFrame(), f"Missing required column '{col}' in df_calendario"
                if col not in df_calendario_passado.columns:
                    return False, pd.DataFrame(), f"Missing required column '{col}' in df_calendario_passado"
            
            df_result = df_calendario.copy()
            
            # Create lookup Series from df_calendario_passado using MultiIndex
            passado_lookup = df_calendario_passado.set_index(['employee_id', 'data'])['horario']
            
            # Create MultiIndex for df_result to enable vectorized lookup (without tipo_turno)
            result_index = df_result.set_index(['employee_id', 'data']).index
            
            # Vectorized lookup: map passado values to result positions
            mapped_values = result_index.map(passado_lookup)
            
            # Create mask for empty horario values in df_result
            empty_mask = (df_result['horario'].isnull()) | (df_result['horario'] == '') | (df_result['horario'] == '-')
            
            # Create mask for valid values from passado (not empty/null)
            valid_passado_mask = mapped_values.notna() & (mapped_values != '') & (mapped_values != '-')
            
            # Combine masks: fill only where current is empty AND passado has valid data
            fill_mask = empty_mask & valid_passado_mask
            
            # Vectorized assignment
            df_result.loc[fill_mask, 'horario'] = mapped_values[fill_mask]
            
            filled_count = fill_mask.sum()
            logger.info(f"Filled {filled_count} empty horario values from df_calendario_passado")
            
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
    Fill df_calendario horario values with absence/vacation data from df_ausencias_ferias.
    Maps tipo_ausencia to horario codes based on employee_id and data.
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
            # Use fk_colaborador if employee_id doesn't exist yet
            employee_col = 'employee_id' if 'employee_id' in df_ausencias_ferias.columns else 'fk_colaborador'
            ausencias_lookup = df_ausencias_ferias.set_index([employee_col, 'data'])['tipo_ausencia']
            
            # Create MultiIndex for df_result to enable vectorized lookup (without tipo_turno)
            # Use the same employee column as in df_result (could be 'employee_id' or 'fk_colaborador')
            result_employee_col = 'employee_id' if 'employee_id' in df_result.columns else 'fk_colaborador'
            result_index = df_result.set_index([result_employee_col, 'data']).index
            
            # Vectorized lookup: map ausencias values to result positions
            mapped_values = result_index.map(ausencias_lookup)
            
            # Create mask for empty horario values in df_result
            empty_mask = (df_result['horario'].isnull()) | (df_result['horario'] == '') | (df_result['horario'] == '-')
            
            # Create mask for valid values from ausencias (not empty/null)
            valid_ausencias_mask = mapped_values.notna() & (mapped_values != '') & (mapped_values != '-')
            
            # Combine masks: fill only where current is empty AND ausencias has valid data
            fill_mask = empty_mask & valid_ausencias_mask
            
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
    Override df_calendario horario values with day-offs from df_core_pro_emp_horario_det.
    Applies 'L' for day-offs (tipo_dia = 'F') to both M and T shifts.
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
            
            # Filter for day-offs only (tipo_dia = 'F')
            df_dayoffs = df_core_pro_emp_horario_det[
                df_core_pro_emp_horario_det['tipo_dia'] == 'F'
            ].copy()
            
            if df_dayoffs.empty:
                logger.info("No day-off records found, returning original df_calendario")
                return True, df_result, ""
            
            # Convert schedule_day to string format for matching
            df_dayoffs['data'] = pd.to_datetime(df_dayoffs['schedule_day']).dt.strftime('%Y-%m-%d')
            
            # Create lookup Series from day-offs using MultiIndex
            dayoffs_lookup = df_dayoffs.set_index(['employee_id', 'data'])['tipo_dia']
            
            # Create MultiIndex for df_result to enable vectorized lookup
            result_index = df_result.set_index(['fk_colaborador', 'data']).index
            
            # Vectorized lookup: map day-off values to result positions
            mapped_values = result_index.map(dayoffs_lookup)
            
            # Create mask for day-off records (tipo_dia = 'F')
            dayoff_mask = mapped_values == 'F'
            
            # Vectorized assignment: Override with 'L' for all day-offs
            df_result.loc[dayoff_mask, 'horario'] = 'L'
            
            filled_count = dayoff_mask.sum()
            logger.info(f"Applied {filled_count} day-off overrides (L) from df_core_pro_emp_horario_det")
            
            return True, df_result, f"Successfully applied {filled_count} day-off overrides"
        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        error_msg = f"Error in add_folgas_ciclos: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg

def add_ciclos_90(df_calendario: pd.DataFrame, df_ciclos_90: pd.DataFrame, use_case: int = 1) -> Tuple[bool, pd.DataFrame, str]:
    """
    Fill df_calendario horario values with 90-day cycle data from df_ciclos_90.
    Uses codigo_trads or horario_ind values for schedule assignments.
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
            if df_ciclos_90.empty:
                logger.info("df_ciclos_90 is empty, returning original df_calendario")
                return True, df_calendario, ""
            
            df_result = df_calendario.copy()
            
            # Convert schedule_day to string format for matching
            df_ciclos_mapped = df_ciclos_90.copy()
            df_ciclos_mapped['data'] = pd.to_datetime(df_ciclos_mapped['schedule_day']).dt.strftime('%Y-%m-%d')
            
            # Use codigo_trads if available, otherwise use horario_ind as fallback
            horario_col = 'codigo_trads' if 'codigo_trads' in df_ciclos_mapped.columns else 'horario_ind'
            
            # Create lookup Series from df_ciclos_90 using MultiIndex
            ciclos_lookup = df_ciclos_mapped.set_index(['employee_id', 'data'])[horario_col]
            
            # Create MultiIndex for df_result to enable vectorized lookup
            result_index = df_result.set_index(['fk_colaborador', 'data']).index
            
            # Vectorized lookup: map ciclos values to result positions
            mapped_values = result_index.map(ciclos_lookup)
            
            # Create mask for empty horario values in df_result
            empty_mask = (df_result['horario'].isnull()) | (df_result['horario'] == '') | (df_result['horario'] == '-')
            
            # Create mask for valid values from ciclos (not empty/null)
            valid_ciclos_mask = mapped_values.notna() & (mapped_values != '') & (mapped_values != '-')
            
            # Combine masks: fill only where current is empty AND ciclos has valid data
            fill_mask = empty_mask & valid_ciclos_mask
            
            # Vectorized assignment
            df_result.loc[fill_mask, 'horario'] = mapped_values[fill_mask]
            
            filled_count = fill_mask.sum()
            logger.info(f"Filled {filled_count} empty horario values from df_ciclos_90")
            
            return True, df_result, f"Successfully filled {filled_count} horario values from 90-day cycles"
        else:
            error_msg = f"use_case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg
        
    except Exception as e:
        error_msg = f"Error in add_ciclos_90: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg

def add_days_off(df_calendario: pd.DataFrame, df_days_off: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Add df_days_off to df_calendario.
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


def add_date_related_columns(df: pd.DataFrame, date_col: str = 'data', add_id_col: bool = False) -> Tuple[bool, pd.DataFrame, str]:
    """
    Add date-related columns to dataframe (WDAY, WW, WD).
    
    Agnostic function that works for both df_calendario and df_estimativas.
    
    Args:
        df: Input dataframe with date column
        date_col: Name of date column ('data' for estimativas, 'DATA' for calendario)
        add_id_col: Whether to add ID column (row index) - usually only for calendario
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, dataframe with new columns, error message)
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
        
        # Add WDAY (1=Monday, 7=Sunday)
        df_result['WDAY'] = df_result[date_col].dt.dayofweek + 1
        
        # Add WW (adjusted ISO week)
        df_result['WW'] = df_result[date_col].apply(adjusted_isoweek)
        
        # Add WD (3-letter weekday name)
        df_result['WD'] = df_result[date_col].dt.day_name().str[:3]
        
        # Add ID column if requested (usually for calendario)
        if add_id_col:
            df_result['ID'] = range(len(df_result))
        
        logger.info(f"Added date-related columns: WDAY, WW, WD" + (" and ID" if add_id_col else ""))
        return True, df_result, ""
        
    except Exception as e:
        error_msg = f"Error adding date-related columns: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, pd.DataFrame(), error_msg


def define_dia_tipo(df: pd.DataFrame, date_col: str = 'data', tipo_turno_col: str = 'TIPO_TURNO', horario_col: str = 'HORARIO', wd_col: str = 'WD') -> Tuple[bool, pd.DataFrame, str]:
    """
    Define DIA_TIPO (day type) column - identifies Sundays and holidays.
    
    Business Logic:
    - 'domYf' (domingo y feriado): Sundays or holidays requiring special rest day handling
    - Regular weekday name (Mon, Tue, etc.): Normal working days
    
    A date is marked as 'domYf' if:
    - The date contains at least one holiday (TIPO_TURNO == 'F'), OR
    - The date is a Sunday (WD == 'Sun')
    - AND the specific row's HORARIO != 'F' (not the holiday row itself)
    
    Requires: WD column must exist (run add_date_related_columns first)
    
    Args:
        df: Input dataframe
        date_col: Date column name (default: 'data')
        tipo_turno_col: Shift type column (default: 'TIPO_TURNO')
        horario_col: Work status column (default: 'HORARIO')
        wd_col: Weekday name column (default: 'WD')
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, dataframe with DIA_TIPO column, error message)
    """
    try:
        # INPUT VALIDATION
        if df is None or df.empty:
            return False, pd.DataFrame(), "Input validation failed: empty or None dataframe"
        
        required_cols = [date_col, tipo_turno_col, horario_col, wd_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, pd.DataFrame(), f"Input validation failed: missing columns {missing_cols}. Run add_date_related_columns first."
        
        # TREATMENT LOGIC
        df_result = df.copy()
        
        def assign_dia_tipo(group):
            """Assign DIA_TIPO for each date group."""
            # Check if this date has any holiday markers OR is a Sunday
            has_holiday = (group[tipo_turno_col] == 'F').any()
            is_sunday = group[wd_col].iloc[0] == 'Sun'
            
            # Apply logic row by row within the date group
            group['DIA_TIPO'] = group.apply(
                lambda row: 'domYf' if ((has_holiday or is_sunday) and row[horario_col] != 'F') else row[wd_col],
                axis=1
            )
            return group
        
        # Group by date and apply the logic
        df_result = df_result.groupby(date_col, group_keys=False).apply(assign_dia_tipo)
        
        # Count results
        domyf_count = (df_result['DIA_TIPO'] == 'domYf').sum()
        logger.info(f"Defined DIA_TIPO column: {domyf_count} rows marked as 'domYf' (Sundays/holidays)")
        
        return True, df_result, ""
        
    except Exception as e:
        error_msg = f"Error defining DIA_TIPO: {str(e)}"
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
        
        # Prepare df_contratos for merging
        if 'schedule_day' in df_contratos_work.columns:
            df_contratos_work[date_col] = pd.to_datetime(df_contratos_work['schedule_day'])
        
        # Ensure required columns exist in df_contratos
        required_contract_cols = ['employee_id', date_col, 'matricula', 'contract_id', 'carga_diaria']
        missing_cols = [col for col in required_contract_cols if col not in df_contratos_work.columns]
        if missing_cols:
            logger.warning(f"df_contratos missing columns: {missing_cols}. Merge may have limited data.")
        
        # Merge contract information
        original_count = len(df_result)
        df_result = df_result.merge(
            df_contratos_work[['employee_id', date_col, 'matricula', 'contract_id', 'carga_diaria']],
            left_on=[employee_col, date_col],
            right_on=['matricula', date_col],
            how='left'
        )
        
        # Drop duplicate columns
        df_result = df_result.drop(columns=['employee_id', 'matricula'], errors='ignore')
        
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
            return False, pd.DataFrame(), f"Input validation failed: missing columns {missing_cols}"
        
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
        
        required_cols_cal = [employee_col, 'DIA_TIPO', 'HORARIO']
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
                (df_cal_result['DIA_TIPO'] == 'domYf') &
                (df_cal_result['HORARIO'] != 'V')
            )
            df_cal_result.loc[cal_mask, 'HORARIO'] = 'L_DOM'
            
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


def adjust_horario_for_admission_date(df_calendario: pd.DataFrame, df_colaborador: pd.DataFrame, employee_col: str = 'COLABORADOR', date_col: str = 'DATA', horario_col: str = 'HORARIO', dia_tipo_col: str = 'DIA_TIPO') -> Tuple[bool, pd.DataFrame, str]:
    """
    Adjust HORARIO based on employee admission dates.
    
    For dates before admission:
    - If domYf (Sunday/holiday): set HORARIO to 'L_' (guaranteed day off)
    - Otherwise: set HORARIO to 'NL' (nao libranza)
    
    Args:
        df_calendario: Calendar dataframe with employee schedules
        df_colaborador: Employee dataframe with admission dates
        employee_col: Employee identifier column (default: 'COLABORADOR')
        date_col: Date column name (default: 'DATA')
        horario_col: Schedule column (default: 'HORARIO')
        dia_tipo_col: Day type column (default: 'DIA_TIPO')
        
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
            logger.warning("data_admissao column not found in df_colaborador, skipping admission date adjustment")
            return True, df_calendario.copy(), ""
        
        # TREATMENT LOGIC
        df_result = df_calendario.copy()
        
        # Merge admission date into calendario
        admission_data = df_colaborador[[employee_col, 'data_admissao']].copy()
        df_result = df_result.merge(admission_data, on=employee_col, how='left')
        
        # Convert dates to datetime if needed
        df_result[date_col] = pd.to_datetime(df_result[date_col])
        df_result['data_admissao'] = pd.to_datetime(df_result['data_admissao'])
        
        # Fill missing admission dates with very old date (assume they were always employed)
        df_result['data_admissao'] = df_result['data_admissao'].fillna(pd.Timestamp('1900-01-01'))
        
        # Vectorized adjustment
        # Mask for dates before admission
        before_admission = df_result[date_col] < df_result['data_admissao']
        
        # For dates before admission AND domYf: set to 'L_'
        mask_domyf = before_admission & (df_result[dia_tipo_col] == 'domYf')
        df_result.loc[mask_domyf, horario_col] = 'L_'
        
        # For dates before admission AND NOT domYf: set to 'NL'
        mask_not_domyf = before_admission & (df_result[dia_tipo_col] != 'domYf')
        df_result.loc[mask_not_domyf, horario_col] = 'NL'
        
        # Drop temporary admission date column
        df_result = df_result.drop(columns=['data_admissao'])
        
        # OUTPUT VALIDATION
        before_count = before_admission.sum()
        domyf_adjusted = mask_domyf.sum()
        nl_adjusted = mask_not_domyf.sum()
        
        logger.info(f"Adjusted HORARIO for {before_count} dates before admission: {domyf_adjusted} to 'L_', {nl_adjusted} to 'NL'")
        
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
        shifts_per_emp_date = employee_shifts.groupby([employee_col, date_col_cal])[shift_col_cal].apply(
            lambda x: set(x)
        ).reset_index()
        shifts_per_emp_date.columns = [employee_col, date_col_cal, 'shifts_worked']
        
        # Apply 0.5 weighting for employees working both M and T
        def calc_shift_weight(row, target_shift):
            shifts = row['shifts_worked']
            if target_shift in shifts:
                return 0.5 if ('M' in shifts and 'T' in shifts) else 1.0
            return 0.0
        
        # Calculate +H for Morning
        shifts_per_emp_date['M_weight'] = shifts_per_emp_date.apply(
            lambda row: calc_shift_weight(row, 'M'), axis=1
        )
        plus_h_m = shifts_per_emp_date.groupby(date_col_cal)['M_weight'].sum().reset_index()
        plus_h_m.columns = [date_col_cal, '+H']
        plus_h_m[shift_col_est] = 'M'
        
        # Calculate +H for Afternoon
        shifts_per_emp_date['T_weight'] = shifts_per_emp_date.apply(
            lambda row: calc_shift_weight(row, 'T'), axis=1
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
