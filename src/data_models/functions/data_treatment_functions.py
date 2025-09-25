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
import numpy as np
from typing import List, Tuple, Dict
from base_data_project.log_config import get_logger

# Local stuff
from src.config import PROJECT_NAME
from src.data_models.functions.helper_functions import convert_types_in
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
            # Continue with original data

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
            # Continue with current state

        # OUTPUT VALIDATION - Allow empty DataFrame as valid result
        return True, df_calendario_passado, ""
        
    except Exception as e:
        logger.error(f"Error in treat_df_calendario_passado: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

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
            return False, pd.DataFrame(), f"Input validation failed: missing columns {missing_columns}"

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

def treat_df_colaborador(df_colaborador: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Treat df_colaborador dataframe.
    """
    try:
        # INPUT VALIDATION
        # TODO: add validations
        if not validate_df_colaborador(df_colaborador):
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

        # Fill missing values
        non_date_columns = [col for col in df_colaborador.columns if col not in ['data_admissao', 'data_demissao']]
        df_colaborador[non_date_columns] = df_colaborador[non_date_columns].fillna(0)

        # Validate seq_turno
        seq_turno_zeros = bool((df_colaborador['seq_turno'] == 0).any())
        seq_turno_nulls = bool(df_colaborador['seq_turno'].isna().any())
        if seq_turno_zeros or seq_turno_nulls:
            error_msg = f"seq_turno=0 or null - columna SEQ_TURNO mal parametrizada: {df_colaborador['seq_turno'] == 0}"
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        contrato_zeros = bool((df_colaborador['tipo_contrato'] == 0).any())
        contrato_nulls = bool(df_colaborador['tipo_contrato'].isna().any())
        if contrato_zeros or contrato_nulls:
            error_msg = f"contrato=0 or null - TIPO_CONTRATO column not valid: {df_colaborador['seq_turno'] == 0}"
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
            logger.error(f"use case {use_case} not supported, please ensure the correct values are defined.")
            return False

        pass
    except Exception as e:
        logger.error(f"", exc_info=True)
        return False

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
        if needed_columns not in df_valid_emp.columns:
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

        return df_result
        
    except Exception as e:
        logger.error(f"Error in add_l_d_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing failed: {str(e)}"                    

def add_l_dom_to_df_colaborador(
    df_colaborador: pd.DataFrame,
    df_festivos: pd.DataFrame,
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
                if df_festivos is not None and len(df_festivos) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:
                        tipo_mask = mask_32_bd & (df_result['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_festivos, tipo_contrato)
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
                if df_festivos is not None and len(df_festivos) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:
                        tipo_mask = mask_32_bd & (df_result['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_festivos, tipo_contrato)
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
                if df_festivos is not None and len(df_festivos) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:                
                        tipo_mask = mask_32_sabeco & (df_result['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_festivos, tipo_contrato)
                            df_result.loc[tipo_mask, 'l_dom'] = coh[0]
                else:
                    df_result.loc[mask_32_bd, 'l_dom'] = 0

        else:
            error_msg = f"use case {use_case} not supported, please ensure the correct values are defined."
            logger.error(error_msg)
            return False, pd.DataFrame(), error_msg

        return df_result

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

        return df_colaborador

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

        return df_colaborador

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


    except Exception as e:
        logger.error(f"Error in add_l_dom_to_df_colaborador: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), f"Processing l_dom for df_colaborador failed: {str(e)}"    

def add_l_total_to_df_colaborador(df_colaborador: pd.DataFrame, df_festivos: pd.DataFrame, convenio_bd: str, num_sundays: int, num_fer_dom: int, use_case: int) -> Tuple[bool, pd.DataFrame, str]:
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
                if df_festivos is not None and len(df_festivos) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:
                        tipo_mask = mask_32_bd & (df_colaborador['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_festivos, tipo_contrato)
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
                if df_festivos is not None and len(df_festivos) > 0:
                    # Process each contract type separately for holiday calculations
                    for tipo_contrato in [3, 2]:                
                        tipo_mask = mask_32_sabeco & (df_colaborador['tipo_contrato'] == tipo_contrato)
                        if tipo_mask.any():
                            coh = count_open_holidays(df_festivos, tipo_contrato)
                            df_colaborador.loc[tipo_mask, 'l_total'] = coh[1]
                else:
                    df_colaborador.loc[mask_32_bd, 'l_total'] = 0       

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
            
        if not employee_id_matriculas_map or len(employee_id_matriculas_map) == 0:
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

def add_calendario_passado(df_calendario: pd.DataFrame, df_calendario_passado: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Add df_calendario_passado to df_calendario.
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # TREATMENT LOGIC
        logger.info(f"Adding df_calendario_passado to df_calendario. Not implemented yet.")
        # TODO: Add implementation when business logic is defined
        
        # OUTPUT VALIDATION
        return True, df_calendario, ""
        
    except Exception as e:
        logger.error(f"Error in add_calendario_passado: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def add_ausencias_ferias(df_calendario: pd.DataFrame, df_ausencias_ferias: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Add df_ausencias_ferias to df_calendario.
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # TREATMENT LOGIC
        logger.info(f"Adding df_ausencias_ferias to df_calendario. Not implemented yet.")
        # TODO: Add implementation when business logic is defined
        
        # OUTPUT VALIDATION
        return True, df_calendario, ""
        
    except Exception as e:
        logger.error(f"Error in add_ausencias_ferias: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def add_folgas_ciclos(df_calendario: pd.DataFrame, df_core_pro_emp_horario_det: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Add df_core_pro_emp_horario_det to df_calendario.
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # TREATMENT LOGIC
        logger.info(f"Adding df_core_pro_emp_horario_det to df_calendario. Not implemented yet.")
        # TODO: Add implementation when business logic is defined
        
        # OUTPUT VALIDATION
        return True, df_calendario, ""
        
    except Exception as e:
        logger.error(f"Error in add_folgas_ciclos: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

def add_ciclos_90(df_calendario: pd.DataFrame, df_ciclos_90: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Add df_ciclos_90 to df_calendario.
    """
    try:
        # INPUT VALIDATION
        if df_calendario.empty:
            return False, pd.DataFrame(), "Input validation failed: empty calendario DataFrame"
            
        # TREATMENT LOGIC
        logger.info(f"Adding df_ciclos_90 to df_calendario. Not implemented yet.")
        # TODO: Add implementation when business logic is defined
        
        # OUTPUT VALIDATION
        return True, df_calendario, ""
        
    except Exception as e:
        logger.error(f"Error in add_ciclos_90: {str(e)}", exc_info=True)
        return False, pd.DataFrame(), ""

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