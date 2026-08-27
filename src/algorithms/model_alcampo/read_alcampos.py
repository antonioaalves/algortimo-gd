import pandas as pd
import math
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
import logging
from base_data_project.log_config import get_logger
from datetime import datetime
from collections import defaultdict
from src.configuration_manager.instance import get_config as get_config_manager
from src.algorithms.model_alcampo.auxiliar_functions_alcampo import (days_off_atributtion, joining_template_with_contract_per_week)

# Set up logger
logger = get_logger(get_config_manager().system.project_name)

def read_data_alcampo(medium_dataframes: Dict[str, pd.DataFrame], shifts: List[str], algorithm_treatment_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Enhanced version of read_data_alcampo with comprehensive logging and error checks.
    
    Args:
        medium_dataframes: Dictionary containing the required DataFrames
        
    Returns:
        Tuple containing all processed data elements for the algorithm
        
    Raises:
        ValueError: If required DataFrames are missing or invalid
        KeyError: If required columns are missing from DataFrames
    """
    try:
        logger.info("Starting enhanced data reading for Alcampo algorithm")
        
        # =================================================================
        # 1. VALIDATE INPUT data
        # =================================================================
        required_dataframes = ['df_colaborador', 'df_estimativas', 'df_calendario']
        missing_dataframes = [df for df in required_dataframes if df not in medium_dataframes]
        
        if missing_dataframes:
            raise ValueError(f"Missing required DataFrames: {missing_dataframes}")
        
        # Extract DataFrames
        # matriz_colaborador_gd = pd.read_csv('data/csvs/matriz_colaborador_alcampos3.csv', sep=';',  engine='python')
        # matriz_estimativas_gd = pd.read_csv('data/csvs/matriz_estimativas_alcampos2.csv', sep=',',  engine='python',index_col=0)
        # matriz_calendario_gd = pd.read_csv('data/csvs/matriz_calendario_alcampos3.csv',  sep=';',  engine='python', index_col=0)

        matriz_colaborador_gd = medium_dataframes['df_colaborador'].copy()
        matriz_estimativas_gd = medium_dataframes['df_estimativas'].copy()
        matriz_calendario_gd = medium_dataframes['df_calendario'].copy()
        matriz_feriados_gd = algorithm_treatment_params['df_feriados'].copy()
        matriz_annual_variables = algorithm_treatment_params["df_annual_variables"]
        matriz_process_rules_gd = algorithm_treatment_params.get('df_process_rules', pd.DataFrame())
        matriz_past_lds_gd = algorithm_treatment_params.get('df_pro_emp_mov', pd.DataFrame())
        if matriz_process_rules_gd is None:
            matriz_process_rules_gd = pd.DataFrame()
        else:
            matriz_process_rules_gd = matriz_process_rules_gd.copy()
        if matriz_past_lds_gd is None:
            matriz_past_lds_gd = pd.DataFrame()
        else:
            matriz_past_lds_gd = matriz_past_lds_gd.copy()

        start_date = matriz_calendario_gd.loc[matriz_calendario_gd["schedule_day"] == algorithm_treatment_params['start_date'], "index"].iloc[0]
        end_date = matriz_calendario_gd.loc[matriz_calendario_gd["schedule_day"] == algorithm_treatment_params['end_date'], "index"].iloc[0]
        period = [start_date, end_date]

        logger.info(f"Period Start and end Time:")
        logger.info(f"Start: {start_date}")
        logger.info(f"End: {end_date}")

        wfm_proc = algorithm_treatment_params['wfm_proc_colab']
        if wfm_proc not in (None, 'None', ''):
            partial_generation = True
            partial_workers = algorithm_treatment_params['employees_id_list_for_posto']
            logger.debug(f"wfm_proc {wfm_proc}, {type(wfm_proc)}")
        else:
            partial_generation = False
            partial_workers = []

        matriz_colaborador_gd.columns = matriz_colaborador_gd.columns.str.lower()
        matriz_estimativas_gd.columns = matriz_estimativas_gd.columns.str.lower()
        matriz_calendario_gd.columns = matriz_calendario_gd.columns.str.lower()       

        logger.info(f"Input DataFrames loaded:")
        logger.info(f"  - matriz_colaborador: {matriz_colaborador_gd.shape}")
        logger.info(f"  - matriz_estimativas: {matriz_estimativas_gd.shape}")
        logger.info(f"  - matriz_calendario: {matriz_calendario_gd.shape}")

        logger.info("Parameters:")
        logger.info(f"  - wfm_proc_colab: {wfm_proc}, if it has value, its a partial generation -> {partial_generation}.")
        if partial_generation == True:
            logger.info(f"  - partial_workers: {partial_workers} workers")
        
        # =================================================================
        # 2. VALIDATE REQUIRED COLUMNS
        # =================================================================
        required_colaborador_cols = ['employee_id', 'VZ', 'L_RES', 'L_RES2']
        required_colaborador_cols = [s.lower() for s in required_colaborador_cols]
        required_calendario_cols = ['employee_id', 'wd', 'dia_tipo', 'horario']
        required_calendario_cols = [s.lower() for s in required_calendario_cols]
        required_estimativas_cols = ['schedule_day', 'turno', 'media_turno', 'max_turno', 'min_turno', 'pess_obj', 'sd_turno', 'wday' ]
        required_estimativas_cols = [s.lower() for s in required_estimativas_cols]
        
        missing_colab_cols = [col for col in required_colaborador_cols if col not in matriz_colaborador_gd.columns]
        missing_cal_cols = [col for col in required_calendario_cols if col not in matriz_calendario_gd.columns]
        # missing_estima_cols = [col for col in required_estimativas_cols if col not in matriz_estimativas_gd.columns]

                
        if missing_colab_cols:
            raise KeyError(f"Missing required columns in matriz_colaborador: {missing_colab_cols}")
        if missing_cal_cols:
            raise KeyError(f"Missing required columns in matriz_calendario: {missing_cal_cols}")
        # if missing_estima_cols:
        #     raise KeyError(f"Missing required columns in matriz_estimativas: {missing_estima_cols}")
        
        logger.info("[OK] All required columns present in DataFrames")

        # =================================================================
        # 3. CALCULATE L_Q FOR colaborador data
        # =================================================================
        #logger.info("Calculating L_Q values for workers")
        
        ## Check for missing values in required columns
        #numeric_cols = ['VZ', 'L_RES', 'L_RES2']
        #numeric_cols = [s.lower() for s in numeric_cols]



        #for col in numeric_cols:
        #    if matriz_colaborador_gd[col].isna().any():
        #        logger.warning(f"Found NaN values in column {col}, filling with 0")
        #        matriz_colaborador_gd[col] = matriz_colaborador_gd[col].fillna(0)



        #matriz_colaborador_gd["l_q"] = (
        #    matriz_colaborador_gd["l_total"] - 
        #    matriz_colaborador_gd["l_dom"] - 
        #    matriz_colaborador_gd["c2d"] - 
        #    matriz_colaborador_gd["c3d"] - 
        #    matriz_colaborador_gd["l_d"] - 
        #    matriz_colaborador_gd["cxx"] - 
        #    matriz_colaborador_gd["vz"] - 
        #    matriz_colaborador_gd["l_res"] - 
        #    matriz_colaborador_gd["l_res2"]
        #)
        
        #logger.info(f"L_Q calculated. Range: {matriz_colaborador_gd['l_q'].min():.2f} to {matriz_colaborador_gd['l_q'].max():.2f}")
        
        # =================================================================
        # 4. PROCESS CALENDARIO data
        # =================================================================
        logger.info("Processing calendario data")
        
        # Ensure colaborador column is numeric
        matriz_calendario_gd['employee_id'] = pd.to_numeric(matriz_calendario_gd['employee_id'], errors='coerce')
        invalid_colaborador = matriz_calendario_gd['employee_id'].isna().sum()
        if invalid_colaborador > 0:
            logger.warning(f"Found {invalid_colaborador} invalid colaborador values, removing these rows")
            matriz_calendario_gd = matriz_calendario_gd.dropna(subset=['employee_id'])
        
        # Convert data column to datetime
        try:
            matriz_calendario_gd['schedule_day'] = pd.to_datetime(matriz_calendario_gd['schedule_day'])
            matriz_estimativas_gd['schedule_day'] = pd.to_datetime(matriz_estimativas_gd['schedule_day'])
            logger.info(f"Date range: {matriz_calendario_gd['schedule_day'].min()} to {matriz_calendario_gd['schedule_day'].max()}")
        except Exception as e:
            raise ValueError(f"Error converting data column to datetime with both mixed and explicit formats: {e}")

        

        # =================================================================
        # 5. IDENTIFY VALID WORKERS (PRESENT IN ALL DATAFRAMES)
        # =================================================================
        logger.info("Identifying valid workers present in all DataFrames")
        
        # Get unique workers from each DataFrame
        workers_colaborador_complete = set(matriz_colaborador_gd['employee_id'].dropna().astype(int))
        workers_calendario_complete = set(matriz_calendario_gd['employee_id'].dropna().astype(int))
        if partial_generation == True:
            for w in partial_workers:
                partial_workers_complete = set(matriz_colaborador_gd['employee_id'][matriz_colaborador_gd['employee_id'] == w].dropna().astype(int))
                logger.info(f"Unique workers found:")
                logger.info(f"  - In matriz_colaborador_complete: {len(workers_colaborador_complete)} workers")
                logger.info(f"  - In matriz_calendario_complete: {len(workers_calendario_complete)} workers")
                logger.info(f"  - In partial_workers_complete: {len(partial_workers_complete)} workers")
        else:
            partial_workers_complete = set()
        
        workers_colaborador = set(matriz_colaborador_gd['employee_id'].dropna().astype(int))

        if partial_generation == True:
            valid_workers = set(partial_workers_complete).intersection(workers_calendario_complete)
            past_workers = workers_calendario_complete - set(partial_workers_complete)
            valid_workers_complete = workers_colaborador_complete.intersection(workers_calendario_complete)

            workers = sorted(valid_workers)
            workers_complete = workers
            complete = pd.DataFrame()
            workers_complete_cycle = [] if complete.empty else workers
            if not complete.empty:
                workers = []
            workers_past = sorted(past_workers)
        else:
            past_workers = set()
            valid_workers = workers_colaborador.intersection(workers_calendario_complete)
            valid_workers_complete = workers_colaborador_complete.intersection(workers_calendario_complete)

            workers = sorted(valid_workers)
            workers_complete = sorted(valid_workers_complete)
            workers_complete_cycle = sorted(set(workers_complete)-set(workers))
            workers_past = sorted(past_workers)
        
        if not valid_workers_complete:
            raise ValueError("No workers found that are present in all required DataFrames")

        logger.info(f"[OK] Final valid workers: {len(workers)} workers for free day atribution")
        logger.info(f"   Worker IDs: {workers[:10]}{'...' if len(workers) > 10 else ''}")
        
        logger.info(f"[OK] Final valid workers (complete): {len(workers_complete)} workers for complete cycle")
        logger.info(f"   Worker IDs (complete): {workers_complete[:10]}{'...' if len(workers_complete) > 10 else ''}")

        # Ensure data type consistency before filtering
        matriz_colaborador_gd['employee_id'] = matriz_colaborador_gd['employee_id'].astype(int)
        matriz_calendario_gd['employee_id'] = matriz_calendario_gd['employee_id'].astype(int)
        
        matriz_colaborador_gd = matriz_colaborador_gd[matriz_colaborador_gd['employee_id'].isin(workers_complete)]
        matriz_calendario_nao_alterada = matriz_calendario_gd.copy()
        matriz_calendario_gd = matriz_calendario_gd[matriz_calendario_gd['employee_id'].isin(workers_complete)]

        
        logger.info(f"Filtered DataFrames to valid workers:")
        logger.info(f"  - matriz_colaborador: {matriz_colaborador_gd.shape}")
        logger.info(f"  - matriz_estimativas: {matriz_estimativas_gd.shape}")
        logger.info(f"  - matriz_calendario: {matriz_calendario_gd.shape}")
        
        # =================================================================
        # 6. EXTRACT DAYS AND DATE INFORMATION
        # =================================================================
        logger.info("Extracting days and date information")
        
        days_of_year = sorted(matriz_calendario_gd['index'].unique().tolist())
        max_day = max(days_of_year)
        logger.info(f"Days of year: {len(days_of_year)} days (from {min(days_of_year)} to {max_day})")
        
        # =================================================================
        # 7. IDENTIFY SPECIAL DAYS
        # =================================================================
        logger.info("Identifying special days")
        
        # Define shifts and special days
        
        sundays = set(matriz_calendario_gd[matriz_calendario_gd['wd'] == 'Sun']['index'].unique().tolist())

        holidays = set(matriz_feriados_gd[(matriz_feriados_gd['tipo_feriado'] == 'A')
                         ]['index'].unique().tolist())
        
        closed_holidays = set(matriz_feriados_gd[(matriz_feriados_gd['tipo_feriado'] == 'F')
                             ]['index'].unique().tolist())
        
        special_days = sorted(list(set(sundays | holidays)))
        
        logger.info(f"Special days identified:")
        logger.info(f"  - Sundays: {len(sundays)} days")
        logger.info(f"  - Holidays (non-Sunday): {len(holidays)} days")
        logger.info(f"  - Closed holidays: {len(closed_holidays)} days")
        logger.info(f"  - Total special days: {len(special_days)} days")



        logger.info(f"Worker-specific data processed for {len(workers)} workers")
        
        # =================================================================
        # 8. CALCULATE ADDITIONAL PARAMETERS
        # =================================================================
        logger.info("Calculating additional parameters")
        
        # Working days (non-special days)
        non_holidays = [d for d in days_of_year if d not in closed_holidays]  # Alias for compatibility
        index_to_date = matriz_calendario_gd.drop_duplicates(subset='index').set_index('index')['schedule_day'].fillna("2000-01-01").astype(str).to_dict()
        # Calculate week information
        unique_dates = sorted(matriz_calendario_gd['schedule_day'].unique())

        if unique_dates:
            unique_dates = [x.strftime('%Y-%m-%d') for x in unique_dates]
            # Get start weekday from the first date in the calendar data (not estimativas)
            # Sort calendar by date to get the actual first date
            matriz_calendario_sorted = matriz_calendario_gd.sort_values('schedule_day')
            first_date_row = matriz_calendario_sorted.iloc[0]

            # Get the year from the first date and create January 1st of that year
            year = matriz_estimativas_gd.loc[
                   (matriz_estimativas_gd['schedule_day'].dt.month == 6) &
                   (matriz_estimativas_gd['schedule_day'].dt.day == 25),
                   'schedule_day'].dt.year.iloc[0]

            # If your system uses 1=Monday, 7=Sunday, add 1:
            start_weekday = 1
        
            
            logger.info(f"First date: {first_date_row['schedule_day']}, WDAY: {start_weekday}")
            
            # Create week to days mapping using WW column and day of year
            week_to_days = {}
            
            # Process each unique date in the calendar (remove duplicates by date)
            unique_calendar_dates = matriz_calendario_gd.drop_duplicates(['index']).sort_values('index')
            week_number = 1
            for _, row in unique_calendar_dates.iterrows():
                day_of_year = row['index']
                
                # Initialize the week list if it doesn't exist
                if week_number not in week_to_days:
                    week_to_days[week_number] = []
                
                # Add the day to its corresponding week (avoid duplicates)
                if day_of_year not in week_to_days[week_number]:
                    week_to_days[week_number].append(day_of_year)
                if day_of_year % 7 == 0:
                    week_number += 1
            
            # Sort days within each week to ensure chronological order
            for week in week_to_days:
                week_to_days[week].sort()
                
            logger.info(f"Week to days mapping created using calendar data:")
            logger.info(f"  - Start weekday (from first date): {start_weekday}")
            logger.info(f"  - Weeks found: {sorted(week_to_days.keys())}")
            logger.info(f"  - Total weeks: {len(week_to_days)}")
            logger.info(f"  - Sample weeks: {dict(list(week_to_days.items())[-3:])}")
                
        else:
            start_weekday = 0
            week_to_days = {}
            logger.warning("No unique dates found in matriz_calendario_gd, week calculations may be incomplete")  

        nbr_weeks = len(week_to_days) 
        logger.info(f"Week calculation:")
        logger.info(f"  - Start weekday: {start_weekday}")
        logger.info(f"  - Number of weeks: {nbr_weeks}")

        # Get the date range from matriz_calendario for validation
        min_calendar_date = matriz_calendario_gd['schedule_day'].min()
        max_calendar_date = matriz_calendario_gd['schedule_day'].max()

        jan1 = matriz_estimativas_gd.loc[
        (matriz_estimativas_gd['schedule_day'].dt.month == 1) &
        (matriz_estimativas_gd['schedule_day'].dt.day == 1) &
        (matriz_estimativas_gd['schedule_day'].dt.year == year)
        ]

        dec31 = matriz_estimativas_gd.loc[
        (matriz_estimativas_gd['schedule_day'].dt.month == 12) &
        (matriz_estimativas_gd['schedule_day'].dt.day == 31) &
        (matriz_estimativas_gd['schedule_day'].dt.year == year)
        ]
    
        min_day_year = jan1['index'].iloc[0] if not jan1.empty else 1
        max_day_year = dec31['index'].iloc[0] if not dec31.empty else 365

        logger.info(f"Calendar date range: {min_calendar_date} to {max_calendar_date}")
        logger.info(f"Calendar day of year range: {min_day_year} to {max_day_year}")
        year_range = [min_day_year, max_day_year]

        # =================================================================
        # 9.1. EXTRACT WORKER CONTRACT INFORMATION
        # =================================================================
        logger.info("Extracting worker contract information")
        
        # Create dictionaries for worker contract data
        contract_type = {}
        total_l = {}
        total_l_dom = {}
        c2d = {}
        c3d = {}
        l_d = {}
        l_q = {}
        cxx = {}
        t_lq = {}
        tc = {}
        total_l_sab = {}
        total_l_dom_or_sab = {}
        data_admissao = {}
        data_demissao = {}
        last_registered_day = {}
        first_registered_day = {}
        work_days_per_week = {}
        dummy_workers = {}
        workers_with_dummy = defaultdict(dict)
        workers_complete_with_dummy = workers_complete.copy()
        workers_list_with_dummy = workers.copy()
        workers_no_contract_changes = []
        
        for w in workers:
            worker_data = matriz_colaborador_gd[matriz_colaborador_gd['employee_id'] == w]
            worker_calendar = matriz_calendario_gd[matriz_calendario_gd['employee_id'] == w]
            if worker_data.empty:
                logger.warning(f"No contract data found for worker {w}")
                # Set default values
                contract_type[w] = 'Contract Error'  # Default contract type
                total_l[w] = 0
                total_l_dom[w] = 0
                c2d[w] = 0
                c3d[w] = 0
                l_d[w] = 0
                l_q[w] = 0
                cxx[w] = 0
                t_lq[w] = 0
                tc[w] = 0
            else:
                worker_row = worker_data.iloc[0]  # Take first row if multiple
        
                # Extract contract information
                contract_type[w] = worker_row.get('tipo_contrato', 0)
                total_l[w] = int(worker_row.get('l_total', 0))
                total_l_dom[w] = int(worker_row.get('l_dom', 0))
                c2d[w] = int(worker_row.get('c2d', 0))
                c3d[w] = int(worker_row.get('c3d', 0))
                l_d[w] = int(worker_row.get('l_d', 0))
                l_q[w] = int(worker_row.get('l_q', 0))
                cxx[w] = int(worker_row.get('cxx', 0))
                t_lq[w] = int(worker_row.get('l_q', 0) + worker_row.get('c2d', 0) + worker_row.get('c3d', 0))
                tc[w] = int(worker_row.get('dofhc', 0))

                logger.info(f"Worker {w} contract information extracted: "
                            f"Contract Type: {contract_type[w]}, "
                            f"Total L: {total_l[w]}, "
                            f"Total L DOM: {total_l_dom[w]}, "
                            f"C2D: {c2d[w]}, "
                            f"C3D: {c3d[w]}, "
                            f"L_D: {l_d[w]}, "
                            f"L_Q: {l_q[w]}, "
                            f"CXX: {cxx[w]}, "
                            f"T_LQ: {t_lq[w]}, "
                            f"TC: {tc[w]}")
                admissao_value = worker_row.get('data_admissao', None)
                logger.info(f"Processing worker {w} with data_admissao: {admissao_value}")
                demissao_value = worker_row.get('data_demissao', None)
                logger.info(f"Processing worker {w} with data_demissao: {demissao_value}")

                # Convert data_admissao to day of year
                data_admissao[w] = 0
                if admissao_value is not None and not pd.isna(admissao_value):
                    if isinstance(admissao_value, (datetime, pd.Timestamp)):
                        admissao_date = admissao_value
                    elif isinstance(admissao_value, str):
                        admissao_date = pd.to_datetime(admissao_value)
                    else:
                        admissao_date = None

                    if admissao_date is not None:
                        # Check if admissao is within calendar date range (not day of year)
                        if min_calendar_date <= admissao_date <= max_calendar_date:
                            admissao_day_of_year = worker_calendar.loc[worker_calendar['schedule_day'] == admissao_date, 'index'].iloc[0]
                            data_admissao[w] = int(admissao_day_of_year)
                            logger.info(f"Worker {w} data_admissao: {admissao_date.date()} -> day of year {admissao_day_of_year}")
                        else:
                            logger.info(f"Worker {w} data_admissao {admissao_date.date()} is outside calendar range ({min_calendar_date.date()} to {max_calendar_date.date()}), set to 0")

                # Convert data_demissao to day of year
                data_demissao[w] = max_day + 1
                if demissao_value is not None and not pd.isna(demissao_value):
                    if isinstance(demissao_value, (datetime, pd.Timestamp)):
                        demissao_date = demissao_value
                    elif isinstance(demissao_value, str):
                        demissao_date = pd.to_datetime(demissao_value)
                    else:
                        demissao_date = None
                    if demissao_date is not None:
                        # Check if demissao is within calendar date range (not day of year)
                        if min_calendar_date <= demissao_date <= max_calendar_date:
                            demissao_day_of_year = worker_calendar.loc[worker_calendar['schedule_day'] == demissao_date, 'index'].iloc[0]
                            data_demissao[w] = int(demissao_day_of_year)
                            logger.info(f"Worker {w} data_demissao: {demissao_date.date()} -> day of year {demissao_day_of_year}")
                        else:
                            logger.info(f"Worker {w} data_demissao {demissao_date.date()} is outside calendar range ({min_calendar_date.date()} to {max_calendar_date.date()}), set to 0")

                # Track first and last registered days
                if w in matriz_calendario_gd['employee_id'].values:
                    first_registered_day[w] = worker_calendar['index'].min()
                    if  first_registered_day[w] < data_admissao[w]:
                        first_registered_day[w] = data_admissao[w]
                    logger.info(f"Worker {w} first registered day: {first_registered_day[w]}")
                else:
                    first_registered_day[w] = 0

                if w in matriz_calendario_gd['employee_id'].values:
                    last_registered_day[w] = worker_calendar['index'].max()
                    # Only adjust if there's an actual dismissal date (not 0)
                    if data_demissao[w] > 0 and last_registered_day[w] > data_demissao[w]:
                        last_registered_day[w] = data_demissao[w]
                    logger.info(f"Worker {w} last registered day: {last_registered_day[w]}")
                else:
                    last_registered_day[w] = 0

                nbr_of_contracts = len(worker_data)
                if nbr_of_contracts == 1:
                    workers_no_contract_changes.append(w)
                if nbr_of_contracts > 1:
                    logger.info(f"Worker {w} changes contract {nbr_of_contracts - 1} times")
                    layer = 1
                    while layer < nbr_of_contracts:
                        new_w = max(workers_complete_with_dummy) + 1
                        workers_complete_with_dummy.append(new_w)
                        workers_list_with_dummy.append(new_w)


                        worker_row = worker_data.iloc[layer]
                        if layer == 1 and layer != nbr_of_contracts - 1:
                            original_end_date = int(worker_calendar.loc[worker_calendar['schedule_day'] == pd.to_datetime(worker_row.get('begin_date', None)), 'index'].iloc[0]) - 1
                        # Extract contract information
                        contract_type[new_w] = int(worker_row.get('tipo_contrato', 0))
                        total_l[new_w] = int(worker_row.get('l_total', 0))
                        total_l_dom[new_w] = int(worker_row.get('l_dom', 0))
                        total_l_dom[w] = int(worker_row.get('l_dom', 0))
                        l_q[new_w] = int(worker_row.get('l_q', 0))
                        t_lq[new_w] = int(worker_row.get('l_q', 0) + worker_row.get('c2d', 0) + worker_row.get('c3d', 0))
                        tc[new_w] = int(worker_row.get('dofhc', 0))
                        c2d[new_w] = int(worker_row.get('c2d', 0))
                        c3d[new_w] = int(worker_row.get('c3d', 0))
                        l_d[new_w] = int(worker_row.get('l_d', 0))
                        cxx[new_w] = int(worker_row.get('cxx', 0))
                        admissao_value = worker_row.get('begin_date', None)
                        logger.info(f"Processing worker {new_w} with data_admissao: {admissao_value}")
                        demissao_value = worker_row.get('end_date', None)
                        logger.info(f"Processing worker {new_w} with data_demissao: {demissao_value}")
                        # Convert data_admissao to day of year
                        data_admissao[new_w] = 0
                        if admissao_value is not None and not pd.isna(admissao_value):
                            if isinstance(admissao_value, (datetime, pd.Timestamp)):
                                admissao_date = admissao_value
                            elif isinstance(admissao_value, str):
                                admissao_date = pd.to_datetime(admissao_value)
                            else:
                                admissao_date = None

                            if admissao_date is not None:
                                # Check if admissao is within calendar date range (not day of year)
                                if min_calendar_date <= admissao_date <= max_calendar_date:
                                    admissao_day_of_year = worker_calendar.loc[worker_calendar['schedule_day'] == admissao_date, 'index'].iloc[0]
                                    data_admissao[new_w] = int(admissao_day_of_year)
                                    logger.info(f"Worker {new_w} data_admissao: {admissao_date.date()} -> day of year {admissao_day_of_year}")
                                else:
                                    logger.info(f"Worker {new_w} data_admissao {admissao_date.date()} is outside calendar range ({min_calendar_date.date()} to {max_calendar_date.date()}), set to 0")

                        # Convert data_demissao to day of year
                        data_demissao[new_w] = max_day + 1
                        if demissao_value is not None and not pd.isna(demissao_value):
                            if isinstance(demissao_value, (datetime, pd.Timestamp)):
                                demissao_date = demissao_value
                            elif isinstance(demissao_value, str):
                                demissao_date = pd.to_datetime(demissao_value)
                            else:
                                demissao_date = None
                            if demissao_date is not None:
                                # Check if demissao is within calendar date range (not day of year)
                                if min_calendar_date <= demissao_date <= max_calendar_date:
                                    demissao_day_of_year = worker_calendar.loc[worker_calendar['schedule_day'] == demissao_date, 'index'].iloc[0]
                                    data_demissao[new_w] = int(demissao_day_of_year)
                                    logger.info(f"Worker {new_w} data_demissao: {demissao_date.date()} -> day of year {demissao_day_of_year}")
                                else:
                                    logger.info(f"Worker {new_w} data_demissao {demissao_date.date()} is outside calendar range ({min_calendar_date.date()} to {max_calendar_date.date()}), set to 0")

                        if layer == nbr_of_contracts - 1:
                            data_demissao[new_w] = data_demissao[w]
                            if layer == 1:
                                original_end_date = int(worker_calendar.loc[worker_calendar['schedule_day'] == pd.to_datetime(worker_row.get('begin_date', None)), 'index'].iloc[0]) - 1
                            data_demissao[w] = original_end_date
                            last_registered_day[w] = data_demissao[w]
                        # Track first and last registered days
                        if w in matriz_calendario_gd['employee_id'].values:
                            first_registered_day[new_w] = worker_calendar['index'].min()
                            if  first_registered_day[new_w] < data_admissao[new_w]:
                                first_registered_day[new_w] = data_admissao[new_w]
                            logger.info(f"Worker {new_w} first registered day: {first_registered_day[new_w]}")
                        else:
                            first_registered_day[new_w] = 0

                        if w in matriz_calendario_gd['employee_id'].values:
                            last_registered_day[new_w] = worker_calendar['index'].max()
                            # Only adjust if there's an actual dismissal date (not 0)
                            if data_demissao[new_w] > 0 and last_registered_day[new_w] > data_demissao[new_w]:
                                last_registered_day[new_w] = data_demissao[new_w]
                            logger.info(f"Worker {new_w} last registered day: {last_registered_day[new_w]}")
                        else:
                            last_registered_day[new_w] = 0
                        workers_with_dummy[w][range(data_admissao[new_w], data_demissao[new_w] + 1)] = new_w
                        dummy_workers[new_w] = {
                            'parent': w,
                            'layer': layer,
                            'start_date': data_admissao[new_w],
                            'end_date': data_demissao[new_w],
                        }
                        layer += 1

        for w in workers:
            if contract_type[w] == 0:
                logger.error(f"Worker {w} has contract type error, removing from workers list")
                workers.pop(workers.index(w))  # Remove worker with contract error
            elif total_l[w] < 0:
                logger.error(f"Worker {w} has non-positive total_l: {total_l[w]}, removing from workers list")
                workers.pop(workers.index(w))  # Remove worker with contract error

        logger.info(f"Contract information extracted for {len(workers)} workers")

        # Initialize dictionaries for worker-specific information
        empty_days = {}
        worker_absences = {}
        vacation_days = {}
        working_days = {}
        data_admissao = {}
        data_demissao = {}
        fixed_days_off = {}
        fixed_LQs = {}
        locked_days = {}
        forced_work_days = {}
        complete_cycle_days = {}
        week_template_temp = {}
        week_template = {}
        fixed_compensation_days = {}
        work_special_days = {}

        shift_data = {f"shift_{value}": {} for value in shifts}

        for w in workers_past:
            worker_calendar = matriz_calendario_nao_alterada[matriz_calendario_nao_alterada['employee_id'] == w]
            #logger.info(worker_calendar.to_string(index=False))

            if worker_calendar.empty:
                logger.warning(f"PAST WORKERS: No calendar data found for worker {w}")
                continue
            else:
                logger.info(f"PAST WORKERS: Calendar data found for worker {w}")
            for value in shifts:
                shift_data[f"shift_{value}"][w] = set(worker_calendar[worker_calendar['horario'].isin([value, 'MoT'])]['index'].tolist())
            fixed_LQs[w] = set(worker_calendar[worker_calendar['horario'] == 'LQ']['index'].tolist())
            fixed_days_off[w] = set(worker_calendar[worker_calendar['horario'].isin(['L', 'C'])]['index'].tolist())
            fixed_compensation_days[w] = set(worker_calendar[worker_calendar['horario'] == 'LD']['index'].tolist())
            empty_days[w] = set(worker_calendar[worker_calendar['horario'] == '-']['index'].tolist())
            vacation_days[w] = set(worker_calendar[worker_calendar['horario'] == 'V']['index'].tolist())
            worker_absences[w] = set(worker_calendar[worker_calendar['horario'].isin(['A', 'AP'])]['index'].tolist())
            work_special_days[w] = set(worker_calendar[worker_calendar['horario'] == 'TC']['index'].tolist())


            first_registered_day[w] = worker_calendar['index'].min()
            last_registered_day[w] = worker_calendar['index'].max()
            working_days[w] = fixed_days_off[w] | fixed_LQs[w] | fixed_compensation_days[w]
            for value in shifts:
                working_days[w] |= shift_data[f"shift_{value}"][w]
        
        
        # Process each worker
        for w in workers_complete:
            worker_calendar = matriz_calendario_gd[matriz_calendario_gd['employee_id'] == w]
            
            if worker_calendar.empty:
                logger.warning(f"No calendar data found for worker {w}")
                empty_days[w] = []
                worker_absences[w] = []
                vacation_days[w] = []
                fixed_days_off[w] = []
                fixed_days_off[w] = []
                fixed_LQs[w] = []
                for value in shifts:
                    shift_data[f"shift_{value}"][w] = []
                fixed_compensation_days[w] = []
                locked_days[w] = []
                forced_work_days[w] = []
                complete_cycle_days[w] = []
                week_template_temp[w] = []
                work_special_days[w] = []

                continue
            
            # Find days with specific statuses
            empty_days[w] = worker_calendar[worker_calendar['horario'].isin(['-', 'A-', 'V-' , '0'])]['index'].tolist()
            vacation_days[w] = worker_calendar[worker_calendar['horario'].isin(['V', 'V-'])]['index'].tolist()
            worker_absences[w] = worker_calendar[worker_calendar['horario'].isin(['AP', 'A-', 'A'])]['index'].tolist()
            fixed_days_off[w] = worker_calendar[worker_calendar['horario'].isin(['L', 'C', 'L_DOM'])]['index'].tolist()
            week_template_temp[w] = (worker_calendar.drop_duplicates(subset='index').set_index('index')['workload_template'].fillna('A').astype(str).to_dict())
            fixed_LQs[w] = set(worker_calendar[worker_calendar['horario'] == 'LQ']['index'].tolist())
            fixed_compensation_days[w] = set(worker_calendar[worker_calendar['horario'] == 'LD']['index'].tolist())
            for value in shifts:
                shift_data[f"shift_{value}"][w] = set(worker_calendar[worker_calendar['horario'].isin([value, 'MoT', 'NL' , f'NL{value}'])]['index'].tolist())
            forced_work_days[w] = worker_calendar[worker_calendar['horario'].isin(['NL', [f"NL{value}" for value in shifts]])]['index'].tolist()
            locked_days[w] = set(worker_calendar[worker_calendar['fixed'] == True]['index'].tolist())
            complete_cycle_days[w] = set(worker_calendar[worker_calendar['tipo_ciclo'] == True]['index'].tolist())
            work_special_days[w] = set(worker_calendar[worker_calendar['horario'] == 'TC']['index'].tolist())


        for w in week_template_temp:
                    week_template[w] = {}
                    for week, days in week_to_days.items():
                        week_template[w][week] = week_template_temp[w][days[1]]

        for dummy in dummy_workers:
            original = dummy_workers[dummy]["parent"]
            start = dummy_workers[dummy]["start_date"]
            end = dummy_workers[dummy]["end_date"]
            for value in shifts:
                shift_data[f"shift_{value}"][dummy] = {d for d in shift_data[f"shift_{value}"][original] if start <= d <= end}
            fixed_LQs[dummy] = {d for d in fixed_LQs[original] if start <= d <= end}
            empty_days[dummy] = [d for d in empty_days[original] if start <= d <= end]
            vacation_days[dummy] = {d for d in vacation_days[original] if start <= d <= end}
            fixed_days_off[dummy] = {d for d in fixed_days_off[original] if start <= d <= end}
            worker_absences[dummy] = {d for d in worker_absences[original] if start <= d <= end}
            forced_work_days[dummy] = {d for d in forced_work_days[original] if start <= d <= end}
            work_special_days[dummy] = {d for d in work_special_days[original] if start <= d <= end}
            locked_days[dummy] = {d for d in locked_days[original] if start <= d <= end}
            complete_cycle_days[dummy] = {d for d in complete_cycle_days[original] if start <= d <= end}
            fixed_compensation_days[dummy] = {d for d in fixed_compensation_days[original] if start <= d <= end}
            week_template[dummy] = week_template[original]

        for original in workers_with_dummy:
            for value in shifts:
                shift_data[f"shift_{value}"][original] = {d for d in shift_data[f"shift_{value}"][original] if d <= data_demissao[original]}
            fixed_LQs[original] = {d for d in fixed_LQs[original] if d <= data_demissao[original]}
            empty_days[original] = [d for d in empty_days[original] if d <= data_demissao[original]]
            vacation_days[original] = {d for d in vacation_days[original] if d <= data_demissao[original]}
            fixed_days_off[original] = {d for d in fixed_days_off[original] if d <= data_demissao[original]}
            worker_absences[original] = {d for d in worker_absences[original] if d <= data_demissao[original]}
            forced_work_days[original] = {d for d in forced_work_days[original] if d <= data_demissao[original]}
            work_special_days[original] = {d for d in work_special_days[original] if d <= data_demissao[original]}
            locked_days[original] = {d for d in locked_days[original] if d <= data_demissao[original]}
            complete_cycle_days[original] = {d for d in complete_cycle_days[original] if d <= data_demissao[original]}
            fixed_compensation_days[original] = {d for d in fixed_compensation_days[original] if d <= data_demissao[original]}

        workers = workers_list_with_dummy
        workers_complete = workers_complete_with_dummy
        
        for w in workers_complete:
            # Mark all remaining days after last_registered_day as 'A' (absent)
            if first_registered_day[w] > 0 or last_registered_day[w] > 0:  # Ensure worker was registered at some point
                empty_days[w].extend([d for d in range( 1, first_registered_day[w]) if d not in empty_days[w]])
                empty_days[w].extend([d for d in range(last_registered_day[w] + 1, 366) if d not in empty_days[w]])
            
            empty_days[w] = set(empty_days[w]) - closed_holidays
            for value in shifts:
                empty_days[w] -= shift_data[f"shift_{value}"][w]
            worker_absences[w] = set(worker_absences[w]) - closed_holidays
            vacation_days[w] = set(vacation_days[w]) - closed_holidays
            fixed_days_off[w] = set(fixed_days_off[w]) - closed_holidays
            work_days_per_week[w] = joining_template_with_contract_per_week(np.full(nbr_weeks, contract_type[w]), week_template[w], w, contract_type[w])
            #supostamente nao se faz isto pois nao?

            #worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w] = days_off_atributtion(w, worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w], week_to_days, closed_holidays, work_days_per_week[w], year_range)
            working_days[w] = set(days_of_year) - empty_days[w] - worker_absences[w] - vacation_days[w] - closed_holidays

            if not working_days[w]:
                logger.warning(f"Worker {w} has no working days after processing. This may indicate an issue with the data.")
        logger.info(f"Worker-specific data processed for {len(workers)} workers")

#### Why do this here instead of data treatment and models?
        #for w in workers:
        #    worker_special_days = [d for d in special_days if d in working_days[w]]
        #    total_l_dom[w] = len(worker_special_days) - l_d[w] - tc[w]

        #    logger.info(f"Worker {w} total L DOM adjusted: {total_l_dom[w]} based on special days and contract type {contract_type[w]}")

        for w in workers:
            if contract_type[w] == 6:
                l_d[w] =  l_d[w] + tc[w] 
            elif contract_type[w] in [4,5]:
                total_l[w] = total_l[w] - tc[w]        
            logger.info(f"Worker {w} L_D adjusted: {l_d[w]}, total L: {total_l[w]} based on contract type {contract_type[w]}")

        logger.info("Worker parameters adjusted based on first and last registered days")

        # =================================================================
        # 11. PROCESS ESTIMATIVAS data
        # =================================================================
        logger.info("Processing estimativas data")
        
        # Extract optimization parameters from estimativas
        pess_obj = {}
        min_workers = {}
        max_workers = {}

        # If estimativas has specific data, process it
        if not matriz_estimativas_gd.empty:
            for d in days_of_year:
                # Process pess_obj for working_shift
                for s in shifts:

                    day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['index'] == d) & (matriz_estimativas_gd['turno'] == s)]
                    if not day_shift_data.empty:
                        # Convert float to integer for OR-Tools compatibility
                        pess_obj[(d, s)] = int(round(day_shift_data['pess_obj'].values[0]))
                        min_workers[(d, s)] = int(round(day_shift_data['min_turno'].values[0]))
                        max_workers[(d, s)] = int(round(day_shift_data['max_turno'].values[0]))
                    else:
                        pess_obj[(d, s)] = 0
                        min_workers[(d, s)] = 0
                        max_workers[(d, s)] = 0
            logger.info(f"Processing estimativas data with {len(matriz_estimativas_gd)} records")
            logger.info(f"  - pess_obj: {len(pess_obj) / 2} entries")
            logger.info(f"  - min_workers: {len(min_workers) / 2} entries")
            logger.info(f"  - max_workers: {len(max_workers) / 2} entries")
        else:
            logger.warning("No estimativas data found, using default values for pess_obj, min_workers, max_workers, and working_shift_2")
               



        # =================================================================
        # 12. ADDITIONAL WORKER ASSIGNMENTS
        # =================================================================
        logger.info("Setting up additional worker assignments")
        
        worker_week_shift = {}

        # Iterate over each worker
        for w in workers_complete:
            # Only iterate over weeks that actually exist in week_to_days
            for week in week_to_days.keys():  # Use only existing weeks instead of range(1, 53)
                worker_week_shift[(w, week, 'M')] = 0
                worker_week_shift[(w, week, 'T')] = 0
                
                # Iterate through days of the week for the current week
                for day in week_to_days[week]:
                        
                        # Get the rows for the current week and day
                         # Use WW column instead of isocalendar().week for consistency
                        shift_entries = matriz_calendario_gd[
                            (matriz_calendario_gd['ww'] == week) & 
                            (matriz_calendario_gd['index'] == day) & 
                            (matriz_calendario_gd['employee_id'] == w)
                        ]
                        
                        #logger.info(f"Processing worker {w}, week {week}, day {day}: found {len(shift_entries)} shift entries with types: {shift_entries['horario'].tolist() if not shift_entries.empty else 'None'}")

                        # Check for morning shifts ('M') for the current worker
                        if not shift_entries[shift_entries['horario'] == "M"].empty:
                            # Assign morning shift to the worker for that week
                            worker_week_shift[(w, week, 'M')] = 1  # Set to 1 if morning shift is found

                        # Check for afternoon shifts ('T') for the current worker
                        if not shift_entries[shift_entries['horario'] == "T"].empty:
                            # Assign afternoon shift to the worker for that week
                            worker_week_shift[(w, week, 'T')] = 1  # Set to 1 if afternoon shift is found
                    
                        #logger.info(f"Worker {w} week {week} day {day}: M={worker_week_shift[(w, week, 'M')]}, T={worker_week_shift[(w, week, 'T')]}")
                
            if not worker_week_shift:
                logger.warning(f"No week shifts found for worker {w}, this may indicate an issue with the data.")

        holiday_rules = {}
        sunday_rules = {}
        override_holiday_sunday = {}

        if not matriz_process_rules_gd.empty:
            #logger.info(matriz_process_rules_gd.to_string())
            matriz_process_rules_gd.columns = matriz_process_rules_gd.columns.str.lower()
            required_cols_rules = {"rule_code", "employee_id", "index", "time_off_additional", "time_off_deadline"}
            if not required_cols_rules.issubset(matriz_process_rules_gd.columns):
                logger.warning("Missing required columns for holiday rules")
                holiday_rules = pd.DataFrame()
                sunday_rules = pd.DataFrame()
                override_holiday_sunday = pd.DataFrame()
            else:
                # employee_id is str in merged rules (treat_df_colaborador); workers_complete is int
                matriz_process_rules_gd['employee_id'] = pd.to_numeric(
                    matriz_process_rules_gd['employee_id'], errors='coerce'
                )
                holiday_df = matriz_process_rules_gd[matriz_process_rules_gd["rule_code"] == "ld_holiday"]
                sunday_df = matriz_process_rules_gd[matriz_process_rules_gd["rule_code"] == "ld_sunday"]
                for w in workers_complete:
                    holiday_df_w = holiday_df[holiday_df["employee_id"] == w].drop_duplicates(subset="index").set_index('index')
                    holiday_rules[w] = {
                        "amount": holiday_df_w['time_off_additional'].fillna(1).astype(int).to_dict(),
                        "compensation_limit": holiday_df_w['time_off_deadline'].fillna(15).astype(int).to_dict(),
                    }
                    if not holiday_rules[w]["amount"]:
                        holiday_rules.pop(w, None)

                    sunday_df_w = sunday_df[sunday_df["employee_id"] == w].drop_duplicates(subset="index").set_index('index')
                    sunday_rules[w] = {
                        "amount": sunday_df_w['time_off_additional'].fillna(1).astype(int).to_dict(),
                        "compensation_limit": sunday_df_w['time_off_deadline'].fillna(15).astype(int).to_dict(),
                    }
                    if not sunday_rules[w]["amount"]:
                        sunday_rules.pop(w, None)

                    override_holiday_sunday[w] = holiday_df_w['overlap_sunday_holiday'].fillna('N').to_dict()
                    if not override_holiday_sunday[w]:
                        override_holiday_sunday.pop(w, None)

        logger.info(
            f"Compensatory rules loaded for {len(holiday_rules)} employee(s) (holidays), "
            f"{len(sunday_rules)} employee(s) (sundays)"
        )
        logger.info(f"holiday rules: {holiday_rules}")
        logger.info(f"sunday rules: {sunday_rules}")
        logger.info(f"override rules: {override_holiday_sunday}")

        holiday_past_lds = {}
        sunday_past_lds = {}
        if not matriz_past_lds_gd.empty:

            logger.info(matriz_past_lds_gd.to_string())
            matriz_past_lds_gd.columns = matriz_past_lds_gd.columns.str.lower()
            required_cols_past = {"rule_code","employee_id","schedule_day","time_off_deadline","n_lds_pending"}

            if not required_cols_past.issubset(matriz_past_lds_gd.columns):
                logger.warning("Missing required columns for past LDS data")
                holiday_past_lds = pd.DataFrame()
                sunday_past_lds = pd.DataFrame()
            else:
                matriz_past_lds_gd['employee_id'] = pd.to_numeric(
                    matriz_past_lds_gd['employee_id'], errors='coerce'
                )
                only_dates = matriz_past_lds_gd['schedule_day'].drop_duplicates()
                day_of_year_dict = dict(zip(only_dates.dt.dayofyear - 400, only_dates.dt.strftime('%Y-%m-%d')))
                day_of_year_dict_inverted = dict(zip(only_dates.dt.strftime('%Y-%m-%d'), only_dates.dt.dayofyear - 400))
                index_to_date.update(day_of_year_dict)

                holiday_lds = matriz_past_lds_gd[matriz_past_lds_gd["rule_code"] == "ld_holiday"]
                sunday_lds = matriz_past_lds_gd[matriz_past_lds_gd["rule_code"] == "ld_sunday"]
                for w in workers_complete:
                    holiday_ld_w = holiday_lds[holiday_lds["employee_id"] == w].set_index('schedule_day')
                    holiday_ld_w.index = holiday_ld_w.index.map(lambda d: day_of_year_dict_inverted[d.strftime('%Y-%m-%d')])
                    holiday_past_lds[w] = {
                        "days_&_limit": holiday_ld_w['time_off_deadline'].astype(int).to_dict(),
                        "days_&_amount": holiday_ld_w['n_lds_pending'].fillna(1).astype(int).to_dict(),
                    }
                    sunday_ld_w = sunday_lds[sunday_lds["employee_id"] == w].set_index('schedule_day')
                    sunday_ld_w.index = sunday_ld_w.index.map(lambda d: day_of_year_dict_inverted[d.strftime('%Y-%m-%d')])
                    sunday_past_lds[w] = {
                        "days_&_limit": sunday_ld_w['time_off_deadline'].astype(int).to_dict(),
                        "days_&_amount": sunday_ld_w['n_lds_pending'].fillna(1).astype(int).to_dict(),
                    }
                    if w in holiday_past_lds:
                        for d in holiday_past_lds[w]["days_&_limit"]:
                            holiday_past_lds[w]["days_&_limit"][d] = holiday_past_lds[w]["days_&_limit"][d] - (pd.to_datetime(index_to_date[period[0]]) - pd.to_datetime(index_to_date[d])).days + 1
                    if w in sunday_past_lds:
                        for d in sunday_past_lds[w]["days_&_limit"]:
                            sunday_past_lds[w]["days_&_limit"][d] = sunday_past_lds[w]["days_&_limit"][d] - (pd.to_datetime(index_to_date[period[0]]) - pd.to_datetime(index_to_date[d])).days + 1

            logger.info(f"past holiday : {holiday_past_lds}")
            logger.info(f"past sunday : {sunday_past_lds}")
        # =================================================================
        # 14. ANNUAL VARIABLES
        # =================================================================
        matriz_annual_variables.columns = matriz_annual_variables.columns.str.lower()
        required_cols_annual = {"begin_date", "end_date", "l_dom", "c2d", "l_sab", "l_dom_or_sab", "apply_l_dom", "apply_c2d", "apply_l_sab", "apply_l_dom_or_sab"}
        annual_variables = defaultdict(dict)
        if not required_cols_annual.issubset(matriz_annual_variables.columns):
            logger.warning("Missing required columns for annual variables data")
        else:
            #se alguma vez worker_calendar nao tiver garantido todos os dias dentro, poderá dar erro
            matriz_annual_variables['employee_id'] = matriz_annual_variables['employee_id'].astype(int)
            for w in workers_complete:
                worker_data = matriz_annual_variables[matriz_annual_variables['employee_id'] == w]
                if worker_data.empty:
                    total_l[w] = 0
                    total_l_dom[w] = 0
                    total_l_sab[w] = 0
                    total_l_dom_or_sab[w] = 0
                    c2d[w] = 0
                    c3d[w] = 0
                    l_d[w] = 0
                    cxx[w] = 0
                    tc[w] = 0

                    continue
                worker_row = worker_data.iloc[0]
                start_date = worker_calendar.loc[worker_calendar['schedule_day'] == worker_row.get("begin_date", None), 'index'].iloc[0]
                end_date = worker_calendar.loc[worker_calendar['schedule_day'] == worker_row.get("end_date", None), 'index'].iloc[0]
                annual_variables[w][range(start_date, end_date + 1)] = {
                    "apply_l_dom": worker_row.get("apply_l_dom", True), 
                    "apply_c2d": worker_row.get("apply_c2d", True), 
                    "apply_l_sab": worker_row.get("apply_l_sab", True),
                    "apply_l_dom_or_sab": worker_row.get("apply_l_dom_or_sab", True),
                    "apply_total_l": worker_row.get("apply_total_l", True),
                    "apply_c3d": worker_row.get("apply_c3d", True),
                    "apply_l_d": worker_row.get("apply_l_d", True),
                    "apply_cxx": worker_row.get("apply_cxx", True),
                    "apply_tc": worker_row.get("apply_tc", True),

                }
                total_l[w] = int(worker_row.get('l_total', 0))
                total_l_dom[w] = int(worker_row.get('l_dom', 0))
                total_l_sab[w] = int(worker_row.get('l_sab', 0))
                total_l_dom_or_sab[w] = int(worker_row.get('l_dom_or_sab', 0))
                c2d[w] = int(worker_row.get('c2d', 0))
                c3d[w] = int(worker_row.get('c3d', 0))
                l_d[w] = int(worker_row.get('l_d', 0))
                cxx[w] = int(worker_row.get('cxx', 0))
                tc[w] = int(worker_row.get('tc', 0)) #nome anterior da coluna: dofhc
                t_lq[w] = int(worker_row.get('l_q', 0) + worker_row.get('c2d', 0) + worker_row.get('c3d', 0))

                size = len(worker_data)
                if size > 1:
                    for row in range(1, size):
                        worker_row = worker_data.iloc[row]
                        start_date = worker_calendar.loc[worker_calendar['schedule_day'] == worker_row.get("begin_date", None), 'index'].iloc[0]
                        end_date = worker_calendar.loc[worker_calendar['schedule_day'] == worker_row.get("end_date", None), 'index'].iloc[0]
                        annual_variables[w][range(start_date, end_date + 1)] = {
                            "apply_l_dom": worker_row.get("apply_l_dom", True), 
                            "apply_c2d": worker_row.get("apply_c2d", True), 
                            "apply_l_sab": worker_row.get("apply_l_sab", True), 
                            "apply_l_dom_or_sab": worker_row.get("apply_l_dom_or_sab", True),
                            "apply_total_l": worker_row.get("apply_total_l", True),
                            "apply_c3d": worker_row.get("apply_c3d", True),
                            "apply_l_d": worker_row.get("apply_l_d", True),
                            "apply_cxx": worker_row.get("apply_cxx", True),
                            "apply_tc": worker_row.get("apply_tc", True),
                        }
        logger.info(f"annual variables: {annual_variables}")

        logger.info("[OK] Data processing completed successfully")
        # =================================================================
        # 13. RETURN ALL PROCESSED data
        # =================================================================
        return {
            "matriz_calendario_gd" : matriz_calendario_gd,
            "days_of_year" : days_of_year,
            "sundays" : sundays,
            "holidays" : holidays,
            "special_days" : special_days,
            "closed_holidays" : closed_holidays,
            "empty_days" : empty_days,
            "worker_absences" : worker_absences,
            "vacation_days" : vacation_days ,
            "working_days" : working_days,
            "non_holidays" : non_holidays,
            "start_weekday" : start_weekday,
            "week_to_days" : week_to_days,
            "worker_week_shift" : worker_week_shift,
            "matriz_colaborador_gd" : matriz_colaborador_gd,
            "workers" : workers,
            "contract_type" : contract_type,
            "total_l" : total_l,
            "total_l_dom" : total_l_dom,
            "c2d" : c2d,
            "c3d" : c3d,
            "l_d" : l_d,
            "l_q" : l_q,
            "cxx" : cxx,
            "t_lq" : t_lq,
            "tc" : tc,
            "matriz_estimativas_gd" : matriz_estimativas_gd,
            "pess_obj" : pess_obj,
            "min_workers" : min_workers,
            "max_workers" : max_workers,
            "workers_complete" : workers_complete,
            "workers_complete_cycle" : workers_complete_cycle,
            "first_registered_day" : first_registered_day,
            "last_registered_day" : last_registered_day,
            "fixed_days_off" : fixed_days_off,
            "workers_past": workers_past,
            "period": period,
            "dummy_workers": dummy_workers,
            "workers_with_dummy": workers_with_dummy,
            "unique_dates": unique_dates,
            "index_to_date": index_to_date,
            "fixed_LQs": fixed_LQs,
            "shift_data": shift_data,
            "fixed_compensation_days": fixed_compensation_days,
            "locked_days": locked_days,
            "forced_work_days": forced_work_days,
            "complete_cycle_days": complete_cycle_days,
            "workers_no_contract_changes": workers_no_contract_changes,
            "year_range": year_range,
            "annual_variables": annual_variables,
            "workers_with_dummy": workers_with_dummy,
            "work_days_per_week": work_days_per_week,
            "work_special_days": work_special_days,
            "holiday_rules": holiday_rules,
            "sunday_rules": sunday_rules,
            "override_holiday_sunday": override_holiday_sunday,
            "holiday_past_lds": holiday_past_lds,
            "sunday_past_lds": sunday_past_lds,
        }
        
    except Exception as e:
        logger.error(f"Error in read_data_alcampo: {e}",  exc_info=True)
        raise


