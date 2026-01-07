import pandas as pd
import math
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from datetime import date, datetime
import logging
from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config as get_config_manager
from src.algorithms.model_salsa.auxiliar_functions_salsa import days_off_atributtion, populate_week_seed_5_6, populate_week_fixed_days_off, check_5_6_pattern_consistency


# Set up logger
logger = get_logger(get_config_manager().system.project_name)

def read_data_salsa(medium_dataframes: Dict[str, pd.DataFrame], algorithm_treatment_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Enhanced version of read_data_salsa with comprehensive logging and error checks.
    
    Args:
        medium_dataframes: Dictionary containing the required DataFrames
        
    Returns:
        Dictonary containing all processed data elements for the algorithm
        
    Raises:
        ValueError: If required DataFrames are missing or invalid
        KeyError: If required columns are missing from DataFrames
    """
    try:
        logger.info("Starting enhanced data reading for salsa algorithm")
        
        # =================================================================
        # 1. VALIDATE INPUT data
        # =================================================================
        logger.info(f"algorithm_treatment_params: {algorithm_treatment_params}")
        
        matriz_colaborador_gd = medium_dataframes['df_colaborador'].copy()
        matriz_estimativas_gd = medium_dataframes['df_estimativas'].copy() 
        matriz_calendario_gd = medium_dataframes['df_calendario'].copy()

        admissao_proporcional = algorithm_treatment_params['admissao_proporcional']
        num_dias_cons = int(algorithm_treatment_params['NUM_DIAS_CONS'])

        start_date = pd.to_datetime(algorithm_treatment_params['start_date']).dayofyear
        end_date = pd.to_datetime(algorithm_treatment_params['end_date']).dayofyear
        period = [start_date, end_date]

        logger.info(f"Start and end Time:")
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
        logger.info(f"  - admissao_proportional: {admissao_proporcional}")
        logger.info(f"  - numero de dias consecutivos de trabalho: {num_dias_cons}")
        logger.info(f"  - wfm_proc_colab: {wfm_proc}, if it has value, its a partial generation -> {partial_generation}.")
        if partial_generation == True:
            logger.info(f"  - partial_workers: {partial_workers} workers")


        # =================================================================
        # 3. PROCESS CALENDARIO data
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
            matriz_estimativas_gd['data'] = pd.to_datetime(matriz_estimativas_gd['data'])
        except Exception as e:
            raise ValueError(f"Error converting data column to datetime: {e}")
        

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
        

        workers_colaborador = set(matriz_colaborador_gd[matriz_colaborador_gd['ciclo'] != 'Completo']['employee_id'].dropna().astype(int))
        
        logger.info(f"  - In matriz_colaborador (ciclo != 'Completo'): {len(workers_colaborador)} workers")


        if partial_generation == True:
            valid_workers = set(partial_workers_complete).intersection(workers_calendario_complete)
            past_workers = workers_calendario_complete - set(partial_workers_complete)
            valid_workers_complete = workers_colaborador_complete.intersection(workers_calendario_complete)

            workers = sorted(valid_workers)
            workers_complete = workers
            complete = complete = matriz_colaborador_gd[(matriz_colaborador_gd['ciclo'] == 'Completo') & (matriz_colaborador_gd['employee_id'] == w)]
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

        logger.info(f"[OK] Final valid past workers): {len(workers_past)} workers for complete cycle")
        logger.info(f"   Worker IDs (past): {workers_past[:10]}{'...' if len(workers_past) > 10 else ''}")

        # Ensure data type consistency before filtering
        matriz_colaborador_gd['employee_id'] = matriz_colaborador_gd['employee_id'].astype(int)
        matriz_calendario_gd['employee_id'] = matriz_calendario_gd['employee_id'].astype(int)
        
        matriz_colaborador_nao_alterada = matriz_colaborador_gd.copy()
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
        
        sundays = sorted(matriz_calendario_gd[matriz_calendario_gd['wd'] == 'Sun']['index'].unique().tolist())

        holidays = sorted(matriz_calendario_gd[
            (matriz_calendario_gd['wd'] != 'Sun') & 
            (matriz_calendario_gd["dia_tipo"] == "domYf")
        ]['index'].unique().tolist())
        
        closed_holidays = set(matriz_calendario_gd[
            matriz_calendario_gd['horario'] == "F"
        ]['index'].unique().tolist())
        
        special_days = sorted(set(holidays))

        logger.info(f"Special days identified:")
        logger.info(f"  - Sundays: {len(sundays)} days")
        logger.info(f"  - Holidays (non-Sunday): {len(holidays)} days")
        logger.info(f"  - Closed holidays: {len(closed_holidays)} days")
        logger.info(f"  - Total special days: {len(special_days)} days")

        
        # =================================================================
        # 8. CALCULATE ADDITIONAL PARAMETERS
        # =================================================================
        logger.info("Calculating additional parameters")
        
        # Working days (non-special days)
        non_holidays = [d for d in days_of_year if d not in closed_holidays]  # Alias for compatibility
        
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
                (matriz_estimativas_gd['data'].dt.month == 6) &
                (matriz_estimativas_gd['data'].dt.day == 25),
                'data'].dt.year.iloc[0]
            january_1st = pd.Timestamp(year=year, month=1, day=1)

            # If your system uses 1=Monday, 7=Sunday, add 1:
            start_weekday = 1
        
            
            logger.info(f"First date in dataset: {first_date_row['schedule_day']}")
            logger.info(f"Year: {year}, January 1st: {january_1st}")
            logger.info(f"Start weekday (January 1st): {start_weekday}")
            
            # Create week to days mapping using WW column and day of year
            week_to_days = {}
            week_to_days_salsa = {}
            
            # Process each unique date in the calendar (remove duplicates by date)
            unique_calendar_dates = matriz_calendario_gd.drop_duplicates(['index']).sort_values('index')
            week_number = 1
            for _, row in unique_calendar_dates.iterrows():
                day_of_year = row['index']
                #week_number = row['ww']  # Use WW column for week number
                
                # Initialize the week list if it doesn't exist
                if week_number not in week_to_days:
                    week_to_days[week_number] = []
                
                if week_number not in week_to_days_salsa:
                    week_to_days_salsa[week_number] = []
                
                if day_of_year not in week_to_days_salsa[week_number]:
                    week_to_days_salsa[week_number].append(day_of_year)
                # Add the day to its corresponding week (avoid duplicates)
                if day_of_year not in week_to_days[week_number] and day_of_year in non_holidays:
                    week_to_days[week_number].append(day_of_year)
                if day_of_year % 7 == 0:
                    week_number += 1

            # Sort days within each week to ensure chronological order
            for week in week_to_days:
                week_to_days[week].sort()
            
            for week in week_to_days_salsa:
                week_to_days_salsa[week].sort()


                        
            logger.info(f"Week to days mapping created using calendar data:")
            logger.info(f"  - Start weekday (from first date): {start_weekday}")
            logger.info(f"  - Weeks found: {sorted(week_to_days.keys())}")
            logger.info(f"  - Total weeks: {len(week_to_days)}")
            logger.info(f"  - Sample weeks: {dict(list(week_to_days.items())[:])}")
                
        else:
            start_weekday = 0
            week_to_days = {}
            logger.warning("No unique dates found in matriz_calendario_gd, week calculations may be incomplete")  
            
        logger.info(f"Week calculation:")
        logger.info(f"  - Start weekday: {start_weekday}")
        logger.info(f"  - Number of weeks: {len(week_to_days)}")
        #logger.info(f"  - Working days: {len(working_days)} days")

        # =================================================================
        # 9. PROCESS WORKER-SPECIFIC data
        # =================================================================
        logger.info("Processing worker-specific data")

        # Get the date range from matriz_calendario for validation
        min_calendar_date = matriz_calendario_gd['schedule_day'].min()
        max_calendar_date = matriz_calendario_gd['schedule_day'].max()

        jan1 = matriz_estimativas_gd.loc[
        (matriz_estimativas_gd['data'].dt.month == 1) &
        (matriz_estimativas_gd['data'].dt.day == 1) &
        (matriz_estimativas_gd['data'].dt.year == year)
        ]

        dec31 = matriz_estimativas_gd.loc[
        (matriz_estimativas_gd['data'].dt.month == 12) &
        (matriz_estimativas_gd['data'].dt.day == 31) &
        (matriz_estimativas_gd['data'].dt.year == year)
        ]
  
        min_day_year = jan1['index'].iloc[0] if not jan1.empty else 1
        max_day_year = dec31['index'].iloc[0] if not dec31.empty else 365

        logger.info(f"Calendar date range: {min_calendar_date} to {max_calendar_date}")
        logger.info(f"Calendar day of year range: {min_day_year} to {max_day_year}")
        year_range = [min_day_year, max_day_year]
        period = [start_date + min_day_year - 1, end_date + min_day_year - 1]
        logger.info(f"Adapted and Final Period Start and end Time:")
        logger.info(f"Start: {period[0]}")
        logger.info(f"End: {period[1]}")

        # Initialize dictionaries for worker-specific information
        empty_days = {}
        worker_absences = {}
        vacation_days = {}
        last_registered_day = {}
        first_registered_day = {}
        working_days = {}
        free_day_complete_cycle = {}
        data_admissao = {}
        data_demissao = {}
        fixed_days_off = {}
        fixed_LQs = {}
        work_day_hours = {}
        shift_M = {}
        shift_T = {}
        fixed_compensation_days = {}
       
        for w in workers_past:
            worker_calendar = matriz_calendario_nao_alterada[matriz_calendario_nao_alterada['employee_id'] == w]
            #logger.info(worker_calendar.to_string(index=False))

            if worker_calendar.empty:
                logger.warning(f"PAST WORKERS: No calendar data found for worker {w}")
                continue
            else:
                logger.info(f"PAST WORKERS: Calendar data found for worker {w}")
            shift_M[w] = set(worker_calendar[worker_calendar['horario'] == 'M']['index'].tolist())
            shift_T[w] = set(worker_calendar[worker_calendar['horario'] == 'T']['index'].tolist())
            fixed_LQs[w] = set(worker_calendar[worker_calendar['horario'] == 'LQ']['index'].tolist())
            fixed_days_off[w] = set(worker_calendar[worker_calendar['horario'] == 'L']['index'].tolist())
            fixed_compensation_days[w] = set(worker_calendar[worker_calendar['horario'] == 'LD']['index'].tolist())
            empty_days[w] = set(worker_calendar[worker_calendar['horario'] == '-']['index'].tolist())
            vacation_days[w] = set(worker_calendar[worker_calendar['horario'] == 'V']['index'].tolist())
            worker_absences[w] = set(worker_calendar[(worker_calendar['horario'] == 'A') | (worker_calendar['horario'] == 'AP')]['index'].tolist())
            work_day_hours[w] = worker_calendar['carga_diaria'].fillna(8).to_numpy()[::2].astype(int)

            logger.info(f"worker hours {w},\n{work_day_hours[w]}\nlen {len(work_day_hours[w])}")

            first_registered_day[w] = worker_calendar['index'].min()
            last_registered_day[w] = worker_calendar['index'].max()
            working_days[w] = set(shift_T[w]) | set(fixed_days_off[w]) | set(shift_M[w]) | set(fixed_LQs[w]) | set(fixed_compensation_days)


        for w in workers_complete:
            worker_calendar = matriz_calendario_gd[matriz_calendario_gd['employee_id'] == w]
            
            if worker_calendar.empty:
                logger.warning(f"No calendar data found for worker {w}")
                empty_days[w] = []
                worker_absences[w] = []
                vacation_days[w] = []
                fixed_days_off[w] = []
                fixed_LQs[w] = []
                work_day_hours[w] = []
                shift_M[w] = []
                shift_T[w] = []
                fixed_compensation_days[w] = []

                continue
            
            # Find days with specific statuses
            empty_days[w] = worker_calendar[(worker_calendar['horario'] == '-') | (worker_calendar['horario'] == 'A-') | (worker_calendar['horario'] == 'V-') | (worker_calendar['horario'] == '0')]['index'].tolist()
            vacation_days[w] = worker_calendar[(worker_calendar['horario'] == 'V') | (worker_calendar['horario'] == 'V-')]['index'].tolist()
            worker_absences[w] = worker_calendar[(worker_calendar['horario'] == 'A') | (worker_calendar['horario'] == 'AP') | (worker_calendar['horario'] == 'A-')]['index'].tolist()
            fixed_days_off[w] = worker_calendar[(worker_calendar['horario'] == 'L') | (worker_calendar['horario'] == 'C')]['index'].tolist()
            free_day_complete_cycle[w] = worker_calendar[worker_calendar['horario'].isin(['L', 'L_DOM'])]['index'].tolist()
            work_day_hours[w] = (worker_calendar.drop_duplicates(subset='index').set_index('index')['carga_diaria'].fillna(8).astype(int).to_dict())
            logger.info(f"worker hours {w},\n{work_day_hours[w]}\nlen {len(work_day_hours[w])}")
            fixed_LQs[w] = set(worker_calendar[worker_calendar['horario'] == 'LQ']['index'].tolist())
            fixed_compensation_days[w] = set(worker_calendar[worker_calendar['horario'] == 'LD']['index'].tolist())
            shift_M[w] = worker_calendar[(worker_calendar['horario'] == 'M') | (worker_calendar['horario'] == 'MoT')]['index'].tolist()
            shift_T[w] = worker_calendar[(worker_calendar['horario'] == 'T') | (worker_calendar['horario'] == 'MoT')]['index'].tolist()
    
            worker_data = matriz_colaborador_gd[matriz_colaborador_gd['employee_id'] == w]
            worker_row = worker_data.iloc[0]

            # MODIFIED: Fix date handling - don't convert Timestamp to datetime
            admissao_value = worker_row.get('data_admissao', None)
            logger.info(f"Processing worker {w} with data_admissao: {admissao_value}")
            demissao_value = worker_row.get('data_demissao', None)
            logger.info(f"Processing worker {w} with data_demissao: {demissao_value}")

            # Convert data_admissao to day of year
            if admissao_value is not None and not pd.isna(admissao_value):
                try:
                    if isinstance(admissao_value, (datetime, pd.Timestamp)):
                        admissao_date = admissao_value
                    elif isinstance(admissao_value, str):
                        admissao_date = pd.to_datetime(admissao_value)
                    else:
                        data_admissao[w] = 0
                        admissao_date = None
                        
                    if admissao_date is not None:
                        # Check if admissao is within calendar date range (not day of year)
                        if min_calendar_date <= admissao_date <= max_calendar_date:
                            admissao_day_of_year = worker_calendar.loc[worker_calendar['schedule_day'] == admissao_date, 'index'].iloc[0]
                            data_admissao[w] = int(admissao_day_of_year)
                            logger.info(f"Worker {w} data_admissao: {admissao_date.date()} -> day of year {admissao_day_of_year}")
                        else:
                            data_admissao[w] = 0
                            logger.info(f"Worker {w} data_admissao {admissao_date.date()} is outside calendar range ({min_calendar_date.date()} to {max_calendar_date.date()}), set to 0")
                    else:
                        data_admissao[w] = 0
                            
                except Exception as e:
                    logger.warning(f"Could not parse data_admissao '{admissao_value}' for worker {w}: {e}")
                    data_admissao[w] = 0
            else:
                data_admissao[w] = 0

            # Convert data_demissao to day of year
            if demissao_value is not None and not pd.isna(demissao_value):
                try:
                    if isinstance(demissao_value, (datetime, pd.Timestamp)):
                        demissao_date = demissao_value
                    elif isinstance(demissao_value, str):
                        demissao_date = pd.to_datetime(demissao_value)
                    else:
                        data_demissao[w] = max_day + 1
                        demissao_date = None
                        
                    if demissao_date is not None:
                        # Check if demissao is within calendar date range (not day of year)
                        if min_calendar_date <= demissao_date <= max_calendar_date:
                            demissao_day_of_year = worker_calendar.loc[worker_calendar['schedule_day'] == demissao_date, 'index'].iloc[0]
                            data_demissao[w] = int(demissao_day_of_year)
                            logger.info(f"Worker {w} data_demissao: {demissao_date.date()} -> day of year {demissao_day_of_year}")
                        else:
                            data_demissao[w] = max_day + 1
                            logger.info(f"Worker {w} data_demissao {demissao_date.date()} is outside calendar range ({min_calendar_date.date()} to {max_calendar_date.date()}), set to 0")
                    else:
                        data_demissao[w] = max_day + 1
                            
                except Exception as e:
                    logger.warning(f"Could not parse data_demissao '{demissao_value}' for worker {w}: {e}")
                    data_demissao[w] = max_day + 1
            else:
                data_demissao[w] = max_day + 1


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

        for w in workers_complete:
            # Mark all remaining days after last_registered_day as 'A' (absent)
            if first_registered_day[w] > 0 or last_registered_day[w] > 0:  # Ensure worker was registered at some point
                empty_days[w].extend([d for d in range(1, first_registered_day[w]) if d not in empty_days[w]])
                empty_days[w].extend([d for d in range(last_registered_day[w] + 1, max_day) if d not in empty_days[w]])
            

            empty_days[w] = set(empty_days[w]) - closed_holidays
            worker_absences[w] = set(worker_absences[w]) - closed_holidays
            fixed_days_off[w] = set(fixed_days_off[w]) - closed_holidays
            vacation_days[w] = set(vacation_days[w]) - closed_holidays
            free_day_complete_cycle[w] = sorted(set(free_day_complete_cycle[w]) - closed_holidays)
            worker_info = matriz_colaborador_gd[matriz_colaborador_gd['employee_id'] == w]

            if not worker_info.empty:
                tipo_contrato = worker_info.iloc[0].get('tipo_contrato', 'Contract Error')
            else:
                logger.warning(f"No collaborator data found for worker {w}")
                tipo_contrato = 'Contract Error'
            if tipo_contrato != 8:
                worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w] = days_off_atributtion(w, worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w], week_to_days_salsa, closed_holidays, None, year_range)
                working_days[w] = set(days_of_year) - empty_days[w] - worker_absences[w] - vacation_days[w] - closed_holidays 
                #logger.info(f"Worker {w} working days after processing: {working_days[w]}")

                if not working_days[w]:
                    logger.warning(f"Worker {w} has no working days after processing. This may indicate an issue with the data.")

        logger.info(f"Worker-specific data processed for {len(workers)} workers")
        
        # =================================================================
        # 10.1. EXTRACT WORKER CONTRACT INFORMATION
        # =================================================================
        logger.info("Extracting worker contract information")
        
        # Create dictionaries for worker contract data
        contract_type = {}
        total_l = {}
        total_l_dom = {}
        c2d = {}
        c3d = {}
        l_d = {}
        cxx = {}
        first_week_5_6 = {}
        work_days_per_week = {}
        week_compensation_limit = {}
        has_week_compensation_limit = False
        has_max_work_days_7 = False if (num_dias_cons != 7) else True

        for w in workers:
            worker_data = matriz_colaborador_gd[matriz_colaborador_gd['employee_id'] == w]
            
            if worker_data.empty:
                logger.warning(f"No contract data found for worker {w}")
                # Set default values
                contract_type[w] = 'Contract Error'  # Default contract type
                total_l[w] = 0
                total_l_dom[w] = 0
                c2d[w] = 0
                c3d[w] = 0
                l_d[w] = 0
                cxx[w] = 0
                first_week_5_6[w] = 0
                work_days_per_week[w] = 0
                week_compensation_limit[w] = 0


            else:
                worker_row = worker_data.iloc[0]  # Take first row if multiple
                # Extract contract information
                contract_type[w] = worker_row.get('tipo_contrato', 'Contract Error')
                total_l[w] = int(worker_row.get('l_total', 0))
                total_l_dom[w] = int(worker_row.get('l_dom', 0))
                c2d[w] = int(worker_row.get('c2d', 0))
                c3d[w] = int(worker_row.get('c3d', 0))
                l_d[w] = int(worker_row.get('l_d', 0))
                cxx[w] = int(worker_row.get('cxx', 0))

                first_week_5_6[w] = int(worker_row.get('seed_5_6', 0))
                week_compensation_limit[w] = int(worker_row.get('n_sem_a_folga', 0))
                if (has_week_compensation_limit == False and week_compensation_limit[w] != 0):
                    has_week_compensation_limit = True

                if contract_type[w] == 8:
                    if (first_week_5_6[w] != 0):
                        work_days_per_week[w] = populate_week_seed_5_6(first_week_5_6[w], data_admissao[w], week_to_days_salsa)
                    else:
                        work_days_per_week[w] = populate_week_fixed_days_off(fixed_days_off[w], fixed_LQs[w], week_to_days_salsa)
                    check_5_6_pattern_consistency(w, fixed_days_off[w], fixed_LQs[w], week_to_days_salsa, work_days_per_week[w])
                    worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w] = days_off_atributtion(w, worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w], week_to_days_salsa, closed_holidays, work_days_per_week[w], year_range)
                    working_days[w] = set(days_of_year) - empty_days[w] - worker_absences[w] - vacation_days[w] - closed_holidays - fixed_compensation_days[w]
                else:
                    work_days_per_week[w] = [5] * 52
                if not working_days[w]:
                    logger.warning(f"Worker {w} has no working days after processing. This may indicate an issue with the data.")
        
        for w in workers:
            if contract_type[w] == 'Contract Error':
                logger.error(f"Worker {w} has contract type error, removing from workers list")
                workers.pop(workers.index(w))  # Remove worker with contract error
            if total_l[w] < 0:
                logger.error(f"Worker {w} has non-positive total_l: {total_l[w]}, removing from workers list")
                workers.pop(workers.index(w))  # Remove worker with contract error
        logger.info(f"Contract information extracted for {len(workers)} workers")

        # =================================================================
        # 10.1B. OPERATIONAL ROLES (manager / keyholder / normal)
        # =================================================================
        logger.info("Deriving operational roles (1=manager, 2=keyholder, empty=normal) a partir dos dados")

        role_by_worker = {}
        managers = []
        keyholders = []

        # Escolher a coluna onde vêm os códigos 1/2 (ou texto), por ordem de preferência
        role_col_data = ["prioridade_folgas"]
        role_col = next((c for c in role_col_data if c in matriz_colaborador_gd.columns), None)

        #if not role_col:
        #    logger.warning("Nenhuma coluna de nível encontrada entre %s. " "Todos tratados como 'normal'.", possible_role_cols)
        #    
        #    for w in workers_complete:
        #        role_by_worker[w] = "normal"
        #else:
        logger.info("Usando coluna de nível: %s", role_col)

        for w in workers_past:
            row = matriz_colaborador_nao_alterada.loc[matriz_colaborador_nao_alterada['matricula'] == w]
            if row.empty:
                logger.info(f"calendario vazio {w}")
                role = "normal"
            else:
                raw = row.iloc[0].get(role_col)

                # Mapear 1/2/NaN e também aceitar 'manager'/'keyholder' como texto
                # 1 → manager ; 2 → keyholder ; vazio/outros → normal
                if pd.isna(raw):
                    role = "normal"
                else:
                    s = str(raw).strip().lower()
                    
                    # fallback por texto
                    if s == "manager":
                        role = "manager"
                    elif s == "keyholder":
                        role = "keyholder"
                    else:
                        role = "normal"

            role_by_worker[w] = role
            if role == "manager":
                managers.append(w)
            elif role == "keyholder":
                keyholders.append(w)

        for w in workers_complete:
            row = matriz_colaborador_gd.loc[matriz_colaborador_gd["employee_id"] == w]

            if row.empty:
                role = "normal"
            else:
                raw = row.iloc[0].get(role_col)

                # Mapear 1/2/NaN e também aceitar 'manager'/'keyholder' como texto
                # 1 → manager ; 2 → keyholder ; vazio/outros → normal
                if pd.isna(raw):
                    role = "normal"
                else:
                    s = str(raw).strip().lower()
                    
                    # fallback por texto
                    if s == "manager":
                        role = "manager"
                    elif s == "keyholder":
                        role = "keyholder"
                    else:
                        role = "normal"

            role_by_worker[w] = role
            if role == "manager":
                managers.append(w)
            elif role == "keyholder":
                keyholders.append(w)

        logger.info("Roles derived: managers=%d, keyholders=%d, normals=%d", len(managers), len(keyholders), len(workers_complete) - len(managers) - len(keyholders))

        # =================================================================
        # 10.2. ADAPT PROPORTIONS FOR WORKERS FOR FIRST AND LAST DAYS
        # =================================================================
        logger.info("Adjusting worker parameters based on last registered days")
        proportion = {}
        for w in workers:
            if (last_registered_day[w] > min_day_year and last_registered_day[w] < max_day_year):
                proportion[w] = (last_registered_day[w]- first_registered_day[w])  / (max_day_year)
                logger.info(f"Adjusting worker {w} parameters based on last registered day {last_registered_day[w]} with proportion[w] {proportion[w]:.2f}")
                total_l[w] = int(round(proportion[w] * total_l[w]))
                total_l_dom[w] = int(round(proportion[w] * total_l_dom[w]))
                c2d[w] = int(math.floor(proportion[w] * c2d[w]))
                c3d[w] = int(math.floor(proportion[w] * c3d[w]))
                l_d[w] = int(round(proportion[w] * l_d[w]))
                cxx[w] = int(round(proportion[w] * cxx[w]))
                
                logger.info(f"Worker {w} parameters adjusted for last registered day {last_registered_day[w]}: "
                            f"Total L: {total_l[w]}, "
                            f"Total L DOM: {total_l_dom[w]}, "
                            f"C2D: {c2d[w]}, "
                            f"C3D: {c3d[w]}, "
                            f"L_D: {l_d[w]}, "
                            f"CXX: {cxx[w]}, ")

        logger.info("Worker parameters adjusted based on first and last registered days")

        # =================================================================
        # 11. PROCESS ESTIMATIVAS data
        # =================================================================
        logger.info("Processing estimativas data")
        
        # Extract optimization parameters from estimativas
        pess_obj = {}
        min_workers = {}
        max_workers = {}
        working_shift = ["M", "T"]

        # If estimativas has specific data, process it
        if not matriz_estimativas_gd.empty: 
            for d in days_of_year: 
                # Process pess_obj for working_shift
                for s in working_shift:
                    day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['index'] == d) & (matriz_estimativas_gd['turno'] == s)]
                    if not day_shift_data.empty:
                        # Convert float to integer for OR-Tools compatibility
                        pess_obj[(d, s)] = int(round(day_shift_data['pess_obj'].values[0]) * 8)
                        min_workers[(d, s)] = int(round(day_shift_data['min_turno'].values[0]) * 8)
                        max_workers[(d, s)] = int(round(day_shift_data['max_turno'].values[0]) * 8)
                    else:
                        pess_obj[(d, s)] = 0
                        min_workers[(d, s)] = 0
                        max_workers[(d, s)] = 0
                    #print(f"day {d} and shift {s}: pessobj {pess_obj[(d, s)]/8}, min_workers {min_workers[(d, s)]/8}, max_workers {max_workers[(d, s)]/8}")
            logger.info(f"Processing estimativas data with {len(matriz_estimativas_gd)} records")
            logger.info(f"  - pess_obj: {len(pess_obj)/2} entries")
            logger.info(f"  - min_workers: {len(min_workers)/2} entries")
            logger.info(f"  - max_workers: {len(max_workers)/2} entries")
        # =================================================================
        # 12. DEFINITION OF ALGORITHM COUNTRY 
        # =================================================================

        if (has_max_work_days_7 == True and has_week_compensation_limit == True):
            country = "spain"
            logger.info("Detected country to be spain")
        elif (has_max_work_days_7 == False and has_week_compensation_limit == False):
            country = "portugal"
            logger.info("Detected country to be portugal")

        else:
            country = "undefined"
            logger.error(f"Some variables are true and others false, something is not being correctly received:"
                         f"\n\t\t\t7 work days in a row  -> {has_max_work_days_7},"
                         f"\n\t\t\tweek compensation limit -> {has_week_compensation_limit}."
                         )
        logger.info("[OK] Data processing completed successfully")
        
        # =================================================================
        # 13. RETURN ALL PROCESSED data
        # =================================================================
        return {
            "matriz_calendario_gd": matriz_calendario_gd,        
            "days_of_year": days_of_year,                        
            "sundays": sundays,                                  
            "holidays": holidays,                                
            "special_days": special_days,                        
            "closed_holidays": closed_holidays,                  
            "empty_days": empty_days,                            
            "worker_absences": worker_absences,
            "vacation_days": vacation_days,
            "working_days": working_days,                        
            "non_holidays": non_holidays,                        
            "start_weekday": start_weekday,                      
            "week_to_days": week_to_days,                        
            "matriz_colaborador_gd": matriz_colaborador_gd,      
            "workers": workers,                                  
            "contract_type": contract_type,                      
            "total_l": total_l,                                  
            "total_l_dom": total_l_dom,                          
            "c2d": c2d,                                          
            "c3d": c3d,                                          
            "l_d": l_d,                                          
            "cxx": cxx,                                          
            "matriz_estimativas_gd": matriz_estimativas_gd,      
            "pess_obj": pess_obj,                                
            "min_workers": min_workers,                          
            "max_workers": max_workers,                          
            "workers_complete": workers_complete,                
            "workers_complete_cycle": workers_complete_cycle,    
            "free_day_complete_cycle": free_day_complete_cycle,  
            "week_to_days_salsa": week_to_days_salsa,            
            "first_registered_day": first_registered_day,        
            "admissao_proporcional": admissao_proporcional,      
            "role_by_worker": role_by_worker,                    
            "data_admissao": data_admissao,                      
            "data_demissao": data_demissao,                      
            "last_registered_day": last_registered_day,          
            "fixed_days_off": fixed_days_off,                    
            "proportion": proportion,                            
            "fixed_LQs": fixed_LQs,                              
            "work_day_hours": work_day_hours,                    
            "work_days_per_week": work_days_per_week,            
            "week_compensation_limit": week_compensation_limit,  
            "num_dias_cons": num_dias_cons,                      
            "country": country,                                  
            "shift_M": shift_M,                                  
            "shift_T": shift_T,                                  
            "partial_workers_complete": partial_workers_complete,
            "workers_past": workers_past,                        
            "fixed_compensation_days": fixed_compensation_days,  
            "year_range": year_range,
            "unique_dates": unique_dates,
            "period": period,
            "managers": managers,
            "keyholders": keyholders,
            }
        
    except Exception as e:
        logger.error(f"Error in read_data_salsa: {e}", exc_info=True)
        raise
