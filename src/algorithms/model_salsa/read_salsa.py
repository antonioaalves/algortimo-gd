import pandas as pd
import math
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
from datetime import date, datetime
import logging
from base_data_project.log_config import get_logger
from src.config import PROJECT_NAME



# Set up logger
logger = get_logger(PROJECT_NAME)

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
        required_dataframes = ['df_colaborador', 'df_estimativas', 'df_calendario']
        missing_dataframes = [df for df in required_dataframes if df not in medium_dataframes]

        required_parameters = ['admissao_proporcional']
        missing_parameters = [param for param in required_parameters if param not in algorithm_treatment_params['treatment_params'] ]
        logger.info(f"algorithm_treatment_params: {algorithm_treatment_params}")
        
        if missing_dataframes:
            raise ValueError(f"Missing required DataFrames: {missing_dataframes}")
        
        if missing_parameters:
            raise ValueError(f"Missing required parameters: {missing_parameters}")
        
    
        
        # Extract DataFrames
        #matriz_colaborador_gd = pd.read_csv('src/algorithms/model_salsa/data/matriz_colaborador_salsa.csv', engine='python')
        #matriz_estimativas_gd = pd.read_csv('src/algorithms/model_salsa/data/matriz_estimativas_salsa.csv', index_col=0)
        #matriz_calendario_gd = pd.read_csv('src/algorithms/model_salsa/data/matriz_calendario_salsa.csv', index_col=0)

        matriz_colaborador_gd = medium_dataframes['df_colaborador'].copy()
        matriz_estimativas_gd = medium_dataframes['df_estimativas'].copy() 
        matriz_calendario_gd = medium_dataframes['df_calendario'].copy()

        admissao_proporcional = algorithm_treatment_params['treatment_params']['admissao_proporcional']
        num_dias_cons = int(algorithm_treatment_params['constraint_params']['NUM_DIAS_CONS'])

        wfm_proc = algorithm_treatment_params['wfm_proc_colab']
        if wfm_proc not in (None, 'None', ''):
            partial_generation = True 
            partial_workers = algorithm_treatment_params['colabs_id_list']
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
        # 2. VALIDATE REQUIRED COLUMNS
        # =================================================================
        required_colaborador_cols = ['matricula', 'L_TOTAL', 'L_DOM', 'C2D', 'C3D', 'L_D', 'CXX', 'VZ', 'data_admissao', 'data_demissao','L_DOM_SALSA', 'L_RES', 'L_RES2', 'seed_5_6', 'n_sem_a_folga']
        required_colaborador_cols = [s.lower() for s in required_colaborador_cols]
        required_calendario_cols = ['colaborador', 'data', 'wd', 'dia_tipo', 'tipo_turno', 'carga_diaria']
        required_calendario_cols = [s.lower() for s in required_calendario_cols]
        required_estimativas_cols = ['data', 'turno', 'media_turno', 'max_turno', 'min_turno', 'pess_obj', 'sd_turno', 'fk_tipo_posto', 'wday' ]
        required_estimativas_cols = [s.lower() for s in required_estimativas_cols]
        
        missing_colab_cols = [col for col in required_colaborador_cols if col not in matriz_colaborador_gd.columns]
        missing_cal_cols = [col for col in required_calendario_cols if col not in matriz_calendario_gd.columns]
        missing_estima_cols = [col for col in required_estimativas_cols if col not in matriz_estimativas_gd.columns]

                
        if missing_colab_cols:
            logger.error(f"Missing required columns in matriz_colaborador: {missing_colab_cols}")
        # if missing_cal_cols:
        #     raise KeyError(f"Missing required columns in matriz_calendario: {missing_cal_cols}")
        # if missing_estima_cols:
        #     raise KeyError(f"Missing required columns in matriz_estimativas: {missing_estima_cols}")
        
        logger.info("[OK] All required columns present in DataFrames")
        
        matriz_calendario_gd = matriz_calendario_gd[matriz_calendario_gd["colaborador"] != "TIPO_DIA"]

        matriz_colaborador_gd = matriz_colaborador_gd[matriz_colaborador_gd["matricula"] != "TIPO_DIA"]

        # =================================================================
        # 3. CALCULATE L_Q FOR colaborador data
        # =================================================================
        logger.info("Calculating L_Q values for workers")
        
        # Check for missing values in required columns
        numeric_cols = ['L_TOTAL', 'L_DOM', 'C2D', 'C3D', 'L_D', 'CXX', 'VZ', 'L_RES', 'L_RES2']
        numeric_cols = [s.lower() for s in numeric_cols]



        for col in numeric_cols:
            if matriz_colaborador_gd[col].isna().any():
                logger.warning(f"Found NaN values in column {col}, filling with 0")
                matriz_colaborador_gd[col] = matriz_colaborador_gd[col].fillna(0)

        
        
        matriz_colaborador_gd["l_q"] = (
            matriz_colaborador_gd["l_total"] - 
            matriz_colaborador_gd["l_dom"] - 
            matriz_colaborador_gd["c2d"] - 
            matriz_colaborador_gd["c3d"] - 
            matriz_colaborador_gd["l_d"] - 
            matriz_colaborador_gd["cxx"] - 
            matriz_colaborador_gd["vz"] - 
            matriz_colaborador_gd["l_res"] - 
            matriz_colaborador_gd["l_res2"]
        )
                
        # =================================================================
        # 4. PROCESS CALENDARIO data
        # =================================================================
        logger.info("Processing calendario data")
        
        # Ensure colaborador column is numeric
        matriz_calendario_gd['colaborador'] = pd.to_numeric(matriz_calendario_gd['colaborador'], errors='coerce')
        invalid_colaborador = matriz_calendario_gd['colaborador'].isna().sum()
        if invalid_colaborador > 0:
            logger.warning(f"Found {invalid_colaborador} invalid colaborador values, removing these rows")
            matriz_calendario_gd = matriz_calendario_gd.dropna(subset=['colaborador'])
        
        # Convert data column to datetime
        try:
            matriz_calendario_gd['data'] = pd.to_datetime(matriz_calendario_gd['data'])
            matriz_estimativas_gd['data'] = pd.to_datetime(matriz_estimativas_gd['data'])
        except Exception as e:
            raise ValueError(f"Error converting data column to datetime: {e}")
        

        # =================================================================
        # 5. IDENTIFY VALID WORKERS (PRESENT IN ALL DATAFRAMES)
        # =================================================================
        logger.info("Identifying valid workers present in all DataFrames")
        
        # Get unique workers from each DataFrame
        workers_colaborador_complete = set(matriz_colaborador_gd['matricula'].dropna().astype(int))
        workers_calendario_complete = set(matriz_calendario_gd['colaborador'].dropna().astype(int))
        if partial_generation == True:
            for w in partial_workers:
                partial_workers_complete = set(matriz_colaborador_gd['matricula'][matriz_colaborador_gd['fk_colaborador'] == w].dropna().astype(int))
                logger.info(f"Unique workers found:")
                logger.info(f"  - In matriz_colaborador_complete: {len(workers_colaborador_complete)} workers")
                logger.info(f"  - In matriz_calendario_complete: {len(workers_calendario_complete)} workers")
                logger.info(f"  - In partial_workers_complete: {len(partial_workers_complete)} workers")
        else:
            partial_workers_complete = set()
        

        workers_colaborador = set(matriz_colaborador_gd[matriz_colaborador_gd['ciclo'] != 'Completo']['matricula'].dropna().astype(int))
        
        logger.info(f"  - In matriz_colaborador (ciclo != 'Completo'): {len(workers_colaborador)} workers")


        if partial_generation == True:
            valid_workers = set(partial_workers_complete).intersection(workers_calendario_complete)
            past_workers = workers_calendario_complete - set(partial_workers_complete)
            valid_workers_complete = workers_colaborador_complete.intersection(workers_calendario_complete)
        else:
            past_workers = set()
            valid_workers = workers_colaborador.intersection(workers_calendario_complete)
            valid_workers_complete = workers_colaborador_complete.intersection(workers_calendario_complete)

        if not valid_workers_complete:
            raise ValueError("No workers found that are present in all required DataFrames")
        

        workers = sorted(list(valid_workers))
        workers_complete = sorted(list(valid_workers_complete))
        workers_complete_cycle = sorted(set(workers_complete)-set(workers))
        workers_past = sorted(list(past_workers))

        logger.info(f"[OK] Final valid workers: {len(workers)} workers for free day atribution")
        logger.info(f"   Worker IDs: {workers[:10]}{'...' if len(workers) > 10 else ''}")
        
        logger.info(f"[OK] Final valid workers (complete): {len(workers_complete)} workers for complete cycle")
        logger.info(f"   Worker IDs (complete): {workers_complete[:10]}{'...' if len(workers_complete) > 10 else ''}")

        logger.info(f"[OK] Final valid past workers): {len(workers_past)} workers for complete cycle")
        logger.info(f"   Worker IDs (past): {workers_past[:10]}{'...' if len(workers_past) > 10 else ''}")

        # Ensure data type consistency before filtering
        matriz_colaborador_gd['matricula'] = matriz_colaborador_gd['matricula'].astype(int)
        matriz_calendario_gd['colaborador'] = matriz_calendario_gd['colaborador'].astype(int)
        
        matriz_colaborador_nao_alterada = matriz_colaborador_gd.copy()
        matriz_colaborador_gd = matriz_colaborador_gd[matriz_colaborador_gd['matricula'].isin(workers_complete)]
        matriz_calendario_nao_alterada = matriz_calendario_gd.copy()
        matriz_calendario_gd = matriz_calendario_gd[matriz_calendario_gd['colaborador'].isin(workers_complete)]

        
        logger.info(f"Filtered DataFrames to valid workers:")
        logger.info(f"  - matriz_colaborador: {matriz_colaborador_gd.shape}")
        logger.info(f"  - matriz_estimativas: {matriz_estimativas_gd.shape}")
        logger.info(f"  - matriz_calendario: {matriz_calendario_gd.shape}")
        
        # =================================================================
        # 6. EXTRACT DAYS AND DATE INFORMATION
        # =================================================================
        logger.info("Extracting days and date information")
        
        days_of_year = sorted(matriz_calendario_gd['data'].dt.dayofyear.unique().tolist())
        logger.info(f"Days of year: {len(days_of_year)} days (from {min(days_of_year)} to {max(days_of_year)})")
        
        # =================================================================
        # 7. IDENTIFY SPECIAL DAYS
        # =================================================================
        logger.info("Identifying special days")
        
        # Define shifts and special days
        shifts = ["M", "T", "L", "LQ", "LD", "F", "V", "A", "-"]
        
        sundays = sorted(matriz_calendario_gd[matriz_calendario_gd['wd'] == 'Sun']['data'].dt.dayofyear.unique().tolist())

        holidays = sorted(matriz_calendario_gd[
            (matriz_calendario_gd['wd'] != 'Sun') & 
            (matriz_calendario_gd["dia_tipo"] == "domYf")
        ]['data'].dt.dayofyear.unique().tolist())
        
        closed_holidays = set(matriz_calendario_gd[
            matriz_calendario_gd['tipo_turno'] == "F"
        ]['data'].dt.dayofyear.unique().tolist())
        
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
        unique_dates = sorted(matriz_calendario_gd['data'].unique())

        if unique_dates:
            # Get start weekday from the first date in the calendar data (not estimativas)
            # Sort calendar by date to get the actual first date
            matriz_calendario_sorted = matriz_calendario_gd.sort_values('data')
            first_date_row = matriz_calendario_sorted.iloc[0]

            # Get the year from the first date and create January 1st of that year
            year = first_date_row['data'].year
            january_1st = pd.Timestamp(year=year, month=1, day=1)

            # If your system uses 1=Monday, 7=Sunday, add 1:
            start_weekday = january_1st.weekday() + 1
        
            
            logger.info(f"First date in dataset: {first_date_row['data']}")
            logger.info(f"Year: {year}, January 1st: {january_1st}")
            logger.info(f"Start weekday (January 1st): {start_weekday}")
            
            # Create week to days mapping using WW column and day of year
            week_to_days = {}
            week_to_days_salsa = {}
            
            # Process each unique date in the calendar (remove duplicates by date)
            unique_calendar_dates = matriz_calendario_gd.drop_duplicates(['data']).sort_values('data')
            
            for _, row in unique_calendar_dates.iterrows():
                day_of_year = row['data'].dayofyear
                week_number = row['ww']  # Use WW column for week number
                
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
            
            # Sort days within each week to ensure chronological order
            for week in week_to_days:
                week_to_days[week].sort()
            
            for week in week_to_days_salsa:
                week_to_days_salsa[week].sort()


                        
            logger.info(f"Week to days mapping created using calendar data:")
            logger.info(f"  - Start weekday (from first date): {start_weekday}")
            logger.info(f"  - Weeks found: {sorted(week_to_days.keys())}")
            logger.info(f"  - Total weeks: {len(week_to_days)}")
            logger.info(f"  - Sample weeks: {dict(list(week_to_days.items())[-3:])}")
                
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
        min_calendar_date = matriz_calendario_gd['data'].min()
        max_calendar_date = matriz_calendario_gd['data'].max()
        min_day_of_year = min_calendar_date.dayofyear
        max_day_of_year = (max_calendar_date - min_calendar_date).days + 1

        logger.info(f"Calendar date range: {min_calendar_date} to {max_calendar_date}")
        logger.info(f"Calendar day of year range: {min_day_of_year} to {max_day_of_year}")
        
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
        fixed_M = {}
        fixed_T = {}
        fixed_compensation_days = {}
        #if partial_generation == True:
        #    valid_workers_complete = partial_workers_complete | valid_workers
        #    workers_complete = sorted(list(valid_workers_complete))
        #    workers_complete_cycle = sorted(set(workers_complete)-set(workers))

        # Process each worker

        for w in workers_past:
            worker_calendar = matriz_calendario_nao_alterada[matriz_calendario_nao_alterada['colaborador'] == w]
            #logger.info(worker_calendar.to_string(index=False))

            if worker_calendar.empty:
                logger.warning(f"PAST WORKERS: No calendar data found for worker {w}")
                continue
            else:
                logger.info(f"PAST WORKERS: Calendar data found for worker {w}")
            fixed_M[w] = worker_calendar[worker_calendar['tipo_turno'] == 'M']['data'].dt.dayofyear.tolist()
            fixed_T[w] = worker_calendar[worker_calendar['tipo_turno'] == 'T']['data'].dt.dayofyear.tolist()
            fixed_LQs[w] = worker_calendar[worker_calendar['tipo_turno'] == 'LQ']['data'].dt.dayofyear.tolist()
            fixed_days_off[w] = worker_calendar[worker_calendar['tipo_turno'] == 'L']['data'].dt.dayofyear.tolist()
            fixed_compensation_days[w] = worker_calendar[worker_calendar['tipo_turno'] == 'LD']['data'].dt.dayofyear.tolist()
            empty_days[w] = worker_calendar[worker_calendar['tipo_turno'] == '-']['data'].dt.dayofyear.tolist()
            vacation_days[w] = worker_calendar[worker_calendar['tipo_turno'] == 'V']['data'].dt.dayofyear.tolist()
            worker_absences[w] = worker_calendar[(worker_calendar['tipo_turno'] == 'A') | (worker_calendar['tipo_turno'] == 'AP')]['data'].dt.dayofyear.tolist()
            work_day_hours[w] = worker_calendar['carga_diaria'].fillna(8).to_numpy()[::2].astype(int)
            logger.info(f"worker hours {w},\n{work_day_hours[w]}\nlen {len(work_day_hours[w])}")

            first_registered_day[w] = worker_calendar['data'].dt.dayofyear.min()
            last_registered_day[w] = worker_calendar['data'].dt.dayofyear.max()
            working_days[w] = set(fixed_T[w]) | set(fixed_days_off[w]) | set(fixed_M[w]) | set(fixed_LQs[w])


        for w in workers_complete:
            worker_calendar = matriz_calendario_gd[matriz_calendario_gd['colaborador'] == w]
            
            if worker_calendar.empty:
                logger.warning(f"No calendar data found for worker {w}")
                empty_days[w] = []
                worker_absences[w] = []
                vacation_days[w] = []
                fixed_days_off[w] = []
                fixed_LQs[w] = []
                work_day_hours[w] = []
                fixed_M[w] = []
                fixed_T[w] = []

                continue
            
            # Find days with specific statuses
            worker_empty = worker_calendar[(worker_calendar['tipo_turno'] == '-') | (worker_calendar['tipo_turno'] == 'A-') | (worker_calendar['tipo_turno'] == 'V-')]['data'].dt.dayofyear.tolist()
            w_holiday = worker_calendar[(worker_calendar['tipo_turno'] == 'V') | (worker_calendar['tipo_turno'] == 'V-')]['data'].dt.dayofyear.tolist()
            w_absences = worker_calendar[(worker_calendar['tipo_turno'] == 'A') | (worker_calendar['tipo_turno'] == 'AP') | (worker_calendar['tipo_turno'] == 'A-')]['data'].dt.dayofyear.tolist()
            worker_fixed_days_off = worker_calendar[(worker_calendar['tipo_turno'] == 'L')]['data'].dt.dayofyear.tolist()
            f_day_complete_cycle = worker_calendar[worker_calendar['tipo_turno'].isin(['L', 'L_DOM'])]['data'].dt.dayofyear.tolist()
            worker_work_day_hours = worker_calendar['carga_diaria'].fillna(8).to_numpy()[::2].astype(int)
            logger.info(f"worker hours {w},\n{worker_work_day_hours}\nlen {len(worker_work_day_hours)}")
            worker_present_days = set(worker_calendar['data'].dt.dayofyear.tolist())

            # Days where worker should potentially appear but doesn't
            days_not_in_calendar = set(days_of_year) - worker_present_days
            logger.info(f" ERROR Worker {w} days not in calendar data: {sorted(list(days_not_in_calendar))}")
            # Add these missing days to empty_days
            #worker_empty.extend(list(days_not_in_calendar))


            empty_days[w] = worker_empty
            vacation_days[w] = w_holiday
            worker_absences[w] = w_absences
            fixed_days_off[w] = worker_fixed_days_off
            free_day_complete_cycle[w] = f_day_complete_cycle
            work_day_hours[w] = worker_work_day_hours
            fixed_LQs[w] = {}

    
            worker_data = matriz_colaborador_gd[matriz_colaborador_gd['matricula'] == w]
            if w not in past_workers :
                worker_row = worker_data.iloc[0]

                # MODIFIED: Fix date handling - don't convert Timestamp to datetime
                admissao_value = worker_row.get('data_admissao', None)
                logger.info(f"Processing worker {w} with data_admissao: {admissao_value}")
                demissao_value = worker_row.get('data_demissao', None)
                logger.info(f"Processing worker {w} with data_demissao: {demissao_value}")
            else:
                admissao_value = None
                demissao_value = None
                logger.info(f"Worker {w} is a past worker, setting data_admissao and data_demissao to None")

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
                            admissao_day_of_year = admissao_date.dayofyear
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
                        data_demissao[w] = 0
                        demissao_date = None
                        
                    if demissao_date is not None:
                        # Check if demissao is within calendar date range (not day of year)
                        if min_calendar_date <= demissao_date <= max_calendar_date:
                            demissao_day_of_year = demissao_date.dayofyear
                            data_demissao[w] = int(demissao_day_of_year)
                            logger.info(f"Worker {w} data_demissao: {demissao_date.date()} -> day of year {demissao_day_of_year}")
                        else:
                            data_demissao[w] = 0
                            logger.info(f"Worker {w} data_demissao {demissao_date.date()} is outside calendar range ({min_calendar_date.date()} to {max_calendar_date.date()}), set to 0")
                    else:
                        data_demissao[w] = 0
                            
                except Exception as e:
                    logger.warning(f"Could not parse data_demissao '{demissao_value}' for worker {w}: {e}")
                    data_demissao[w] = 0
            else:
                data_demissao[w] = 0


        # Track first and last registered days
            if w in matriz_calendario_gd['colaborador'].values:
                first_registered_day[w] = worker_calendar['data'].dt.dayofyear.min()
                if w not in past_workers:
                    if  first_registered_day[w] < data_admissao[w]:
                        first_registered_day[w] = data_admissao[w]
                logger.info(f"Worker {w} first registered day: {first_registered_day[w]}")
            else:
                first_registered_day[w] = 0

            if w in matriz_calendario_gd['colaborador'].values:
                last_registered_day[w] = worker_calendar['data'].dt.dayofyear.max()
                # Only adjust if there's an actual dismissal date (not 0)
                if w not in past_workers:
                    if w not in past_workers and data_demissao[w] > 0 and last_registered_day[w] > data_demissao[w]:
                        last_registered_day[w] = data_demissao[w]
                logger.info(f"Worker {w} last registered day: {last_registered_day[w]}")
            else:
                last_registered_day[w] = 0

        for w in workers_complete:
            # Mark all remaining days after last_registered_day as 'A' (absent)
            if first_registered_day[w] > 0 or last_registered_day[w] > 0:  # Ensure worker was registered at some point
                empty_days[w].extend([d for d in range( 1, first_registered_day[w]) if d not in empty_days[w]])
                empty_days[w].extend([d for d in range(last_registered_day[w] + 1, 366) if d not in empty_days[w]])
            

            empty_days[w] = set(empty_days[w]) - closed_holidays
            worker_absences[w] = set(worker_absences[w]) - closed_holidays
            fixed_days_off[w] = set(fixed_days_off[w]) - closed_holidays
            vacation_days[w] = set(vacation_days[w]) - closed_holidays
            free_day_complete_cycle[w] = sorted(set(free_day_complete_cycle[w]) - closed_holidays)
            worker_info = matriz_colaborador_gd[matriz_colaborador_gd['matricula'] == w]

            if not worker_info.empty:
                tipo_contrato = worker_info.iloc[0].get('tipo_contrato', 'Contract Error')
            else:
                logger.warning(f"No collaborator data found for worker {w}")
                tipo_contrato = 'Contract Error'
            if tipo_contrato != 8:
                worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w] = days_off_atributtion(w, worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w], week_to_days_salsa, closed_holidays, None)
                working_days[w] = set(days_of_year) - empty_days[w] - worker_absences[w] - vacation_days[w] - closed_holidays 
                #logger.info(f"Worker {w} working days after processing: {working_days[w]}")

                if not working_days[w]:
                    logger.warning(f"Worker {w} has no working days after processing. This may indicate an issue with the data.")




        logger.info(f"Worker-specific data processed for {len(workers)} workers")

        # # =================================================================
        # # 9. PROCESS WORKER-SPECIFIC data
        # # =================================================================
        
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
        l_q = {}
        cxx = {}
        t_lq = {}
        first_week_5_6 = {}
        work_days_per_week = {}
        week_compensation_limit = {}
        has_week_compensation_limit = False
        has_max_work_days_7 = False

        for w in workers:
            worker_data = matriz_colaborador_gd[matriz_colaborador_gd['matricula'] == w]
            
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
                first_week_5_6[w] = 0
                work_days_per_week[w] = 0
                week_compensation_limit[w] = 0


            else:
                worker_row = worker_data.iloc[0]  # Take first row if multiple
                # Extract contract information
                contract_type[w] = worker_row.get('tipo_contrato', 'Contract Error')
                total_l[w] = int(worker_row.get('l_total', 0))
                total_l_dom[w] = int(worker_row.get('l_dom_salsa', 0))
                c2d[w] = int(worker_row.get('c2d', 0))
                c3d[w] = int(worker_row.get('c3d', 0))
                l_d[w] = int(worker_row.get('l_d', 0))
                l_q[w] = int(worker_row.get('l_q', 0))
                cxx[w] = int(worker_row.get('cxx', 0))
                t_lq[w] = int(worker_row.get('l_q', 0) + worker_row.get('c2d', 0) + worker_row.get('c3d', 0))

                first_week_5_6[w] = int(worker_row.get('seed_5_6', 0))
                week_compensation_limit[w] = int(worker_row.get('n_sem_a_folga', 0))
                if (has_week_compensation_limit == False and week_compensation_limit[w] != 0):
                    has_week_compensation_limit = True

                if contract_type[w] == 8:
                    if (first_week_5_6[w] != 0):
                        work_days_per_week[w] = populate_week_seed_5_6(first_week_5_6[w], data_admissao[w], week_to_days_salsa)
                    else:
                        work_days_per_week[w] = populate_week_fixed_days_off(fixed_days_off[w], fixed_LQs[w], week_to_days_salsa)
                    check_5_6_pattern_consistency(w, fixed_days_off[w], fixed_LQs[w], week_to_days, work_days_per_week[w])
                    worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w] = days_off_atributtion(w, worker_absences[w], vacation_days[w], fixed_days_off[w], fixed_LQs[w], week_to_days_salsa, closed_holidays, work_days_per_week[w])
                    working_days[w] = set(days_of_year) - empty_days[w] - worker_absences[w] - vacation_days[w] - closed_holidays 
                else:
                    work_days_per_week[w] = [5] * 52
                if not working_days[w]:
                    logger.warning(f"Worker {w} has no working days after processing. This may indicate an issue with the data.")
                #logger.info(f"Worker {w} contract information extracted: "
                #            f"Contract Type: {contract_type[w]}, "
                #            f"Total L: {total_l[w]}, "
                #            f"Total L DOM: {total_l_dom[w]}, "
                #            f"C2D: {c2d[w]}, "
                #            f"C3D: {c3d[w]}, "
                #            f"L_D: {l_d[w]}, "
                #            f"L_Q: {l_q[w]}, "
                #            f"CXX: {cxx[w]}, "
                #            f"T_LQ: {t_lq[w]}, "
                #            f"5 ou 6: {first_week_5_6[w]}, "
                #            f"Data Admissao: {data_admissao[w]}, "
                #            f"Data Demissao: {data_demissao[w]}")
        
        for w in workers:
            #if contract_type[w] == 8:
            #    print(w, first_week_5_6[w])

            if contract_type[w] == 'Contract Error':
                logger.error(f"Worker {w} has contract type error, removing from workers list")
                workers.pop(workers.index(w))  # Remove worker with contract error
            if total_l[w] < 0:
                logger.error(f"Worker {w} has non-positive total_l: {total_l[w]}, removing from workers list")
                workers.pop(workers.index(w))  # Remove worker with contract error

        if (has_max_work_days_7 == False and num_dias_cons == 7):
            has_max_work_days_7 = True

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
            row = matriz_colaborador_gd.loc[matriz_colaborador_gd["matricula"] == w]

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
            if (last_registered_day[w] > 0 and last_registered_day[w] < 364):
                proportion[w] = (last_registered_day[w]- first_registered_day[w])  / (days_of_year[-1] - first_registered_day[w])
                logger.info(f"Adjusting worker {w} parameters based on last registered day {last_registered_day[w]} with proportion[w] {proportion[w]:.2f}")
                total_l[w] = int(round(proportion[w] * total_l[w]))
                total_l_dom[w] = int(round(proportion[w] * total_l_dom[w]))
                c2d[w] = int(math.floor(proportion[w] * c2d[w]))
                c3d[w] = int(math.floor(proportion[w] * c3d[w]))
                l_d[w] = int(round(proportion[w] * l_d[w]))
                l_q[w] = int(round(proportion[w] * l_q[w]))
                cxx[w] = int(round(proportion[w] * cxx[w]))
                t_lq[w] = int(round(proportion[w] * t_lq[w]))
                
                logger.info(f"Worker {w} parameters adjusted for last registered day {last_registered_day[w]}: "
                            f"Total L: {total_l[w]}, "
                            f"Total L DOM: {total_l_dom[w]}, "
                            f"C2D: {c2d[w]}, "
                            f"C3D: {c3d[w]}, "
                            f"L_D: {l_d[w]}, "
                            f"L_Q: {l_q[w]}, "
                            f"CXX: {cxx[w]}, "
                            f"T_LQ: {t_lq[w]}, ")

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

                    day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['data'].dt.dayofyear == d) & (matriz_estimativas_gd['turno'] == s)]
                    if not day_shift_data.empty:
                        # Convert float to integer for OR-Tools compatibility
                        pess_obj[(d, s)] = int(round(day_shift_data['pess_obj'].values[0]) * 8)
                    else:
                        pess_obj[(d, s)] = 0  # or any default value you prefer
                
                # Process min/max workers for all shifts
                for shift_type in shifts:
                    day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['data'].dt.dayofyear == d) & (matriz_estimativas_gd['turno'] == shift_type)]
                    if not day_shift_data.empty:
                                    # Convert floats to integers for OR-Tools compatibility
                                    min_workers[(d, shift_type)] = int(round(day_shift_data['min_turno'].values[0]) * 8)
                                    max_workers[(d, shift_type)] = int(round(day_shift_data['max_turno'].values[0]) * 8)

            logger.info(f"Processing estimativas data with {len(matriz_estimativas_gd)} records")
            logger.info(f"  - pess_obj: {len(pess_obj)} entries")
            logger.info(f"  - min_workers: {len(min_workers)} entries")
            logger.info(f"  - max_workers: {len(max_workers)} entries")
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
                            (matriz_calendario_gd['data'].dt.day_of_year == day) & 
                            (matriz_calendario_gd['colaborador'] == w)
                        ]
                        
                        # Check for morning shifts ('M') for the current worker
                        if not shift_entries[shift_entries['tipo_turno'] == "M"].empty:
                            # Assign morning shift to the worker for that week
                            worker_week_shift[(w, week, 'M')] = 1  # Set to 1 if morning shift is found

                        # Check for afternoon shifts ('T') for the current worker
                        if not shift_entries[shift_entries['tipo_turno'] == "T"].empty:
                            # Assign afternoon shift to the worker for that week
                            worker_week_shift[(w, week, 'T')] = 1  # Set to 1 if afternoon shift is found
                    
                        #logger.info(f"Worker {w} week {week} day {day}: M={worker_week_shift[(w, week, 'M')]}, T={worker_week_shift[(w, week, 'T')]}")
                
            if not worker_week_shift:
                logger.warning(f"No week shifts found for worker {w}, this may indicate an issue with the data.")

        working_shift_2 = ["M", "T"]
        # =================================================================
        # 13. DEFINITION OF ALGORITHM COUNTRY 
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
        # 14. RETURN ALL PROCESSED data
        # =================================================================
        return {
            "matriz_calendario_gd": matriz_calendario_gd,         # 0
            "days_of_year": days_of_year,                         # 1
            "sundays": sundays,                                   # 2
            "holidays": holidays,                                 # 3
            "special_days": special_days,                         # 4
            "closed_holidays": closed_holidays,                   # 5
            "empty_days": empty_days,                             # 6
            "worker_absences": worker_absences,                     # 7
            "vacation_days": vacation_days,                         # 8
            "working_days": working_days,                         # 9
            "non_holidays": non_holidays,                         # 10
            "start_weekday": start_weekday,                       # 11
            "week_to_days": week_to_days,                         # 12
            "worker_week_shift": worker_week_shift,               # 13
            "matriz_colaborador_gd": matriz_colaborador_gd,       # 14
            "workers": workers,                                   # 15
            "contract_type": contract_type,                       # 16
            "total_l": total_l,                                   # 17
            "total_l_dom": total_l_dom,                           # 18
            "c2d": c2d,                                           # 19
            "c3d": c3d,                                           # 20
            "l_d": l_d,                                           # 21
            "l_q": l_q,                                           # 22
            "cxx": cxx,                                           # 23
            "t_lq": t_lq,                                         # 24
            "matriz_estimativas_gd": matriz_estimativas_gd,       # 25
            "pess_obj": pess_obj,                                 # 26
            "min_workers": min_workers,                           # 27
            "max_workers": max_workers,                           # 28
            "working_shift_2": working_shift_2,                   # 29
            "workers_complete": workers_complete,                 # 30
            "workers_complete_cycle": workers_complete_cycle,     # 31
            "free_day_complete_cycle": free_day_complete_cycle,   # 32
            "week_to_days_salsa": week_to_days_salsa,             # 33
            "first_registered_day": first_registered_day,         # 34
            "admissao_proporcional": admissao_proporcional,       # 35
            "role_by_worker": role_by_worker,                     # 36
            "data_admissao": data_admissao,                       # 37
            "data_demissao": data_demissao,                       # 38
            "last_registered_day": last_registered_day,           # 39
            "fixed_days_off": fixed_days_off,                     # 40
            "proportion": proportion,                             # 41
            "fixed_LQs": fixed_LQs,                               # 42
            "work_day_hours": work_day_hours,                     # 43
            "work_days_per_week": work_days_per_week,             # 44
            "week_compensation_limit": week_compensation_limit,   # 45
            "num_dias_cons": num_dias_cons,                       # 46
            "country": country,                                   # 47
            "fixed_M": fixed_M,                                   # 48
            "fixed_T": fixed_T,                                   # 49
            "partial_workers_complete": partial_workers_complete, # 50
            "workers_past": workers_past,                         # 51
            "fixed_compensation_days": fixed_compensation_days,   # 52
            }
        
    except Exception as e:
        logger.error(f"Error in read_data_salsa: {e}", exc_info=True)
        raise

def consecutive_days(vacations_in_week, nbr_vacations, cut_off, days):
    if nbr_vacations <= 2:
        #print("week too short")
        return False
    if cut_off == 5:
        if not all(day in vacations_in_week for day in days[2:5]):
            print(f"holidays not in a row {vacations_in_week}")
            return False
        if vacations_in_week[-1] != days[4]:
            print(f"holidays dont end on friday {vacations_in_week[-1]} {days[4]}")
            return False
    elif cut_off == 6:
        if not all(day in vacations_in_week for day in days[3:6]):
            print(f"holidays not in a row {vacations_in_week}")
            return False
        if vacations_in_week[-1] != days[5]:
            print(f"holidays dont end on saturday {vacations_in_week[-1]} {days[5]}")
            return False
    return True

def days_off_atributtion(w, absences, vacations, fixed_days_off, fixed_LQs, week_to_days_salsa, closed_holidays, work_days_per_week):
    fixed_LQs = []
    for week, days in week_to_days_salsa.items():
        if len(days) <= 6:
            continue

        days_set = set(days)
        days_off = days_set.intersection(fixed_days_off.union(fixed_LQs))
        absences_in_week = days_set.intersection(absences.union(closed_holidays))
        nbr_absences = len(absences_in_week)
        vacations_in_week = days_set.intersection(vacations.union(closed_holidays))
        nbr_vacations = len(vacations_in_week)

        if work_days_per_week is None or work_days_per_week[week - 1] == 5:

            if nbr_absences < 5:
                if consecutive_days(sorted(vacations_in_week), nbr_vacations, 5, days) == False:
                    continue

            if len(days_off) >= 2:
                logger.warning(f"For week with absences {week}, {w} already has {days_off} day off, not changing anything")
                continue

            atributing_days = sorted(days_set - closed_holidays)
            if len(days_off) == 1:
                logger.warning(f"For week with absences {week}, {w} already has {days_off} day off")
                only_day_off = sorted(days_off)[0]
                if only_day_off == atributing_days[-1] and only_day_off == days[6] and atributing_days[-2] == days[5]:
                    l2 = atributing_days[-2]
                    absences -= {l2}
                    vacations -= {l2}
                    fixed_LQs.append(l2)

                elif only_day_off == atributing_days[-2] and only_day_off == days[5] and atributing_days[-1] == days[6]:
                    l1 = atributing_days[-1]
                    absences -= {l1}
                    vacations -= {l1}
                    fixed_days_off |= {l1}
                    fixed_days_off -= {only_day_off}
                    fixed_LQs.append(only_day_off)
                else:
                    #last day insured not to be an already fixed day off
                    l1 = sorted(set(atributing_days) - {only_day_off})[-1]
                    absences -= {l1}
                    vacations -= {l1}
                    fixed_days_off |= {l1}
            else:
                l1 = atributing_days[-1]
                l2 = atributing_days[-2]

                if l1 == days[6] and l2 == days[5]:
                    absences -= {l2, l1}
                    vacations -= {l2, l1}
                    fixed_days_off |= {l1}
                    fixed_LQs.append(l2)
                else:
                    absences -= {l2,l1}
                    vacations -= {l2,l1}
                    fixed_days_off |= {l2,l1}
                
        else:
            if len(days_off) > 0:
                logger.warning(f"For week with absences {week}, {w} already has {days_off} day off, not changing. (6 working days week)")
                continue
            if nbr_absences <= 6:
                if consecutive_days(sorted(vacations_in_week), nbr_vacations, 6, days) == False:
                    continue
            atributing_days = sorted(days_set - closed_holidays)
            l1 = atributing_days[-1]
            absences -= {l1}
            vacations -= {l1}
            fixed_days_off |= {l1}
    
    return absences, vacations, fixed_days_off, fixed_LQs

def populate_week_seed_5_6(first_week_5_6, data_admissao, week_to_days):
    nbr_weeks = len(week_to_days)
    work_days_per_week = np.full(nbr_weeks, 5)

    # Find starting week, default to week 0 if not found
    week = next((wk for wk, val in week_to_days.items() if data_admissao in val), 1) - 1
    other = 6 if first_week_5_6 == 5 else 5
    work_days_per_week[week:] = np.tile(np.array([first_week_5_6, other]), (nbr_weeks // 2) + 1)[:nbr_weeks - week]

    return work_days_per_week.astype(int)

def populate_week_fixed_days_off(fixed_days_off, fixed_LQs, week_to_days):
    nbr_weeks = len(week_to_days)
    work_days_per_week = np.full(nbr_weeks, 5)
    week_5_days = 0
    for week, days in week_to_days.items():
        days_set = set(days)
        days_off_week = days_set.intersection(fixed_days_off.union(fixed_LQs))
        if len(days_off_week) > 1:
            week_5_days = week - 1
            break

    if week_5_days % 2 == 0:
        logger.info(f"Found week that has to be of 5 working days in week {week_5_days + 1}, "
                    f"with days {days_off_week} since its even, first week will start with 5")
        work_days_per_week= np.tile(np.array([5, 6]), (nbr_weeks // 2) + 1)[:nbr_weeks]
    else:
        logger.info(f"Found week that has to be of 5 working days in week {week_5_days + 1}, "
                    f"with days {days_off_week} since its odd, first week will start with 6")
        work_days_per_week= np.tile(np.array([6, 5]), (nbr_weeks // 2) + 1)[:nbr_weeks]

    return work_days_per_week.astype(int)

def check_5_6_pattern_consistency(w, fixed_days_off, fixed_LQs, week_to_days, work_days_per_week):
    for week, days in week_to_days.items():

        days_set = set(days)
        actual_days_off = len(days_set.intersection(fixed_days_off.union(fixed_LQs)))
        expected_days_off = 7 - work_days_per_week[week - 1]

        if actual_days_off > expected_days_off:
            logger.error(f"For worker {w}, in week {week} by contract they're supposed to work "
                         f"{work_days_per_week[week - 1]} days but have received {actual_days_off} "
                         f"days off: {days_set.intersection(fixed_days_off.union(fixed_LQs))}. Process will be Infeasible!")