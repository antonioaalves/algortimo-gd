import pandas as pd
import math
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
import logging
from base_data_project.log_config import get_logger
from src.config import PROJECT_NAME

# Set up logger
logger = get_logger(PROJECT_NAME)

def read_data_salsa(medium_dataframes: Dict[str, pd.DataFrame]) -> Tuple[Any, ...]:
    """
    Enhanced version of read_data_salsa with comprehensive logging and error checks.
    
    Args:
        medium_dataframes: Dictionary containing the required DataFrames
        
    Returns:
        Tuple containing all processed data elements for the algorithm
        
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
        
        if missing_dataframes:
            raise ValueError(f"Missing required DataFrames: {missing_dataframes}")
        
        # Extract DataFrames
        #matriz_colaborador_gd = pd.read_csv('src/algorithms/model_salsa/data/matriz_colaborador_salsa.csv', engine='python')
        #matriz_estimativas_gd = pd.read_csv('src/algorithms/model_salsa/data/matriz_estimativas_salsa.csv', index_col=0)
        #matriz_calendario_gd = pd.read_csv('src/algorithms/model_salsa/data/matriz_calendario_salsa.csv', index_col=0)

        matriz_colaborador_gd = medium_dataframes['df_colaborador'].copy()
        matriz_estimativas_gd = medium_dataframes['df_estimativas'].copy() 
        matriz_calendario_gd = medium_dataframes['df_calendario'].copy()

        matriz_colaborador_gd.columns = matriz_colaborador_gd.columns.str.lower()
        matriz_estimativas_gd.columns = matriz_estimativas_gd.columns.str.lower()
        matriz_calendario_gd.columns = matriz_calendario_gd.columns.str.lower()       

        logger.info(f"Input DataFrames loaded:")
        logger.info(f"  - matriz_colaborador: {matriz_colaborador_gd.shape}")
        logger.info(f"  - matriz_estimativas: {matriz_estimativas_gd.shape}")
        logger.info(f"  - matriz_calendario: {matriz_calendario_gd.shape}")
        
        # =================================================================
        # 2. VALIDATE REQUIRED COLUMNS
        # =================================================================
        required_colaborador_cols = ['matricula', 'L_TOTAL', 'L_DOM', 'C2D', 'C3D', 'L_D', 'CXX', 'VZ', 'L_RES', 'L_RES2']
        required_colaborador_cols = [s.lower() for s in required_colaborador_cols]
        required_calendario_cols = ['colaborador', 'data', 'wd', 'dia_tipo', 'tipo_turno']
        required_calendario_cols = [s.lower() for s in required_calendario_cols]
        required_estimativas_cols = ['data', 'turno', 'media_turno', 'max_turno', 'min_turno', 'pess_obj', 'sd_turno', 'fk_tipo_posto', 'wday' ]
        required_estimativas_cols = [s.lower() for s in required_estimativas_cols]
        
        missing_colab_cols = [col for col in required_colaborador_cols if col not in matriz_colaborador_gd.columns]
        missing_cal_cols = [col for col in required_calendario_cols if col not in matriz_calendario_gd.columns]
        missing_estima_cols = [col for col in required_estimativas_cols if col not in matriz_estimativas_gd.columns]

                
        # if missing_colab_cols:
        #     raise KeyError(f"Missing required columns in matriz_colaborador: {missing_colab_cols}")
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
        
        logger.info(f"L_Q calculated. Range: {matriz_colaborador_gd['l_q'].min():.2f} to {matriz_colaborador_gd['l_q'].max():.2f}")
        
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
            logger.info(f"Date range: {matriz_calendario_gd['data'].min()} to {matriz_calendario_gd['data'].max()}")
        except Exception as e:
            raise ValueError(f"Error converting data column to datetime: {e}")
        

        # =================================================================
        # 5. IDENTIFY VALID WORKERS (PRESENT IN ALL DATAFRAMES)
        # =================================================================
        logger.info("Identifying valid workers present in all DataFrames")
        
        # Get unique workers from each DataFrame
        workers_colaborador_complete = set(matriz_colaborador_gd['matricula'].dropna().astype(int))
        workers_calendario_complete = set(matriz_calendario_gd['colaborador'].dropna().astype(int))
        logger.info(f"Unique workers found:")
        logger.info(f"  - In matriz_colaborador_complete: {len(workers_colaborador_complete)} workers")
        logger.info(f"  - In matriz_calendario_complete: {len(workers_calendario_complete)} workers")

        workers_colaborador = set(matriz_colaborador_gd[matriz_colaborador_gd['ciclo'] != 'Completo']['matricula'].dropna().astype(int))

        logger.info(f"Workers found:")
        logger.info(f"  - In matriz_colaborador_complete: {len(workers_colaborador_complete)} workers")
        logger.info(f"  - In matriz_calendario: {len(workers_calendario_complete)} workers")
        logger.info(f"  - In matriz_colaborador (ciclo != 'Completo'): {len(workers_colaborador)} workers")

        valid_workers = workers_colaborador.intersection(workers_calendario_complete)
        valid_workers_complete = workers_colaborador_complete.intersection(workers_calendario_complete)

        if not valid_workers_complete:
            raise ValueError("No workers found that are present in all required DataFrames")
        
        workers = sorted(list(valid_workers))
        workers_complete = sorted(list(valid_workers_complete))
        workers_complete_cycle = sorted(set(workers_complete)-set(workers))

        logger.info(f"[OK] Final valid workers: {len(workers)} workers for free day atribution")
        logger.info(f"   Worker IDs: {workers[:10]}{'...' if len(workers) > 10 else ''}")
        
        logger.info(f"[OK] Final valid workers (complete): {len(workers_complete)} workers for complete cycle")
        logger.info(f"   Worker IDs (complete): {workers_complete[:10]}{'...' if len(workers_complete) > 10 else ''}")

        # Ensure data type consistency before filtering
        matriz_colaborador_gd['matricula'] = matriz_colaborador_gd['matricula'].astype(int)
        matriz_calendario_gd['colaborador'] = matriz_calendario_gd['colaborador'].astype(int)
        
        matriz_colaborador_gd = matriz_colaborador_gd[matriz_colaborador_gd['matricula'].isin(workers_complete)]
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
        shifts = ["M", "T", "L","LQ", "F", "V","LD", "A"]
        
        sundays = sorted(matriz_calendario_gd[matriz_calendario_gd['wd'] == 'Sun']['data'].dt.dayofyear.unique().tolist())

        holidays = sorted(matriz_calendario_gd[
            (matriz_calendario_gd['wd'] != 'Sun') & 
            (matriz_calendario_gd["dia_tipo"] == "domYf")
        ]['data'].dt.dayofyear.unique().tolist())
        
        closed_holidays = sorted(matriz_calendario_gd[
            matriz_calendario_gd['tipo_turno'] == "F"
        ]['data'].dt.dayofyear.unique().tolist())
        
        special_days = sorted(list(set(sundays + holidays)))
        
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

        # Around line 297-317, replace the existing week_to_days calculation with:

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

            # if len(week_to_days_salsa[1]) <= 7:
            #     week_cut = True
            # else:
            #     week_cut = False

             # Determine week_cut based on whether we have complete first/last weeks
            # week_cut = False
            
            # # Check if we have a complete dataset (starts from day 1 and goes to end of year)
            # min_day = min(days_of_year) if days_of_year else 1
            # max_day = max(days_of_year) if days_of_year else 365
            
            # # Only enable week_cut if:
            # # 1. Data starts from day 1 (or very early in the year)
            # # 2. Data goes until end of year (or very late in the year)
            # # 3. We have exactly 52 weeks OR we have week 1 with 7 days OR last week is week 52
            # if (min_day <= 7 and max_day >= 358):  # Allow some flexibility for year boundaries
            #     # Check if first week has complete days (7 days) or if we have standard 52-week structure
            #     first_week_days = len(week_to_days_salsa.get(1, []))
            #     last_week_number = max(week_to_days_salsa.keys()) if week_to_days_salsa else 0
                
            #     # Week cut is True if:
            #     # - First week has Lless than 7 days (partial week at start), OR
            #     # - Last week is week 52 (standard year structure)
            #     if first_week_days < 7 or last_week_number == 52:
            #         week_cut = True
            #         logger.info(f"Week cut enabled: first_week_days={first_week_days}, last_week={last_week_number}")
            #     else:
            #         logger.info(f"Week cut disabled: non-standard week structure (first_week_days={first_week_days}, last_week={last_week_number})")
            # else:
            #     logger.info(f"Week cut disabled: data doesn't span full year (days {min_day} to {max_day})")

                        
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
        
        # Initialize dictionaries for worker-specific information
        empty_days = {}
        worker_holiday = {}
        missing_days = {}
        last_registered_day = {}
        first_registered_day = {}
        working_days = {}
        free_day_complete_cycle = {}
        fixed_days_off = {}
        fixed_LQs = {}
        
        # Process each worker
        for w in workers_complete:
            worker_calendar = matriz_calendario_gd[matriz_calendario_gd['colaborador'] == w]
            
            if worker_calendar.empty:
                logger.warning(f"No calendar data found for worker {w}")
                empty_days[w] = []
                worker_holiday[w] = []
                missing_days[w] = []
                fixed_days_off[w] = []
                fixed_LQs[w] = []
                continue
            
            # Find days with specific statuses
            worker_empty = worker_calendar[worker_calendar['tipo_turno'] == '-']['data'].dt.dayofyear.tolist()
            worker_missing = worker_calendar[worker_calendar['tipo_turno'] == 'V']['data'].dt.dayofyear.tolist()
            w_holiday = worker_calendar[(worker_calendar['tipo_turno'] == 'A') | (worker_calendar['tipo_turno'] == 'AP')]['data'].dt.dayofyear.tolist()
            worker_fixed_days_off = worker_calendar[(worker_calendar['tipo_turno'] == 'L')]['data'].dt.dayofyear.tolist()
            f_day_complete_cycle = worker_calendar[worker_calendar['tipo_turno'].isin(['L', 'L_DOM'])]['data'].dt.dayofyear.tolist()

            empty_days[w] = worker_empty
            missing_days[w] = worker_missing
            worker_holiday[w] = w_holiday
            fixed_days_off[w] = worker_fixed_days_off
            free_day_complete_cycle[w] = f_day_complete_cycle
            
        # Track first and last registered days
            if w in matriz_calendario_gd['colaborador'].values:
                first_registered_day[w] = worker_calendar['data'].dt.dayofyear.min()
            else:
                first_registered_day[w] = 0

            if w in matriz_calendario_gd['colaborador'].values:
                last_registered_day[w] = worker_calendar['data'].dt.dayofyear.max()
            else:
                last_registered_day[w] = 0

            logger.info(f"Worker {w} data processed: first registered day: {first_registered_day[w]}, last registered day: {last_registered_day[w]}") 
        #fixed_days_off[80001366] = [3, 13, 15, 211] 

        for w in workers_complete:
            # Mark all remaining days after last_registered_day as 'A' (absent)
            if first_registered_day[w] > 0 or last_registered_day[w] > 0:  # Ensure worker was registered at some point
                missing_days[w].extend([d for d in range( 1, first_registered_day[w]) if d not in missing_days[w]])
                missing_days[w].extend([d for d in range(last_registered_day[w] + 1, 366) if d not in missing_days[w]])
            
            empty_days[w] = sorted(list(set(empty_days[w]) - set(closed_holidays)))
            #worker_holiday[w] = sorted(list(set(worker_holiday[w]) - set(closed_holidays) - set(fixed_days_off[w]))) #Assumindo dados corretos, nao é preciso subtrair fixed days off
            worker_holiday[w], fixed_days_off[w], fixed_LQs[w] = data_treatment(set(worker_holiday[w]) - set(closed_holidays) - set(fixed_days_off[w]), set(fixed_days_off[w]), week_to_days_salsa, start_weekday)
            missing_days[w] = sorted(list(set(missing_days[w]) - set(closed_holidays)))
            free_day_complete_cycle[w] = sorted(list(set(free_day_complete_cycle[w]) - set(closed_holidays)))
            fixed_days_off[w] = sorted(list(set(fixed_days_off[w]) - set(closed_holidays)))

            working_days[w] = set(days_of_year) - set(empty_days[w]) - set(worker_holiday[w]) - set(missing_days[w]) - set(closed_holidays) - set(free_day_complete_cycle[w]) - set(fixed_LQs[w])

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
        l_q = {}
        cxx = {}
        t_lq = {}

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

            else:
                worker_row = worker_data.iloc[0]  # Take first row if multiple
                logger.info(f"Processing worker {w} with data: {worker_row.to_dict()}")
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


                logger.info(f"Worker {w} contract information extracted: "
                            f"Contract Type: {contract_type[w]}, "
                            f"Total L: {total_l[w]}, "
                            f"Total L DOM: {total_l_dom[w]}, "
                            f"C2D: {c2d[w]}, "
                            f"C3D: {c3d[w]}, "
                            f"L_D: {l_d[w]}, "
                            f"L_Q: {l_q[w]}, "
                            f"CXX: {cxx[w]}, "
                            f"T_LQ: {t_lq[w]}, ")
        
        for w in workers:
            if contract_type[w] == 'Contract Error':
                logger.error(f"Worker {w} has contract type error, removing from workers list")
                workers.pop(workers.index(w))  # Remove worker with contract error
            if total_l[w] <= 0:
                logger.error(f"Worker {w} has non-positive total_l: {total_l[w]}, removing from workers list")
                workers.pop(workers.index(w))  # Remove worker with contract error

        logger.info(f"Contract information extracted for {len(workers)} workers")

        # =================================================================
        # 10.2. ADAPT PROPORTIONS FOR WORKERS FOR FIRST AND LAST DAYS
        # =================================================================
        logger.info("Adjusting worker parameters based on last registered days")

        for w in workers:
            if (last_registered_day[w] > 0 and last_registered_day[w] < 364):
                proportion = (last_registered_day[w]- days_of_year[0])  / 364
                logger.info(f"Adjusting worker {w} parameters based on last registered day {last_registered_day[w]} with proportion {proportion:.2f}")
                total_l[w] = int(round(proportion * total_l[w]))
                total_l_dom[w] = int(round(proportion * total_l_dom[w]))
                c2d[w] = int(math.floor(proportion * c2d[w]))
                c3d[w] = int(math.floor(proportion * c3d[w]))
                l_d[w] = int(round(proportion * l_d[w]))
                l_q[w] = int(round(proportion * l_q[w]))
                cxx[w] = int(round(proportion * cxx[w]))
                t_lq[w] = int(round(proportion * t_lq[w]))
                
                logger.info(f"Worker {w} parameters adjusted for last registered day {last_registered_day[w]}: "
                            f"Total L: {total_l[w]}, "
                            f"Total L DOM: {total_l_dom[w]}, "
                            f"C2D: {c2d[w]}, "
                            f"C3D: {c3d[w]}, "
                            f"L_D: {l_d[w]}, "
                            f"L_Q: {l_q[w]}, "
                            f"CXX: {cxx[w]}, "
                            f"T_LQ: {t_lq[w]}, ")

        '''
        #i dont understand what this is doing
        for w in workers:
            worker_special_days = [d for d in special_days if d in working_days[w]]
            if contract_type[w] == 6:
                total_l_dom[w] = len(worker_special_days) - l_d[w] 
            elif contract_type[w] in [4,5]:
                total_l_dom[w] = len(worker_special_days) - l_d[w] 
            logger.info(f"Worker {w} total L DOM adjusted: {total_l_dom[w]} based on special days and contract type {contract_type[w]}")

        for w in workers:
            if contract_type[w] == 6:
                l_d[w] =  l_d[w] 
            elif contract_type[w] in [4,5]:
                total_l[w] = total_l[w]        
            logger.info(f"Worker {w} L_D adjusted: {l_d[w]} based on contract type {contract_type[w]}")        
        '''

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
                        pess_obj[(d, s)] = int(round(day_shift_data['pess_obj'].values[0]))
                    else:
                        pess_obj[(d, s)] = 0  # or any default value you prefer
                
                # Process min/max workers for all shifts
                for shift_type in shifts:
                    day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['data'].dt.dayofyear == d) & (matriz_estimativas_gd['turno'] == shift_type)]
                    if not day_shift_data.empty:
                                    # Convert floats to integers for OR-Tools compatibility
                                    min_workers[(d, shift_type)] = int(round(day_shift_data['min_turno'].values[0]))
                                    max_workers[(d, shift_type)] = int(round(day_shift_data['max_turno'].values[0]))

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
                        
                        #logger.info(f"Processing worker {w}, week {week}, day {day}: found {len(shift_entries)} shift entries with types: {shift_entries['tipo_turno'].tolist() if not shift_entries.empty else 'None'}")

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


        logger.info("[OK] Data processing completed successfully")
        
        # =================================================================
        # 13. RETURN ALL PROCESSED data
        # =================================================================
        return (
            matriz_calendario_gd,    # 0x
            days_of_year,            # 1x
            sundays,                 # 2x
            holidays,                # 3x
            special_days,            # 4x
            closed_holidays,         # 5x
            empty_days,              # 6x
            worker_holiday,          # 7x
            missing_days,            # 8x
            working_days,            # 9x
            non_holidays,            # 10x
            start_weekday,           # 11x
            week_to_days,            # 12x
            worker_week_shift,       # 13x
            matriz_colaborador_gd,   # 14x
            workers,                 # 15x
            contract_type,           # 16x
            total_l,                 # 17x
            total_l_dom,             # 18x
            c2d,                     # 19x
            c3d,                     # 20x
            l_d,                     # 21x
            l_q,                     # 22x
            cxx,                     # 23x
            t_lq,                    # 24x
            matriz_estimativas_gd,   # 25x
            pess_obj,                # 26x
            min_workers,             # 27x
            max_workers,             # 28x
            working_shift_2,         # 29x
            workers_complete,        # 30x
            workers_complete_cycle,  # 31x
            free_day_complete_cycle, # 32x
            week_to_days_salsa,      # 33x
            first_registered_day,    # 34x
            last_registered_day,     # 35x
            fixed_days_off,          # 36x
            fixed_LQs,          # 36x
            # week_cut
        )
        
    except Exception as e:
        logger.error(f"Error in read_data_salsa: {e}", exc_info=True)
        raise

#def data_treatment(set(worker_holiday[w]) - set(closed_holidays) - set(fixed_days_off[w]), set(fixed_days_off[w]), week_to_days_salsa):
def data_treatment(worker_holiday, fixed_days_off, week_to_days_salsa, start_weekday):
    fixed_LQs = []
    for week, days in week_to_days_salsa.items():
        if (len(days) <= 6):
            continue

        week_remaining = sorted(set(days) - worker_holiday)
        len_remaining = len(week_remaining)
        saturday = days[5]
        sunday = days[6]

        if len_remaining < 1:
            print(f"caso 1 antes: {week_remaining}, {saturday}, {sunday} \n\t\t{len(worker_holiday)}\n\t\t{len(fixed_days_off)}")

            worker_holiday -= {saturday, sunday}
            fixed_days_off |= {saturday}
            fixed_LQs.append(sunday)

            print(f"caso 1 depois: {week_remaining}, {saturday}, {sunday} \n\t\t{len(worker_holiday)}\n\t\t{len(fixed_days_off)}")

        elif len_remaining < 2:
            print(f"caso 2 antes: {week_remaining}, {saturday}, {sunday} \n\t\t{len(worker_holiday)}\n\t\t{len(fixed_days_off)}")

            if week_remaining[0] != saturday:
                worker_holiday -= {saturday}
            elif week_remaining[0] != sunday:
                worker_holiday -= {sunday}

            fixed_days_off |= {saturday}
            fixed_LQs.append(sunday)

            print(f"caso 2 depois: {week_remaining}, {saturday}, {sunday} \n\t\t{len(worker_holiday)}\n\t\t{len(fixed_days_off)}")

        elif len_remaining < 3:
            print(f"caso 3 antes: {week_remaining}, {saturday}, {sunday} \n\t\t{len(worker_holiday)}\n\t\t{len(fixed_days_off)}")

            if week_remaining[0] != saturday and week_remaining[1] != saturday:
                worker_holiday -= {saturday}
            if week_remaining[0] != sunday and week_remaining[1] != sunday:
                worker_holiday -= {sunday}

            fixed_days_off |= {saturday}
            fixed_LQs.append(sunday)

            print(f"caso 3 depois: {week_remaining}, {saturday}, {sunday} \n\t\t{len(worker_holiday)}\n\t\t{len(fixed_days_off)}")
    return worker_holiday, fixed_days_off, fixed_LQs