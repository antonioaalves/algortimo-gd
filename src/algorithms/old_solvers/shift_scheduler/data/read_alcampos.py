import pandas as pd
import math

def read_data(matriz_calendario, matriz_estimativas, matriz_colaborador):

    df = pd.read_csv(matriz_estimativas, index_col=0, sep = ',')

    cal = pd.read_csv(matriz_calendario, index_col=0, sep = ',')
    cal = cal[cal["COLABORADOR"] != "TIPO_DIA"]

    colaboradores = pd.read_csv(matriz_colaborador, sep=None,  engine='python')

    colaboradores["L_Q"] = colaboradores["L_TOTAL"] - colaboradores["L_DOM"] - colaboradores["C2D"] - colaboradores["C3D"] - colaboradores["L_D"] - colaboradores["CXX"] - colaboradores["VZ"] - colaboradores["L_RES"] - colaboradores["L_RES2"]

    # Ensure COLABORADOR in cal is numeric
    cal['COLABORADOR'] = pd.to_numeric(cal['COLABORADOR'], errors='coerce')

    workers = colaboradores['MATRICULA'].dropna().unique().astype(int).tolist()
    #workers = colaboradores[colaboradores['MATRICULA'] != 5037932]['MATRICULA'].dropna().unique().astype(int).tolist()



    # Convert 'DATA' column to datetime and extract days of the year
    cal['DATA'] = pd.to_datetime(cal['DATA'])
    days_of_year = cal['DATA'].dt.dayofyear.unique().tolist()


    # Define shifts and special days
    shifts = ["M", "T", "L","LQ", "F", "V","LD", "A"]
    sundays = cal[cal['WD'] == 'Sun']['DATA'].dt.dayofyear.unique().tolist()
    holidays = cal[(cal['WD'] != 'Sun') & (cal["DIA_TIPO"] == "domYf")]['DATA'].dt.dayofyear.unique().tolist()

    closed_holidays = cal[(cal['TIPO_TURNO'] == "F")]['DATA'].dt.dayofyear.unique().tolist()


    # Initialize dictionaries to store days with '-' and 'V' for each worker
    empty_days = {}
    worker_holiday = {}
    missing_days = {}
    last_registered_day = {}
    first_registered_day = {}

    # Iterate over each worker and find their last registered working day
# Iterate over each worker and find their last registered working day
    for w in workers:
        if w in cal['COLABORADOR'].values:
            last_registered_day[w] = cal[cal['COLABORADOR'] == w]['DATA'].dt.dayofyear.max()
        else:
            last_registered_day[w] = 0  # If worker is not in cal, assume they never worked

     # Iterate over each worker and find their last registered working day
    for w in workers:
        if w in cal['COLABORADOR'].values:
            first_registered_day[w] = cal[cal['COLABORADOR'] == w]['DATA'].dt.dayofyear.min()
        else:
            first_registered_day[w] = 0  # If worker is not in cal, assume they never worked

    # Iterate over each worker
    for w in workers:
        # Get the days with '-' for the current worker
        empty_days[w] = cal[(cal['COLABORADOR'] == w) & (cal['TIPO_TURNO'] == '-')]['DATA'].dt.dayofyear.unique().tolist()
        
        # Get the days with 'V' for the current worker
        missing_days[w] = cal[(cal['COLABORADOR'] == w) & (cal['TIPO_TURNO'] == 'V')]['DATA'].dt.dayofyear.unique().tolist()

        # Get the days with 'V' for the current worker
        worker_holiday[w] = cal[(cal['COLABORADOR'] == w) & (cal['TIPO_TURNO'] == 'A')]['DATA'].dt.dayofyear.unique().tolist()

        # Mark all remaining days after last_registered_day as 'A' (absent)
        if first_registered_day[w] > 0 or last_registered_day[w] > 0:  # Ensure worker was registered at some point
            missing_days[w].extend([d for d in range( 1, first_registered_day[w]) if d not in missing_days[w]])
            missing_days[w].extend([d for d in range(last_registered_day[w] + 1, 366) if d not in missing_days[w]])
        



    for w in workers:
        empty_days[w] = list(set(empty_days[w]) - set(closed_holidays))
        worker_holiday[w] = list(set(worker_holiday[w]) - set(closed_holidays))
        missing_days[w] = list(set(missing_days[w]) - set(closed_holidays))


    working_days = {}
    for w in workers:
        working_days[w] = set(days_of_year) - set(empty_days[w]) - set(worker_holiday[w]) - set(missing_days[w]) - set(closed_holidays)

    # Convert 'DATA' column in df to datetime
    df['DATA'] = pd.to_datetime(df['DATA'])

    #Contract Type dictionary
    contract_type = {}
    for w in workers:
        contract_type[w] = colaboradores[colaboradores['MATRICULA'] == w]['TIPO_CONTRATO'].values[0]

    #Total free days dictionary
    total_l = {}
    for w in workers:
        total_l[w] = colaboradores[colaboradores['MATRICULA'] == w]['L_TOTAL'].values[0]


    #Total free days in sundays and holidays
    total_l_dom = {}
    for w in workers:
        total_l_dom[w] = colaboradores[colaboradores['MATRICULA'] == w]['L_DOM'].values[0]

    #Total Good Quality Weekends (free saturday and sunday)
    c2d= {}
    for w in workers:
        c2d[w] = colaboradores[colaboradores['MATRICULA'] == w]['C2D'].values[0]

    #Total Good Quality 3 day Weekends (free saturday and sunday and either friday or monday)
    c3d= {}
    for w in workers:
        c3d[w] = colaboradores[colaboradores['MATRICULA'] == w]['C3D'].values[0]

    #Free days of compensasion for working in holidays and sundays
    l_d= {}
    for w in workers:
        l_d[w] = colaboradores[colaboradores['MATRICULA'] == w]['L_D'].values[0]

    #Free days of compensasion for working in holidays and sundays
    l_q= {}
    for w in workers:
        l_q[w] = colaboradores[colaboradores['MATRICULA'] == w]['L_Q'].values[0]

    #Free days of compensasion for working in holidays and sundays
    cxx= {}
    for w in workers:
        cxx[w] = colaboradores[colaboradores['MATRICULA'] == w]['CXX'].values[0]

    #True LQ
    t_lq= {}
    for w in workers:
        t_lq[w] = colaboradores[colaboradores['MATRICULA'] == w]['L_Q'].values[0] + colaboradores[colaboradores['MATRICULA'] == w]['C2D'].values[0] + colaboradores[colaboradores['MATRICULA'] == w]['C3D'].values[0]


    #True LQ
    tc = {}
    for w in workers:
        tc[w] = colaboradores[colaboradores['MATRICULA'] == w]['DOFHC'].values[0]

    special_days = sorted(list(set(sundays + holidays)))

        # Adjust the values based on last registered day for workers who haven't worked the full year
    for w in workers:
        if (last_registered_day[w] > 0 and last_registered_day[w] < 365):
            proportion = last_registered_day[w]  / 365
            total_l[w] = round(proportion * total_l[w])
            total_l_dom[w] = round(proportion * total_l_dom[w])
            c2d[w] = math.floor(proportion * c2d[w])
            c3d[w] = math.floor(proportion * c3d[w])
            l_d[w] = round(proportion * l_d[w])
            l_q[w] = round(proportion * l_q[w])
            cxx[w] = round(proportion * cxx[w])
            t_lq[w] = round(proportion * t_lq[w])
            tc[w] = round(proportion * tc[w])


    for w in workers:
        worker_special_days = [d for d in special_days if d in working_days[w]]
        if contract_type[w] == 6:
            total_l_dom[w] = len(worker_special_days) - l_d[w] - tc[w]
        elif contract_type[w] in [4,5]:
            total_l_dom[w] = len(worker_special_days) - l_d[w] - tc[w]

    for w in workers:
        if contract_type[w] == 6:
            l_d[w] =  l_d[w] + tc[w] 
        elif contract_type[w] in [4,5]:
            total_l[w] = total_l[w] - tc[w]
            

    working_shift = ["M", "T", "TC"]

    pessObj = {}
    for d in days_of_year:
        for s in working_shift:
            day_shift_data = df[(df['DATA'].dt.dayofyear == d) & (df['TURNO'] == s)]
            if not day_shift_data.empty:
                pessObj[(d, s)] = day_shift_data['pessObj'].values[0]
            else:
                pessObj[(d, s)] = 0  # or any default value you prefer




    # Initialize dictionaries to store min and max workers for each day and shift
    min_workers = {}
    max_workers = {}

    # Loop through each day and shift to populate the min_workers and max_workers dictionaries
    for day in days_of_year:
        for shift_type in shifts:
            day_shift_data = df[(df['DATA'].dt.dayofyear == day) & (df['TURNO'] == shift_type)]
            if not day_shift_data.empty:
                min_workers[(day, shift_type)] = day_shift_data['minTurno'].values[0]
                max_workers[(day, shift_type)] = day_shift_data['maxTurno'].values[0]

    # Check the days of the year that are not holidays
    non_holidays = [d for d in days_of_year if d not in closed_holidays]

    # Get the weekday of the first day of the year
    start_weekday = df["WDAY"].iloc[0]  # e.g., 4 for Wednesday

    week_to_days = {}
    for d in non_holidays:
        # Compute the correct week number
        week = (d + (5 - start_weekday)) // 7 + 1
        if week not in week_to_days:
            week_to_days[week] = []
        week_to_days[week].append(d)

    

    # Create binary variables to indicate if a worker is assigned to any shift in a given week
    worker_week_shift = {}


    # Iterate over each worker
    for w in workers:
        for week in range(1, 53):  # Iterate over the 52 weeks
            worker_week_shift[(w, week, 'M')] = 0
            worker_week_shift[(w, week, 'T')] = 0
            
            # Iterate through days of the week for the current week
            for day in week_to_days[week]:
                if day in non_holidays:  # Make sure we're only checking non-holiday days
                    
                    # Get the rows for the current week and day
                    shift_entries = cal[(cal['DATA'].dt.isocalendar().week == week) & (cal['DATA'].dt.day_of_year == day) & (cal['COLABORADOR'] == w)]
                    
                    # Check for morning shifts ('M') for the current worker
                    if not shift_entries[shift_entries['TIPO_TURNO'] == "M"].empty:
                        # Assign morning shift to the worker for that week
                        worker_week_shift[(w, week, 'M')] = 1  # Set to 1 if morning shift is found

                    # Check for afternoon shifts ('T') for the current worker
                    if not shift_entries[shift_entries['TIPO_TURNO'] == "T"].empty:
                        # Assign afternoon shift to the worker for that week
                        worker_week_shift[(w, week, 'T')] = 1  # Set to 1 if afternoon shift is found
    
    working_shift_2 = ["M", "T"]

    return cal, days_of_year, sundays, holidays, special_days, closed_holidays, empty_days, worker_holiday, missing_days, working_days , non_holidays, start_weekday, week_to_days, worker_week_shift, \
           colaboradores, workers, contract_type, total_l, total_l_dom, c2d, c3d, l_d, l_q, cxx, t_lq, tc, \
           df, pessObj, min_workers, max_workers, working_shift_2



