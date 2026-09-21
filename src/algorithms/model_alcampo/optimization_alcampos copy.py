from src.configuration_manager.instance import get_config
from base_data_project.log_config import get_logger
import math
import numpy as np

_config_manager = get_config()
logger = get_logger(_config_manager.project_name)



def optimization_prediction(model,days_of_year, workers, workers_complete_cycle, shift, pessObj, min_workers, closed_holidays, week_to_days,  working_days, contract_type, special_days,
                            workers_past, real_working_shift, out_workers, year_range):
    scale = 10000
    objective_terms = []
    days_of_year_real = [d for d in days_of_year if year_range[0] <= d <= year_range[1] and d not in closed_holidays]
    days_of_year_working = [d for d in days_of_year if d not in closed_holidays]
    sundays = [d for d in sundays if d not in closed_holidays]
    saturdays = [d - 1 for d in sundays if d - 1 not in closed_holidays]
    workers_not_complete = [w for w in workers if w not in workers_complete_cycle]
    if len(workers_not_complete) < 1:
        workers_not_complete_exist = False
    else:
        workers_not_complete_exist = True
    n = int(len(days_of_year_real) / 6)

    q_groups = {i: [] for i in range(6)}

    if workers_not_complete_exist:
        for w in workers_not_complete:
            days_worked = len(working_days[w])

            if days_worked >= n * 5:
                q_groups[0].append(w)
            elif days_worked >= n * 4:
                q_groups[1].append(w)
            elif days_worked >= n * 3:
                q_groups[2].append(w)
            elif days_worked >= n * 2:
                q_groups[3].append(w)
            elif days_worked >= n * 1:
                q_groups[4].append(w)
            else:
                q_groups[5].append(w)

    # Weights:
    sum_pess_year = sum(pessObj.get((d, s), 0) for d in days_of_year_working for s in real_working_shift)
    excess_min_worst_scenario = (3 / 5) * sum_pess_year
    if excess_min_worst_scenario < 1:
        logger.error(f" Estimativas a virem vazias ao longo do ano todo! {excess_min_worst_scenario}")
        excess_min_worst_scenario = 0.1
    percentage_of_importance_no_excess = 0 
    excess_weight = int(scale * percentage_of_importance_no_excess / excess_min_worst_scenario)

    deficit_min_worst_scenario = (1 / 8) * sum_pess_year
    if deficit_min_worst_scenario < 1:
        logger.error(f" Estimativas a virem vazias ao longo do ano todo! {deficit_min_worst_scenario}")
        deficit_min_worst_scenario = 0.1
    percentage_of_importance_no_deficit = 3
    deficit_weight = int(scale * percentage_of_importance_no_deficit / deficit_min_worst_scenario)

    no_workers_min_worst_scenario = 1
    percentage_of_importance_workers = 3 
    no_workers_weight = int(scale * percentage_of_importance_workers / no_workers_min_worst_scenario)

    sundays_diff_min_worst_scenario = 2
    percentage_of_importance_sundays_equal = 1 
    sundays_diff_weight = int(scale * percentage_of_importance_sundays_equal / sundays_diff_min_worst_scenario)

    LQs_diff_min_worst_scenario = 1
    percentage_of_importance_LQs_equal = 1 
    LQs_diff_weight = int(scale * percentage_of_importance_LQs_equal / LQs_diff_min_worst_scenario)

    sunday_imbalance_weight_periodicity_min_worst_scenario = 1
    percentage_of_importance_sunday_imbalance_weight_periodicity = 3 
    sunday_imbalance_weight_periodicity = int(scale * percentage_of_importance_sunday_imbalance_weight_periodicity / sunday_imbalance_weight_periodicity_min_worst_scenario)

    LQ_imbalance_per_semeste_min_worst_scenario = 1
    percentage_of_importance_LQ_balance = 1
    LQ_imbalance_weight = int(scale * percentage_of_importance_LQ_balance / LQ_imbalance_per_semeste_min_worst_scenario)
    if workers_not_complete_exist:
        LQ_imbalance_weight_average = int(math.ceil(LQ_imbalance_weight / len(workers_not_complete)))
    else:
        LQ_imbalance_weight_average = int(LQ_imbalance_weight)

    weekly_diff_min_worst_scenario = 52 * 4 #number of weeks*hours
    percentage_of_importance_weekly_diff = 1 
    weekly_diff_weight = int(scale * percentage_of_importance_weekly_diff/weekly_diff_min_worst_scenario)
    weekly_diff_min_worst_scenario_per_day = 52 * 4 #number of weeks*hours
    percentage_of_importance_weekly_diff_per_day = 1
    weekly_diff_weight_per_day = int(scale * percentage_of_importance_weekly_diff_per_day / weekly_diff_min_worst_scenario_per_day)

    excess_and_deficit_worst_scenario = 8
    percentage_of_importance_excess_and_deficit = 2 
    excess_and_deficit_weight = int(scale * percentage_of_importance_excess_and_deficit / excess_and_deficit_worst_scenario)
    
    number_free_days_exceeded_worst_case_scenario = 5 
    percentage_of_total_exceeded_days_weight = 2
    if len(workers) >= 6:
        free_days_per_day_worst_case_scenario = len(workers) // 3  
    if len(workers) < 6:
        free_days_per_day_worst_case_scenario = 2 
    total_exceeded_days_weight = int(scale * percentage_of_total_exceeded_days_weight / number_free_days_exceeded_worst_case_scenario)
    
    number_free_sundays_exceeded_worst_case_scenario = 1 
    if len(workers) >= 6:
        free_days_per_sunday_worst_case_scenario= len(workers) // 3
        percentage_of_total_exceeded_sundays_weight = 2  
    if len(workers) < 6:
        percentage_of_total_exceeded_sundays_weight = 2
        free_days_per_sunday_worst_case_scenario = 2 
    total_exceeded_sundays_weight = int(scale * percentage_of_total_exceeded_sundays_weight / number_free_sundays_exceeded_worst_case_scenario)
    

    day_deficit_hours_worst_case_scenario = 8 #in a shift
    percentage_of_importance_day_deficit = 2 
    max_deficit_weight = int(scale * percentage_of_importance_day_deficit / day_deficit_hours_worst_case_scenario) 

    deficit_over_x_worst_case = 8 #value of x in hours
    percentage_of_importance_day_deficit_over_x = 1 
    num_days_deficit_over_x_worst_case = 100
    deficit_over_x_day_weight = int(scale * percentage_of_importance_day_deficit_over_x / num_days_deficit_over_x_worst_case)
    
    deficit_over_y_worst_case = 12 #value of y in hours
    percentage_of_importance_day_deficit_over_y = 1
    num_days_deficit_over_y_worst_case = 50
    deficit_over_y_day_weight = int(scale * percentage_of_importance_day_deficit_over_y / num_days_deficit_over_y_worst_case)

    deficit_over_z_worst_case = 16 #value of z in hours
    percentage_of_importance_day_deficit_over_z = 1 
    num_days_deficit_over_z_worst_case = 15
    deficit_over_z_day_weight = int(scale * percentage_of_importance_day_deficit_over_z / num_days_deficit_over_z_worst_case)

    deficit_over_t_worst_case = 20 #value of t in hours
    percentage_of_importance_day_deficit_over_t = 1 
    num_days_deficit_over_t_worst_case = 5 
    deficit_over_t_day_weight = int(scale * percentage_of_importance_day_deficit_over_t / num_days_deficit_over_t_worst_case)

    deficit_over_k_worst_case = 24 #value of k in hours
    percentage_of_importance_day_deficit_over_k = 1
    num_days_deficit_over_k_worst_case = 2
    deficit_over_k_day_weight = int(scale * percentage_of_importance_day_deficit_over_k / num_days_deficit_over_k_worst_case)

    deficit_over_q_worst_case = 32 #value of q in hours
    percentage_of_importance_day_deficit_over_q = 1 
    num_days_deficit_over_q_worst_case = 1
    deficit_over_q_day_weight = int(scale * percentage_of_importance_day_deficit_over_q / num_days_deficit_over_q_worst_case)

    out_worst_scenario = 1
    percentage_of_importance_out = 1 
    out_weight = int(scale * percentage_of_importance_out / out_worst_scenario)

    all_workers = workers + workers_past

    tc_to_shift     = {}
    effective_shift = {}
    total_TC        = {}
    for d in special_days:
        total_TC[d] = sum(shift.get((w, d, "TC"), 0) for w in all_workers)
        tc_to_shift[d] = {}
        for s in real_working_shift:
            tc_to_shift[d][f"shift_{s}"] = model.NewIntVar(0, len(all_workers),f"tc_to_{s}_{d}")
        model.Add(sum([tc_to_shift[d][f"shift_{s}"] for s in real_working_shift]) == total_TC[d])
    
    # ===============================
    # 1.1 Total excess and deficit 
    # ===============================

    excess_diff_vars  = []
    deficit_diff_vars = []
    for d in days_of_year:
        for s in real_working_shift:
            target = pessObj.get((d, s), 0)
            assigned_workers = sum(shift[(w, d, s)] * 10 for w in all_workers if (w, d, s) in shift)
            assigned_workers += sum(shift[(w, d, 'MoT')] * 5 for w in all_workers if (w, d, 'Mot') in shift)

            if d in special_days:
                effective_shift = tc_to_shift[d][f"shift_{s}"] + assigned_workers
            else:
                effective_shift = assigned_workers
            excess  = model.NewIntVar(0, len(all_workers) * 10, f'excess_{d}_{s}')
            deficit = model.NewIntVar(0, target * 10, f'deficit_{d}_{s}')

            model.Add(excess >= effective_shift - target)
            model.Add(deficit >= target - effective_shift)

            excess_diff_vars.append((d, s, excess))
            deficit_diff_vars.append((d, s, deficit))

    objective_terms.append(sum(excess for (d, _, excess) in excess_diff_vars if d in days_of_year_working) * excess_weight)
    objective_terms.append(sum(deficit for (d, _, deficit) in deficit_diff_vars if d in days_of_year_working) * deficit_weight)        

    # ===============================
    # 1.2. Max deficit across all shifts
    # ===============================

    daily_deficit              = {}
    daily_excess               = {}
    max_daily_deficit_possible = len(real_working_shift) * max(pessObj.values()) * 80
    max_daily_excess_possible  = len(real_working_shift) * len(all_workers) * 80
    for d in days_of_year_working:
        daily_deficit[d] = model.NewIntVar(0, max_daily_deficit_possible, f'daily_deficit_{d}')
        daily_excess[d]  = model.NewIntVar(0, max_daily_excess_possible,  f'daily_excess_{d}')

        model.Add(daily_deficit[d] == sum(deficit for (dd, s, deficit) in deficit_diff_vars if dd == d))
        model.Add(daily_excess[d]  == sum(excess for (dd, s, excess) in excess_diff_vars if dd == d))

    # ===============================
    # 1.3. Max deficit across all shifts
    # ===============================

    max_deficit = model.NewIntVar(0, max_daily_deficit_possible, 'max_deficit')
    for (d, _, deficit) in deficit_diff_vars:
        if d not in days_of_year_working:
           continue
        else:
            model.Add(max_deficit >= deficit)
    objective_terms.append(max_deficit * max_deficit_weight)

    # ===============================
    # 1.4. Exces and deficit at the same day
    # ===============================

    day_has_excess  = {}
    day_has_deficit = {}
    day_has_both    = {}
    penalty_vars    = []

    for d in days_of_year_working:
        daily_deficit[d] = model.NewIntVar(0, max_daily_deficit_possible, f'daily_deficit_{d}')
        daily_excess[d] = model.NewIntVar(0, max_daily_excess_possible,  f'daily_excess_{d}')

        model.Add(daily_deficit[d] == sum(deficit for (dd, s, deficit) in deficit_diff_vars if dd == d))
        model.Add(daily_excess[d] == sum(excess for (dd, s, excess) in excess_diff_vars if dd == d))

    for d in days_of_year_working:
        day_has_excess[d] = model.NewBoolVar(f'day_{d}_has_excess')
        day_has_deficit[d] = model.NewBoolVar(f'day_{d}_has_deficit')
        day_has_both[d] = model.NewBoolVar(f'day_{d}_has_both')


        model.Add(daily_excess[d] >= 1).OnlyEnforceIf(day_has_excess[d])
        model.Add(daily_excess[d] == 0).OnlyEnforceIf(day_has_excess[d].Not())

        model.Add(daily_deficit[d] >= 1).OnlyEnforceIf(day_has_deficit[d])
        model.Add(daily_deficit[d] == 0).OnlyEnforceIf(day_has_deficit[d].Not())

        model.AddBoolAnd([day_has_excess[d], day_has_deficit[d]]).OnlyEnforceIf(day_has_both[d])
        model.AddBoolOr([day_has_excess[d].Not(), day_has_deficit[d].Not()]).OnlyEnforceIf(day_has_both[d].Not())
        penalty_vars.append(day_has_both[d])
    objective_terms.append(sum(penalty_vars) * excess_and_deficit_weight)

    # ===============================
    # 1.5. Number of days with deficit over certain values
    # ===============================

    deficit_cases = {
        'x': (deficit_over_x_worst_case, deficit_over_x_day_weight),
        'y': (deficit_over_y_worst_case, deficit_over_y_day_weight),
        'z': (deficit_over_z_worst_case, deficit_over_z_day_weight),
        't': (deficit_over_t_worst_case, deficit_over_t_day_weight),
        'k': (deficit_over_k_worst_case, deficit_over_k_day_weight),
        'q': (deficit_over_q_worst_case, deficit_over_q_day_weight),
    }

    day_deficit_over = {c: {} for c in deficit_cases}

    for d in days_of_year_working:
        for c, (worst_case, _) in deficit_cases.items():

            b = model.NewBoolVar(f'day_{d}_deficit_over_{c}')
            day_deficit_over[c][d] = b

            model.Add(daily_deficit[d] >= worst_case + 1).OnlyEnforceIf(b)
            model.Add(daily_deficit[d] <= worst_case).OnlyEnforceIf(b.Not())

    for c, (_, weight) in deficit_cases.items():
        num_days = model.NewIntVar(0, len(days_of_year_working), f'num_days_deficit_over_{c}')
        model.Add(num_days == sum(day_deficit_over[c][d] for d in days_of_year_working))
        objective_terms.append(num_days * weight)

    # ===============================
    # 1.6. Weekly difference balancing
    # ===============================

    weekly_diff_vars         = []
    weekly_diff_vars_per_day = []
    sorted_weeks             = sorted(week_to_days.keys())
    safe_limit               = len(all_workers) * 10 * len(real_working_shift)

    for week in sorted_weeks:
        days = set(week_to_days[w])

        # collect excess and deficit variables for this week
        excess_vars = [ex for (d, s, ex) in excess_diff_vars if d in days and d not in closed_holidays]
        deficit_vars = [df for (d, s, df) in deficit_diff_vars if d in days and d not in closed_holidays]

        excess_vars_per_day = [sum(ex for (dd, s, ex) in excess_diff_vars if dd == d) for d in days if d not in closed_holidays]
        deficit_vars_per_day = [sum(df for (dd, s, df) in deficit_diff_vars if dd == d) for d in days if d not in closed_holidays]

        if not excess_vars and not deficit_vars:
            continue  # nothing to analyze this week

        # combine variables, taking negative for deficits
        if excess_vars and not deficit_vars:
            window_vars = excess_vars
        elif not excess_vars and deficit_vars:
            window_vars = [-df for df in deficit_vars]
        else:
            window_vars = excess_vars + [-df for df in deficit_vars]

        if excess_vars_per_day and not deficit_vars_per_day:
            window_vars_per_day = excess_vars_per_day
        elif not excess_vars_per_day and deficit_vars_per_day:
            window_vars_per_day = [-df for df in deficit_vars_per_day]
        else:
            window_vars_per_day = excess_vars_per_day + [-df for df in deficit_vars_per_day]

        # max and min of the week
        max_var = model.NewIntVar(-safe_limit, safe_limit, f'week_{week}_max')
        min_var = model.NewIntVar(-safe_limit, safe_limit, f'week_{week}_min')
        model.AddMaxEquality(max_var, window_vars)
        model.AddMinEquality(min_var, window_vars)

        # max and min of the week
        max_var_per_day = model.NewIntVar(-2 * safe_limit, 2 * safe_limit, f'week_{week}_max_per_day')
        min_var_per_day = model.NewIntVar(-2 * safe_limit, 2 * safe_limit, f'week_{week}_min_per_day')
        model.AddMaxEquality(max_var_per_day, window_vars_per_day)
        model.AddMinEquality(min_var_per_day, window_vars_per_day)

        # difference
        diff_var = model.NewIntVar(0, 2 * safe_limit, f'week_{week}_diff')
        model.Add(diff_var == max_var - min_var)

        # difference
        diff_var_per_day = model.NewIntVar(0, 4 * safe_limit, f'week_{week}_diff')
        model.Add(diff_var_per_day == max_var_per_day - min_var_per_day)

        weekly_diff_vars.append(diff_var)
        weekly_diff_vars_per_day.append(diff_var_per_day)

        if weekly_diff_vars:
            total_weekly_diff = model.NewIntVar(0, len(weekly_diff_vars) * 2 * safe_limit, 'total_biweekly_diff')
            model.Add(total_weekly_diff == sum(weekly_diff_vars))
            if percentage_of_importance_weekly_diff > 0:
                objective_terms.append(total_weekly_diff * weekly_diff_weight)

        if weekly_diff_vars_per_day:
            total_weekly_diff_per_day = model.NewIntVar(0, len(weekly_diff_vars_per_day) * 4 * safe_limit, 'total_weekly_diff_per_day')
            model.Add(total_weekly_diff_per_day == sum(weekly_diff_vars_per_day))
            if percentage_of_importance_weekly_diff_per_day > 0:
                objective_terms.append(total_weekly_diff_per_day * weekly_diff_weight_per_day)

    # ===============================
    # 2 No workers in a day
    # ===============================
    
    zero_assigned_vars = []
    for d in days_of_year_working:
        for s in real_working_shift:  
            target = pessObj.get((d, s), 0)
            assigned_workers = sum(shift[(w, d, s)] for w in all_workers if (w, d, s) in shift)
            assigned_workers += sum(shift[(w, d, 'MoT')] for w in all_workers if (w, d, 'Mot') in shift)
            if d in special_days:
                effective_shift = tc_to_shift[d][f"shift_{s}"] + assigned_workers
            else:
                effective_shift = assigned_workers
            if target > 0:
                zero_assigned = model.NewBoolVar(f'zero_assigned_{d}_{s}')
                model.Add(effective_shift == 0).OnlyEnforceIf(zero_assigned)
                model.Add(effective_shift >= 1).OnlyEnforceIf(zero_assigned.Not())
                zero_assigned_vars.append(zero_assigned)

    objective_zero = sum(zero_assigned_vars)
    if percentage_of_importance_workers > 0:
        objective_terms.append(objective_zero * no_workers_weight)

    # ===============================
    # 3.1 Balancing number of free sundays across the workers 
    # ===============================
    
    for qi, workers_q in q_groups.items():
    
        if len(workers_q) <= 1:
            continue  

        sundays_per_worker_q = []

        for w in workers_q:
            sunday_free = sum(shift[(w, d, 'L')] for d in sundays if (w, d, 'L') in shift and year_range[0] <= d <= year_range[1])
            sundays_per_worker_q.append(sunday_free)

        if not sundays_per_worker_q:
            continue    

        max_sundays_q = model.NewIntVar(0, len(sundays), f"max_sundays_q{qi}")
        min_sundays_q = model.NewIntVar(0, len(sundays), f"min_sundays_q{qi}")

        model.AddMaxEquality(max_sundays_q, sundays_per_worker_q)
        model.AddMinEquality(min_sundays_q, sundays_per_worker_q)

        sunday_diff_q = model.NewIntVar(0, len(sundays), f"sunday_diff_q{qi}")
        model.Add(sunday_diff_q == max_sundays_q - min_sundays_q)
        objective_terms.append(sunday_diff_q * sundays_diff_weight)

    # ===============================
    # 3.2 Balancing number of free saturdays across the workers 
    # ===============================
    
    for qi, workers_q in q_groups.items():
    
        if len(workers_q) <= 1:
            continue  

        saturdays_per_worker_q = []

        for w in workers_q:
            saturday_free = sum(shift[(w, d, s)] for d in saturdays for s in ['LQ', 'L'] if (w, d, s) in shift and year_range[0] <= d <= year_range[1])
            saturdays_per_worker_q.append(saturday_free)

        if not saturdays_per_worker_q:
            continue    

        max_saturdays_q = model.NewIntVar(0, len(saturdays), f"max_saturdays_q{qi}")
        min_saturdays_q = model.NewIntVar(0, len(saturdays), f"min_saturdays_q{qi}")

        model.AddMaxEquality(max_saturdays_q, saturdays_per_worker_q)
        model.AddMinEquality(min_saturdays_q, saturdays_per_worker_q)
        
        saturday_diff_q = model.NewIntVar(0, len(saturdays), f"saturday_diff_q{qi}")
        model.Add(saturday_diff_q == max_saturdays_q - min_saturdays_q)
        objective_terms.append(saturday_diff_q * sundays_diff_weight)

    # ===============================
    # 4. Balancing number of free LQ across the workers 
    # ===============================
    
    for qi, workers_q in q_groups.items():
    
        if len(workers_q) <= 1:
            continue  

        LQs_per_worker_q = []

        for w in workers_q:
            LQs = sum(shift[(w, d, 'LQ')] for d in saturdays if (w, d, 'LQ') in shift and year_range[0] < d <= year_range[1])
            LQs_per_worker_q.append(LQs)
        if not LQs_per_worker_q:
            continue    

        max_LQs_q = model.NewIntVar(0, len(saturdays), f"max_LQs_q{qi}")
        min_LQs_q = model.NewIntVar(0, len(saturdays), f"min_LQs_q{qi}")

        model.AddMaxEquality(max_LQs_q, LQs_per_worker_q)
        model.AddMinEquality(min_LQs_q, LQs_per_worker_q)

        LQs_diff_q = model.NewIntVar(0, len(saturdays), f"LQs_diff_q{qi}")
        model.Add(LQs_diff_q == max_LQs_q - min_LQs_q)
        objective_terms.append(LQs_diff_q * LQs_diff_weight)

    # ===============================
    # 5. Try not to assign too many free days on the same day with deficit. 
    # ===============================

    is_free_dict        = {} 
    exceeded            = {}
    limit               = free_days_per_day_worst_case_scenario
    total_exceeded_days = model.NewIntVar(0, len(days_of_year_working), "total_exceeded_days")

    for w in workers:
        for d in working_days[w]:
            is_free = model.NewBoolVar(f"is_free_{w}_{d}")
            free_terms = []

            if (w, d, 'L') in shift:
                free_terms.append(shift[(w, d, 'L')])
            if (w, d, 'LQ') in shift:
                free_terms.append(shift[(w, d, 'LQ')])
            if free_terms:
                model.AddMaxEquality(is_free, free_terms)
                is_free_dict[(w, d)] = is_free

    for d in days_of_year_working:

        free_count = model.NewIntVar(0, len(all_workers), f"free_count_day_{d}")
        free_vars_today = [is_free_dict[(w, d)] for w in all_workers if (w, d) in is_free_dict]
        model.Add(free_count == sum(free_vars_today))
        
        free_exceeded = model.NewBoolVar(f"free_exceeded_day_{d}")
        model.Add(free_count >= limit + 1).OnlyEnforceIf(free_exceeded)
        model.Add(free_count <= limit).OnlyEnforceIf(free_exceeded.Not())
        
        deficit_positive = model.NewBoolVar(f"deficit_positive_{d}")
        model.Add(daily_deficit[d] >= 1).OnlyEnforceIf(deficit_positive)
        model.Add(daily_deficit[d] <= 0).OnlyEnforceIf(deficit_positive.Not())

        exceeded[d] = model.NewBoolVar(f"exceeded_day_{d}")
        model.AddBoolAnd([free_exceeded, deficit_positive]).OnlyEnforceIf(exceeded[d])
        model.AddBoolOr([free_exceeded.Not(), deficit_positive.Not()]).OnlyEnforceIf(exceeded[d].Not())
        
    model.Add(total_exceeded_days == sum(exceeded[d] for d in days_of_year_working))
    
    if total_exceeded_days_weight > 0:
        objective_terms.append(total_exceeded_days * total_exceeded_days_weight)

    # ===============================
    # 6. Try not to assign too many free days on Sundays with deficit
    # ===============================

    exceeded               = {}
    limit                  = free_days_per_sunday_worst_case_scenario
    total_exceeded_sundays = model.NewIntVar(0, len(sundays), "total_exceeded_sundays")

    for d in sundays:

        free_count = model.NewIntVar(0, len(all_workers), f"free_count_sunday_{d}")
        free_vars_today = [is_free_dict[(w, d)] for w in all_workers if (w, d) in is_free_dict]
        model.Add(free_count == sum(free_vars_today))
        
        free_exceeded = model.NewBoolVar(f"free_exceeded_sunday_{d}")
        model.Add(free_count >= limit + 1).OnlyEnforceIf(free_exceeded)
        model.Add(free_count <= limit).OnlyEnforceIf(free_exceeded.Not())
        
        deficit_positive = model.NewBoolVar(f"deficit_positive_sunday_{d}")
        model.Add(daily_deficit[d] >= 1).OnlyEnforceIf(deficit_positive)
        model.Add(daily_deficit[d] <= 0).OnlyEnforceIf(deficit_positive.Not())
        
        exceeded[d] = model.NewBoolVar(f"exceeded_sunday_{d}")
        model.AddBoolAnd([free_exceeded, deficit_positive]).OnlyEnforceIf(exceeded[d])
        model.AddBoolOr([free_exceeded.Not(), deficit_positive.Not()]).OnlyEnforceIf(exceeded[d].Not())

    total_exceeded_sundays = model.NewIntVar(0, len(sundays), "total_exceeded_sundays")
    model.Add(total_exceeded_sundays == sum(exceeded[d] for d in sundays))

    if total_exceeded_sundays_weight > 0:
        objective_terms.append(total_exceeded_sundays * total_exceeded_sundays_weight)

    # ===============================
    # 7.1 Control the periodicity of free Sundays
    # ===============================
    
    excess_free_sundays_per_worker = {}
    windows                        = [sundays[i : i + 3] for i in range(len(sundays) - 2)]

    if workers_not_complete_exist:
        all_workers_not_complete = workers_not_complete + workers_past
        for w in workers_not_complete:
            window_violations = []

            for idx, window in enumerate(windows):
                free_sundays = []

                for d in window:
                    if (w, d, 'L') in shift:
                        free = shift[(w, d, 'L')]  
                    else:
                        free = model.NewIntVar(0, 0, f"missing_L_{w}_{d}")

                    free_sundays.append(free)

                total_free = model.NewIntVar(0, 3, f"free_3s_{w}_{idx}")
                model.Add(total_free == sum(free_sundays))

                violation = model.NewBoolVar(f"excess_free_sunday_{w}_{idx}")
                model.Add(total_free >= 2).OnlyEnforceIf(violation)
                model.Add(total_free <= 1).OnlyEnforceIf(violation.Not())

                window_violations.append(violation)

            total_excess = model.NewIntVar(0, len(window_violations), f"total_excess_free_sundays_{w}")
            model.Add(total_excess == sum(window_violations))
            excess_free_sundays_per_worker[w] = total_excess

        total_excess_free_sundays = model.NewIntVar(0, len(sundays) * len(workers_not_complete),"total_excess_free_sundays")
        model.Add(total_excess_free_sundays == sum(excess_free_sundays_per_worker.values()))
        objective_terms.append(total_excess_free_sundays * sunday_imbalance_weight_periodicity)

    # ===============================
    # 8. Balancing LQ's across the year
    # ===============================

    parts = np.array_split(days_of_year_real, 6) 

    if workers_not_complete_exist:
        for w in all_workers_not_complete:
            list_of_free_LQs_per_semester=[]
            for part in parts:   
                LQs_semester = sum(shift[(w, d - 1, 'LQ')] for d in part if (w, d - 1, 'LQ') in shift)
                list_of_free_LQs_per_semester.append(LQs_semester)    

            max_free_LQs = model.NewIntVar(0, len(sundays), f"max_free_semester_LQs_{w}")
            min_free_LQs = model.NewIntVar(0, len(sundays), f"min_free_semester_LQs_{w}") 

            model.AddMaxEquality(max_free_LQs, list_of_free_LQs_per_semester)
            model.AddMinEquality(min_free_LQs, list_of_free_LQs_per_semester)

            semester_diff = model.NewIntVar(0, len(sundays), f"semester_diff_{w}")
            model.Add(semester_diff == max_free_LQs - min_free_LQs)
            objective_terms.append(semester_diff*LQ_imbalance_weight_average)  

    # ===============================
    # 9. Control the worst-case outcome LQs 
    # ===============================

    diff_per_worker_LQ = []

    if workers_not_complete_exist:
        for w in all_workers_not_complete:
            list_of_free_LQs_per_semester = []

            for part_index, part in enumerate(parts):
                LQs_vars = [shift[(w, d - 1, 'LQ')] for d in part if (w, d - 1, 'LQ') in shift]

                if LQs_vars:
                    LQs_semester = sum(LQs_vars)
                else:
                    LQs_semester = model.NewIntVar(0, 0, f"LQs_empty_{w}_{part_index}")

                list_of_free_LQs_per_semester.append(LQs_semester)

            max_free_LQs = model.NewIntVar(0, len(sundays), f"max_free_semester_LQs_{w}")
            min_free_LQs = model.NewIntVar(0, len(sundays), f"min_free_semester_LQs_{w}")

            if list_of_free_LQs_per_semester:
                model.AddMaxEquality(max_free_LQs, list_of_free_LQs_per_semester)
                model.AddMinEquality(min_free_LQs, list_of_free_LQs_per_semester)
            else:
                model.Add(max_free_LQs == 0)
                model.Add(min_free_LQs == 0)

            semester_diff = model.NewIntVar(0, len(sundays), f"semester_diff_{w}")
            model.Add(semester_diff == max_free_LQs - min_free_LQs)

            diff_per_worker_LQ.append(semester_diff)

        max_diff_LQ = model.NewIntVar(0, len(sundays), "max_diff_LQ")
        model.AddMaxEquality(max_diff_LQ, diff_per_worker_LQ)
        objective_terms.append(max_diff_LQ * LQ_imbalance_weight)

    # ===============================
    # 10. Penalize having workers working on the same days as their OuT partner
    # ===============================

    if out_workers:
        for d in days_of_year:
            for w in all_workers:
                if w in out_workers:
                    w_shifts = sum(shift.get((w, d, s), 0) for s in real_working_shift +  ['Mot', 'TC'])
                    w_worked_day = model.NewBoolVar(f"worked_same_day_{w}_{d}")
                    model.Add(w_shifts >= 1).OnlyEnforceIf(w_worked_day)
                    model.Add(w_shifts == 0).OnlyEnforceIf(w_worked_day.Not())
                    for outie in out_workers[w]:
                        if outie in workers_past and w in workers_past:
                            continue
                        w_shifts = sum(shift.get((outie, d, s), 0) for s in real_working_shift +  ['Mot', 'TC'])
                        outie_worked_day = model.NewBoolVar(f"worked_same_day_{w}_{outie}_{d}")
                        model.Add(w_shifts >= 1).OnlyEnforceIf(outie_worked_day)
                        model.Add(w_shifts == 0).OnlyEnforceIf(outie_worked_day.Not())

                        worked_same_day = model.NewBoolVar(f"worked_same_day_{w}_{outie}_{d}")
                        model.AddBoolAnd([w_worked_day, outie_worked_day]).OnlyEnforceIf(worked_same_day)
                        model.AddBoolOr([w_worked_day.Not(), outie_worked_day.Not()]).OnlyEnforceIf(worked_same_day.Not())

                        objective_terms.append(out_weight * worked_same_day)
              
    model.Minimize(sum(objective_terms))
