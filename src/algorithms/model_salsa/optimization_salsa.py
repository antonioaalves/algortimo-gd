
from pyexpat import model
from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config
_config_manager = get_config()
logger = get_logger(_config_manager.project_name)
import numpy as np
import math 


def salsa_optimization(model, days_of_year, workers, workers_complete_cycle, real_working_shift, shift, pessObj, working_days, closed_holidays, min_workers,max_workers, week_to_days, sundays, c2d, first_day, last_day, role_by_worker, work_day_hours, workers_past, year_range):

    pos_diff_dict = {}
    neg_diff_dict = {}
    no_workers_penalties = {}
    min_workers_penalties_shift = {}
    min_workers_penalties_day = {}
    inconsistent_shift_penalties = {}

    # Create the objective function with heavy penalties
    objective_terms = []
    PESS_OBJ_PENALTY = 1000  # Penalty for deviations from pessObj
    CONSECUTIVE_FREE_DAY = -1  # Bonus for consecutive free days
    HEAVY_PENALTY = 300  # Penalty for days with no workers
    MIN_WORKER_PENALTY_SHIFT = 600  # Penalty for breaking minimum worker requirements per shift
    MIN_WORKER_PENALTY_DAY = 6000  # Penalty for breaking minimum worker requirements per day
    SUNDAY_YEAR_BALANCE_PENALTY = 1  # Penalty for unbalanced Sunday free days ALL YEAR
    C2D_YEAR_BALANCE_PENALTY = 8  # Penalty for unbalanced C2D free days ALL YEAR
    INCONSISTENT_SHIFT_PENALTY = 3  # Penalty for inconsistent shift types
    SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY = 5  # Penalty for balancing Sundays across workers
    C2D_BALANCE_ACROSS_WORKERS_PENALTY = 5  # Penalty for balancing C2D across workers
    MANAGER_KEYHOLDER_CONFLICT_PENALTY = 30000
    KEYHOLDER_KEYHOLDER_CONFLICT_PENALTY = 50000
    MANAGER_MANAGER_CONFLICT_PENALTY = 50000
    hours_scale = 1000

    optimization_details = {
        'point_1_pessobj_deviations': {
            'variables': {},
            'weights': {},
            'penalty_weight': PESS_OBJ_PENALTY
        },
        'point_2_consecutive_free_days': {
            'variables': [],
            'weight': CONSECUTIVE_FREE_DAY
        },
        'point_3_no_workers': {
            'variables': {},
            'penalty_weight': HEAVY_PENALTY
        },
        'point_4_1_min_workers': {
            'variables': {},
            'penalty_weight': MIN_WORKER_PENALTY_SHIFT
        },
        'point_4_2_min_workers': {
            'variables': {},
            'penalty_weight': MIN_WORKER_PENALTY_DAY
        },
        'point_5_1_sunday_balance': {
            'variables': [],
            'penalty_weight': SUNDAY_YEAR_BALANCE_PENALTY
        },
        'point_5_2_c2d_balance': {
            'variables': [],
            'penalty_weight': C2D_YEAR_BALANCE_PENALTY
        },
        'point_6_inconsistent_shifts': {
            'variables': {},
            'penalty_weight': INCONSISTENT_SHIFT_PENALTY
        },
        'point_7_sunday_balance_across_workers': {
            'variables': [],
            'penalty_weight': SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY
        },
        'point_7b_lq_balance_across_workers': {
            'variables': [],
            'penalty_weight': C2D_BALANCE_ACROSS_WORKERS_PENALTY
        },
        'point_8_manager_keyholder_conflicts': {
            'variables': {},
            'penalty_weights': {
                'mgr_kh_same_off': MANAGER_KEYHOLDER_CONFLICT_PENALTY,
                'kh_overlap': KEYHOLDER_KEYHOLDER_CONFLICT_PENALTY,
                'mgr_overlap': MANAGER_MANAGER_CONFLICT_PENALTY
            }
        }
    }

    scale=10000
    objective_terms = []
    days_of_year_real= [d for d in days_of_year if year_range[0] <= d <= year_range[1] and d not in closed_holidays]
    days_of_year_working= [d for d in days_of_year if d not in closed_holidays]
    workers_not_complete = [w for w in workers if w not in workers_complete_cycle]
    if len(workers_not_complete) < 1:
        workers_not_complete = 1

    n = int(len(days_of_year_real) / 6)

    q_groups = {i: [] for i in range(6)}

    for w in workers_not_complete:
        days_worked = len(working_days[w])
       
        if days_worked >= n*5:
            q_groups[0].append(w)
        elif days_worked >= n*4:
            q_groups[1].append(w)
        elif days_worked >= n*3:
            q_groups[2].append(w)
        elif days_worked >= n*2:
            q_groups[3].append(w)
        elif days_worked >= n*1:
            q_groups[4].append(w)
        else:
            q_groups[5].append(w)

    
    q0 = q_groups[0]
    q1 = q_groups[1]
    q2 = q_groups[2]
    q3 = q_groups[3]
    q4 = q_groups[4]
    q5 = q_groups[5]

    # Weights:
        
    excess_min_worst_scenario=(3/5) * sum(
        pessObj.get((d, s), 0)
        for d in days_of_year_working
        for s in real_working_shift)
    if excess_min_worst_scenario < 1:
        logger.error(f" Estimativas a virem vazias ao longo do ano todo! {excess_min_worst_scenario}")
        excess_min_worst_scenario = 0.1
    percentage_of_importance_no_excess=0 
    excess_weight=int(scale*percentage_of_importance_no_excess/excess_min_worst_scenario)

    deficit_min_worst_scenario= (1/8) * sum(
        pessObj.get((d, s), 0)
        for d in days_of_year_working
        for s in real_working_shift)
    if deficit_min_worst_scenario < 1:
        logger.error(f" Estimativas a virem vazias ao longo do ano todo! {deficit_min_worst_scenario}")
        deficit_min_worst_scenario = 0.1
    percentage_of_importance_no_deficit=3
    deficit_weight=int(scale*percentage_of_importance_no_deficit/deficit_min_worst_scenario)

    no_workers_min_worst_scenario=1
    percentage_of_importance_workers=0
    no_workers_weight=int(scale*percentage_of_importance_workers/no_workers_min_worst_scenario)

    sundays_diff_min_worst_scenario=2
    percentage_of_importance_sundays_equal=1
    sundays_diff_weight=int(scale*percentage_of_importance_sundays_equal/sundays_diff_min_worst_scenario)

    LQs_diff_min_worst_scenario=1
    percentage_of_importance_LQs_equal=1
    LQs_diff_weight=int(scale*percentage_of_importance_LQs_equal/LQs_diff_min_worst_scenario)

    no_consec_free_days_min_worst_scenario=52*2*len(workers)
    percentage_of_importance_consec_free_days=0
    no_consec_free_days_weight=int(scale*percentage_of_importance_consec_free_days/no_consec_free_days_min_worst_scenario)

    sunday_imbalance_per_semeste_min_worst_scenario=2
    percentage_of_importance_sunday_balance=1
    sunday_imbalance_weight=int(scale*percentage_of_importance_sunday_balance/sunday_imbalance_per_semeste_min_worst_scenario)
    sunday_imbalance_weight_average=int(math.ceil(sunday_imbalance_weight/len(workers_not_complete)))
    total_sunday_imbalance_weight=int(scale*percentage_of_importance_sunday_balance/sunday_imbalance_per_semeste_min_worst_scenario)

    sunday_imbalance_weight_periodicity_min_worst_scenario=1
    percentage_of_importance_sunday_imbalance_weight_periodicity=1
    sunday_imbalance_weight_periodicity=int(scale*percentage_of_importance_sunday_imbalance_weight_periodicity/sunday_imbalance_weight_periodicity_min_worst_scenario)

    LQ_imbalance_per_semeste_min_worst_scenario=1
    percentage_of_importance_LQ_balance=1
    LQ_imbalance_weight=int(scale*percentage_of_importance_LQ_balance/LQ_imbalance_per_semeste_min_worst_scenario)
    LQ_imbalance_weight_average=int(math.ceil(LQ_imbalance_weight/len(workers_not_complete)))

    inconsistent_number_of_weeks_min_worst_scenario=52*len(workers)
    percentage_of_importance_consistent_number_of_weeks=0
    inconsistent_number_of_weeks_weight=int(scale*percentage_of_importance_consistent_number_of_weeks/inconsistent_number_of_weeks_min_worst_scenario)

    no_key_shift_min_worst_scenario=1
    percentage_of_importance_key=2
    no_key_weight=int(scale*percentage_of_importance_key/no_key_shift_min_worst_scenario)

    same_free_day_manager_min_worst_scenario= 3    
    percentage_of_importance_managers=0
    same_free_day_manager_weight=int(scale*percentage_of_importance_managers/same_free_day_manager_min_worst_scenario)

    same_free_day_keyholders_min_worst_scenario= 3  # This includes the managers  
    percentage_of_importance_keyholders=0
    same_free_day_keyholders_weight=int(scale*percentage_of_importance_keyholders/same_free_day_keyholders_min_worst_scenario)

    excess_and_deficit_worst_scenario=4
    percentage_of_importance_excess_and_deficit=1
    excess_and_deficit_weight=int(scale*percentage_of_importance_excess_and_deficit/excess_and_deficit_worst_scenario)

    number_of_deficit_day_worst_scenario=5
    percentage_of_importance_deficit_day=0 
    deficit_day_weight=int( scale*percentage_of_importance_deficit_day/number_of_deficit_day_worst_scenario )
    
    if len(workers) >= 6:
        free_days_per_day_worst_case_scenario= len(workers)//2
        number_free_days_exceeded_worst_case_scenario= 5 
        percentage_of_total_exceeded_days_weight=1 
    if len(workers) <6:
        percentage_of_total_exceeded_days_weight=0    
    total_exceeded_days_weight=int(scale*percentage_of_total_exceeded_days_weight/ number_free_days_exceeded_worst_case_scenario)
    
    day_deficit_hours_worst_case_scenario=8 #in a shift
    percentage_of_importance_day_deficit=2
    max_deficit_weight=int(scale*percentage_of_importance_day_deficit/day_deficit_hours_worst_case_scenario) 

    deficit_over_x_worst_case=8 #value of x in hours
    percentage_of_importance_day_deficit_over_x=1
    num_days_deficit_over_x_worst_case= 100
    deficit_over_x_day_weight=int(scale*percentage_of_importance_day_deficit_over_x/num_days_deficit_over_x_worst_case)
 
    deficit_over_y_worst_case=12 #value of y in hours
    percentage_of_importance_day_deficit_over_y=1
    num_days_deficit_over_y_worst_case= 50
    deficit_over_y_day_weight=int(scale*percentage_of_importance_day_deficit_over_y/num_days_deficit_over_y_worst_case)

    deficit_over_z_worst_case=16 #value of z in hours
    percentage_of_importance_day_deficit_over_z=1
    num_days_deficit_over_z_worst_case= 15
    deficit_over_z_day_weight=int(scale*percentage_of_importance_day_deficit_over_z/num_days_deficit_over_z_worst_case)

    deficit_over_t_worst_case=20 #value of t in hours
    percentage_of_importance_day_deficit_over_t=1
    num_days_deficit_over_t_worst_case= 5 
    deficit_over_t_day_weight=int(scale*percentage_of_importance_day_deficit_over_t/num_days_deficit_over_t_worst_case)

    deficit_over_k_worst_case=24 #value of k in hours
    percentage_of_importance_day_deficit_over_k=1
    num_days_deficit_over_k_worst_case= 2
    deficit_over_k_day_weight=int(scale*percentage_of_importance_day_deficit_over_k/num_days_deficit_over_k_worst_case)

    deficit_over_q_worst_case=32 #value of q in hours
    percentage_of_importance_day_deficit_over_q=1
    num_days_deficit_over_q_worst_case= 1
    deficit_over_q_day_weight=int(scale*percentage_of_importance_day_deficit_over_q/num_days_deficit_over_q_worst_case)


    # ===============================
    # 1.1 Excess and deficit per shift
    # ===============================

    excess_diff_vars = []
    deficit_diff_vars = []

    for d in days_of_year:
        for s in real_working_shift:
            target = pessObj.get((d, s), 0)
            assigned_workers = sum(
                shift[(w, d, s)] * work_day_hours[w].get(d, 8)
                for w in workers if (w, d, s) in shift
            )

            excess  = model.NewIntVar(0, len(workers)*8, f'excess_{d}_{s}')
            deficit = model.NewIntVar(0, target*8, f'deficit_{d}_{s}')

            model.Add(excess >= assigned_workers - target)
            model.Add(deficit >= target - assigned_workers)

            excess_diff_vars.append((d, s, excess))
            deficit_diff_vars.append((d, s, deficit))

    
    # ===============================
    # 1.2. Daily totals
    # ===============================

    daily_deficit = {}
    daily_excess  = {}

    max_daily_deficit_possible = len(real_working_shift)*max(pessObj.values()) * 8
    max_daily_excess_possible  = len(real_working_shift)*len(workers) * 8

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

    objective_terms.append(sum(excess for (d, _, excess) in excess_diff_vars if d in days_of_year_working) * excess_weight)
    objective_terms.append(sum(deficit for (d, _, deficit) in deficit_diff_vars if d in days_of_year_working) * deficit_weight)
    objective_terms.append(max_deficit * max_deficit_weight)

    # ===============================
    # 1.4. Day-level Booleans
    # ===============================

    day_has_excess  = {}
    day_has_deficit = {}
    day_has_both    = {}
    penalty_vars    = []

    for d in days_of_year_working:
        day_has_excess[d]  = model.NewBoolVar(f'day_{d}_has_excess')
        day_has_deficit[d] = model.NewBoolVar(f'day_{d}_has_deficit')
        day_has_both[d]    = model.NewBoolVar(f'day_{d}_has_both')


        model.Add(daily_excess[d] >= 1).OnlyEnforceIf(day_has_excess[d])
        model.Add(daily_excess[d] == 0).OnlyEnforceIf(day_has_excess[d].Not())

        model.Add(daily_deficit[d] >= 1).OnlyEnforceIf(day_has_deficit[d])
        model.Add(daily_deficit[d] == 0).OnlyEnforceIf(day_has_deficit[d].Not())

        model.AddBoolAnd([day_has_excess[d], day_has_deficit[d]]).OnlyEnforceIf(day_has_both[d])
        model.AddBoolOr([day_has_excess[d].Not(), day_has_deficit[d].Not()]).OnlyEnforceIf(day_has_both[d].Not())

        penalty_vars.append(day_has_both[d])

    objective_terms.append(sum(penalty_vars) * excess_and_deficit_weight)

    
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

        num_days = model.NewIntVar(
            0, len(days_of_year_working),
            f'num_days_deficit_over_{c}'
        )

        model.Add(
            num_days == sum(day_deficit_over[c][d] for d in days_of_year_working)
        )

        objective_terms.append(num_days * weight)


    # 2 No workers in a day
    
    zero_assigned_vars = []
    for d in days_of_year_working:
        for s in real_working_shift:  
            target = pessObj.get((d,s), 0)
            assigned_workers = sum(shift[(w,d,s)] for w in workers if (w,d,s) in shift)

            if target > 0:
                zero_assigned = model.NewBoolVar(f'zero_assigned_{d}_{s}')
                model.Add(assigned_workers == 0).OnlyEnforceIf(zero_assigned)
                model.Add(assigned_workers >= 1).OnlyEnforceIf(zero_assigned.Not())
                zero_assigned_vars.append(zero_assigned)

    objective_zero = sum(zero_assigned_vars)
    if percentage_of_importance_workers>0:
        objective_terms.append(objective_zero * no_workers_weight)


    # 3. Balancing number of sundays free days across the workers 
 
    for qi, workers_q in q_groups.items():
    
        if len(workers_q) <= 1:
            continue  

        sundays_per_worker_q = []

        for w in workers_q:
            sunday_free = sum(
                shift[(w, d, 'L')]
                for d in sundays
                if (w, d, 'L') in shift and year_range[0] <= d <= year_range[1]
            )
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


    # 4. Balancing number of LQ across the workers 

    
    for qi, workers_q in q_groups.items():
    
        if len(workers_q) <= 1:
            continue  

        LQs_per_worker_q = []

        for w in workers_q:
            LQs = sum(
                shift[(w, d-1, 'LQ')]
                for d in sundays
                if (w, d-1, 'LQ') in shift and year_range[0] < d <= year_range[1]
            )
            LQs_per_worker_q.append(LQs)
        if not LQs_per_worker_q:
            continue    

        
        max_LQs_q = model.NewIntVar(0, len(sundays), f"max_LQs_q{qi}")
        min_LQs_q = model.NewIntVar(0, len(sundays), f"min_LQs_q{qi}")

        model.AddMaxEquality(max_LQs_q, LQs_per_worker_q)
        model.AddMinEquality(min_LQs_q, LQs_per_worker_q)

       
        LQs_diff_q = model.NewIntVar(0, len(sundays), f"LQs_diff_q{qi}")
        model.Add(LQs_diff_q == max_LQs_q - min_LQs_q)

        
        objective_terms.append(LQs_diff_q * LQs_diff_weight)


    # 5. Not consecutive free days error 
      
    list_of_consecutive_free_days_per_worker = []
    list_of_free_days_per_worker = []
    is_free_dict = {} #new

    for w in workers:
        consecutive_free_vars = []
        free_day_vars = []

        for d in working_days[w]:
            
            is_free = model.NewBoolVar(f"is_free_{w}_{d}")
            if (w,d,'L') in shift and (w,d,'LQ') in shift:
                model.Add(is_free == shift[(w, d, 'L')] + shift[(w, d, 'LQ')])
                free_day_vars.append(is_free)
                is_free_dict[(w, d)] = is_free #new

            
            if d+1 in working_days[w] and (w,d + 1,'L') in shift and (w,d + 1,'LQ') in shift:
                next_d = d+1
                next_is_free = model.NewBoolVar(f"next_is_free_{w}_{next_d}")
                model.Add(next_is_free == shift[(w, next_d, 'L')] + shift[(w, next_d, 'LQ')])

                consecutive_free = model.NewBoolVar(f"consecutive_free_{w}_{d}")
                model.AddBoolAnd([is_free, next_is_free]).OnlyEnforceIf(consecutive_free)
                model.AddBoolOr([is_free.Not(), next_is_free.Not()]).OnlyEnforceIf(consecutive_free.Not())

                consecutive_free_vars.append(consecutive_free)

        
        list_of_free_days_per_worker.append(sum(free_day_vars))
        list_of_consecutive_free_days_per_worker.append(sum(consecutive_free_vars))


    diff_vars = [
        list_of_free_days_per_worker[i] - list_of_consecutive_free_days_per_worker[i]
        for i in range(len(workers))
    ]

    objective_consecutive_free_days = sum(diff_vars)
    if no_consec_free_days_weight > 0:
        objective_terms.append(objective_consecutive_free_days*no_consec_free_days_weight)


    # 5.1 Try not to assign too many free days on the same day with deficit. 
    limit = free_days_per_day_worst_case_scenario

    total_exceeded_days = model.NewIntVar(0, len(days_of_year_working), "total_exceeded_days")
    exceeded = {}

    for d in days_of_year_working:
        # total de free-days por dia
        free_count = model.NewIntVar(0, len(workers), f"free_count_day_{d}")
        free_vars_today = [is_free_dict[(w, d)] for w in workers if (w, d) in is_free_dict]
        model.Add(free_count == sum(free_vars_today))

        # variável binária: free-days excedem o limite
        free_exceeded = model.NewBoolVar(f"free_exceeded_day_{d}")
        model.Add(free_count >= limit + 1).OnlyEnforceIf(free_exceeded)
        model.Add(free_count <= limit).OnlyEnforceIf(free_exceeded.Not())

        # variável binária: há déficit nesse dia
        deficit_positive = model.NewBoolVar(f"deficit_positive_{d}")
        model.Add(daily_deficit[d] >= 1).OnlyEnforceIf(deficit_positive)
        model.Add(daily_deficit[d] <= 0).OnlyEnforceIf(deficit_positive.Not())

        # exceeded[d] = 1 se free-days excedem E há déficit
        exceeded[d] = model.NewBoolVar(f"exceeded_day_{d}")
        model.AddBoolAnd([free_exceeded, deficit_positive]).OnlyEnforceIf(exceeded[d])
        model.AddBoolOr([free_exceeded.Not(), deficit_positive.Not()]).OnlyEnforceIf(exceeded[d].Not())

        # soma total de dias relevantes
    model.Add(total_exceeded_days == sum(exceeded[d] for d in days_of_year_working))

    # adicionar ao objetivo
    if total_exceeded_days_weight > 0:
        objective_terms.append(total_exceeded_days * total_exceeded_days_weight)


    # 6. Balancing sundays across the year (average)
    
    parts = np.array_split(days_of_year_real, 12) 
    sunday_parts=[]
    for part in parts:
        sunday_part=[d for d in part if d in sundays]
        sunday_parts.append(sunday_part)
    
    for w in workers_not_complete :
        list_of_free_sundays_per_semester=[]
        for part in sunday_parts:
            free_sundays_semester=sum(shift[(w, d, 'L')] for d in part if (w,d,'L') in shift)
            list_of_free_sundays_per_semester.append(free_sundays_semester)
        
        max_free_sundays = model.NewIntVar(0, len(sundays), f"max_free_semester_sundays_{w}")
        min_free_sundays = model.NewIntVar(0, len(sundays), f"min_free_semester_sundays_{w}") 
        
        model.AddMaxEquality(max_free_sundays, list_of_free_sundays_per_semester)
        model.AddMinEquality(min_free_sundays, list_of_free_sundays_per_semester)

        semester_diff = model.NewIntVar(0, len(sundays), f"semester_diff_{w}")
        model.Add(semester_diff == max_free_sundays - min_free_sundays)
        
        objective_terms.append(semester_diff*sunday_imbalance_weight_average)  


    #6.1 Control the worst-case outcome sundays
    
    diff_per_worker = []

    for w in workers_not_complete:
        list_of_free_sundays_per_semester = []

        for idx, part in enumerate(sunday_parts):
            sunday_vars = [shift[(w, d, 'L')] for d in part if (w, d, 'L') in shift]

            if sunday_vars:
                free_sundays_semester = sum(sunday_vars)
            else:
                free_sundays_semester = model.NewIntVar(0, 0, f"free_sundays_empty_{w}_{idx}")

            list_of_free_sundays_per_semester.append(free_sundays_semester)

        max_free_sundays = model.NewIntVar(0, len(sundays), f"max_free_semester_sundays_{w}")
        min_free_sundays = model.NewIntVar(0, len(sundays), f"min_free_semester_sundays_{w}")

        model.AddMaxEquality(max_free_sundays, list_of_free_sundays_per_semester)
        model.AddMinEquality(min_free_sundays, list_of_free_sundays_per_semester)

        semester_diff = model.NewIntVar(0, len(sundays), f"semester_diff_{w}")
        model.Add(semester_diff == max_free_sundays - min_free_sundays)

        diff_per_worker.append(semester_diff)


    max_diff_worker = model.NewIntVar(0, len(sundays), "max_diff_worker")
    model.AddMaxEquality(max_diff_worker, diff_per_worker)

    
    objective_terms.append(max_diff_worker * sunday_imbalance_weight)  

    
    # 6.2 Balancing Sundays across the year (global)
   
    parts = np.array_split(days_of_year_real, 12)

    sunday_parts = []
    for part in parts:
        sunday_parts.append([d for d in part if d in sundays])

    sundays_per_part = []

    max_possible = len(workers) * len(sundays)

    for i, part in enumerate(sunday_parts):

        sundays_count = model.NewIntVar(
            0,
            max_possible,
            f"sundays_part_{i}"
        )

        model.Add(
            sundays_count == sum(
                shift[(w, d, 'L')]
                for w in workers
                for d in part
                if (w, d, 'L') in shift
            )
        )

        sundays_per_part.append(sundays_count)


    max_sundays = model.NewIntVar(
        0,
        max_possible,
        "max_sundays_year"
    )

    min_sundays = model.NewIntVar(
        0,
        max_possible,
        "min_sundays_year"
    )
    model.AddMaxEquality(max_sundays, sundays_per_part)
    model.AddMinEquality(min_sundays, sundays_per_part)


    
    sunday_difference = model.NewIntVar(
        0,
        max_possible,
        "sunday_difference"
    )
    model.Add(sunday_difference == max_sundays - min_sundays)


    if sunday_imbalance_weight_average > 0:
        objective_terms.append(
            sunday_difference * total_sunday_imbalance_weight
        )

    #6.3 Control the periodicity of excessive free Sundays
    
    excess_free_sundays_per_worker = {}

    windows = [
        sundays[i:i+3]
        for i in range(len(sundays) - 2)
    ]

    for w in workers_not_complete:
        window_violations = []

        for idx, window in enumerate(windows):
            free_sundays = []

            for d in window:
                if (w, d, 'L') in shift:
                    free = shift[(w, d, 'L')]  # 0/1
                else:
                    free = model.NewIntVar(0, 0, f"missing_L_{w}_{d}")

                free_sundays.append(free)

            total_free = model.NewIntVar(0, 3, f"free_3s_{w}_{idx}")
            model.Add(total_free == sum(free_sundays))

            violation = model.NewBoolVar(f"excess_free_sunday_{w}_{idx}")
            model.Add(total_free >= 2).OnlyEnforceIf(violation)
            model.Add(total_free <= 1).OnlyEnforceIf(violation.Not())

            window_violations.append(violation)

        total_excess = model.NewIntVar(
            0, len(window_violations),
            f"total_excess_free_sundays_{w}"
        )
        model.Add(total_excess == sum(window_violations))

        excess_free_sundays_per_worker[w] = total_excess

    total_excess_free_sundays = model.NewIntVar(0, len(sundays) * len(workers_not_complete),"total_excess_free_sundays")

    model.Add(total_excess_free_sundays == sum(excess_free_sundays_per_worker.values()))

    objective_terms.append(total_excess_free_sundays * sunday_imbalance_weight_periodicity)
       

    # 7. Balancing LQ's across the year

    parts = np.array_split(days_of_year_real, 6) 

    for w in workers_not_complete:
        list_of_free_LQs_per_semester=[]
        for part in parts:   
            LQs_semester = sum(shift[(w, d-1, 'LQ')] for d in part if (w, d-1, 'LQ') in shift)
            list_of_free_LQs_per_semester.append(LQs_semester)    
             
        max_free_LQs = model.NewIntVar(0, len(sundays), f"max_free_semester_LQs_{w}")
        min_free_LQs = model.NewIntVar(0, len(sundays), f"min_free_semester_LQs_{w}") 
            
        model.AddMaxEquality(max_free_LQs, list_of_free_LQs_per_semester)
        model.AddMinEquality(min_free_LQs, list_of_free_LQs_per_semester)
            
        semester_diff = model.NewIntVar(0, len(sundays), f"semester_diff_{w}")
        model.Add(semester_diff == max_free_LQs - min_free_LQs)
        
        objective_terms.append(semester_diff*LQ_imbalance_weight_average)  


   # 7.1 Control the worst-case outcome LQs 

    diff_per_worker_LQ = []

    for w in workers_not_complete:
        list_of_free_LQs_per_semester = []

        for part_index, part in enumerate(parts):
            LQs_vars = [shift[(w, d-1, 'LQ')] for d in part if (w, d-1, 'LQ') in shift]

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
            
        
    # 8. Weeks of inconsistent shifts error

    for w in workers:
        inconsistent_weeks=[]
        for week, days in week_to_days.items():
            
            shift_M=model.NewBoolVar(f"shift_M_{w}_{week}")
            model.AddBoolOr([shift[(w, d, 'M')] for d in days if (w,d,'M') in shift]).OnlyEnforceIf(shift_M)
            model.AddBoolAnd([shift[(w, d, 'M')].Not() for d in days if (w,d,'M') in shift]).OnlyEnforceIf(shift_M.Not())
            
            shift_T=model.NewBoolVar(f"shift_T_{w}_{week}")
            model.AddBoolOr([shift[(w, d, 'T')] for d in days if (w,d,'T') in shift]).OnlyEnforceIf(shift_T)
            model.AddBoolAnd([shift[(w, d, 'T')].Not() for d in days if (w,d,'T') in shift]).OnlyEnforceIf(shift_T.Not())
           
            is_inconsistent=model.NewBoolVar(f"is_inconsistent_{w}_{week}")
            model.AddBoolAnd([shift_M, shift_T]).OnlyEnforceIf(is_inconsistent)
            model.AddBoolOr([shift_M.Not(), shift_T.Not()]).OnlyEnforceIf(is_inconsistent.Not())

            inconsistent_weeks.append(is_inconsistent)
            
        if inconsistent_number_of_weeks_weight>0:   
            objective_terms.append(sum(inconsistent_weeks)*inconsistent_number_of_weeks_weight)   
            

    # 9. No managers/keyholders error

    workers_with_key=[w for w in workers if role_by_worker.get(w, "normal") in ["manager", "keyholder"]]
    list_shifts_no_keys=[]
    for d in days_of_year_working:
        for s in real_working_shift:
            if pessObj.get((d, s), 0)>0:
                no_key=model.NewBoolVar(f"no_key_{d}_{s}")
                model.AddBoolAnd([shift[(w, d, s)].Not() for w in workers_with_key if (w,d,s) in shift]).OnlyEnforceIf(no_key)
                model.AddBoolOr([shift[(w, d, s)] for w in workers_with_key if (w,d,s) in shift]).OnlyEnforceIf(no_key.Not())
                list_shifts_no_keys.append(no_key)

    objective_terms.append(sum(list_shifts_no_keys)*no_key_weight)   

        
    # 10. Same free day assigned to managers/keyholders error      

    managers=[w for w in workers if role_by_worker.get(w, "normal") =="manager"]
    keyholders=[w for w in workers if role_by_worker.get(w, "normal") =="keyholder"]
    workers_with_key=managers+keyholders 

    for d in days_of_year_working:
        for s in real_working_shift:
            if pessObj.get((d, s), 0)>0:
                free_manager=model.NewIntVar(0, len(managers), f"free_managers_{d}_{s}")
                model.Add(free_manager==sum([shift[(w, d, s)].Not() for w in managers if (w,d,s) in shift]))
                free_keyholders=model.NewIntVar(0, len(keyholders), f"free_keyholders_{d}_{s}")
                model.Add(free_keyholders==sum([shift[(w, d, s)].Not() for w in keyholders if (w,d,s) in shift]))
                if same_free_day_manager_weight>0:
                    objective_terms.append(free_manager*same_free_day_manager_weight)  
                if same_free_day_keyholders_weight>0:     
                    objective_terms.append(free_keyholders*same_free_day_keyholders_weight)   
                 

    model.Minimize(sum(objective_terms))

    return optimization_details



