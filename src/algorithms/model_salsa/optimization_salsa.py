from base_data_project.log_config import get_logger
from src.config import PROJECT_NAME
import numpy as np

logger = get_logger(PROJECT_NAME)


def salsa_optimization(model, days_of_year, workers, shift, pessObj, working_days, closed_holidays, week_to_days, sundays, role_by_worker):
    scale=10000
    objective_terms = []
    real_working_shift=['M', 'T']

    # Weights:
        
    excess_min_worst_scenario=(2/5) * sum(
        pessObj.get((d, s), 0)
        for d in days_of_year
        for s in real_working_shift)
    percentage_of_importance_no_excess=1
    excess_weight=int(scale*percentage_of_importance_no_excess/excess_min_worst_scenario)

    deficit_min_worst_scenario= (1/5) * sum(
        pessObj.get((d, s), 0)
        for d in days_of_year
        for s in real_working_shift)
    percentage_of_importance_no_deficit=1
    deficit_weight=int(scale*percentage_of_importance_no_deficit/deficit_min_worst_scenario)

    no_workers_min_worst_scenario=1
    percentage_of_importance_workers=1
    no_workers_weight=int(scale*percentage_of_importance_workers/no_workers_min_worst_scenario)

    sundays_diff_min_worst_scenario=2
    percentage_of_importance_sundays_equal=1
    sundays_diff_weight=int(scale*percentage_of_importance_sundays_equal/sundays_diff_min_worst_scenario)


    LQs_diff_min_worst_scenario=2
    percentage_of_importance_LQs_equal=1
    LQs_diff_weight=int(scale*percentage_of_importance_LQs_equal/LQs_diff_min_worst_scenario)

    no_consec_free_days_min_worst_scenario=52*2*len(workers)
    percentage_of_importance_consec_free_days=1
    no_consec_free_days_weight=int(scale*percentage_of_importance_consec_free_days/no_consec_free_days_min_worst_scenario)

    sunday_imbalance_per_semeste_min_worst_scenario=5
    percentage_of_importance_sunday_balance=1
    sunday_imbalance_weight=int(scale*percentage_of_importance_sunday_balance/sunday_imbalance_per_semeste_min_worst_scenario)

    LQ_imbalance_per_semeste_min_worst_scenario=2
    percentage_of_importance_LQ_balance=1 
    LQ_imbalance_weight=int(scale*percentage_of_importance_LQ_balance/LQ_imbalance_per_semeste_min_worst_scenario)

    inconsistent_number_of_weeks_min_worst_scenario=52*len(workers)
    percentage_of_importance_consistent_number_of_weeks=1
    inconsistent_number_of_weeks_weight=int(scale*percentage_of_importance_consistent_number_of_weeks/inconsistent_number_of_weeks_min_worst_scenario)

    no_key_shift_min_worst_scenario=1
    percentage_of_importance_key=1
    no_key_weight=int(scale*percentage_of_importance_key/no_key_shift_min_worst_scenario)

    same_free_day_manager_min_worst_scenario= 3    #?
    percentage_of_importance_managers=1
    same_free_day_manager_weight=int(scale*percentage_of_importance_managers/same_free_day_manager_min_worst_scenario)

    same_free_day_keyholders_min_worst_scenario= 3 #? # This includes the manegers 
    percentage_of_importance_keyholders=1
    same_free_day_keyholders_weight=int(scale*percentage_of_importance_keyholders/same_free_day_keyholders_min_worst_scenario)
    
    max_excess_min_worst_scenario=16
    percentage_of_importance_max_excess=1
    max_excess_weight=int(scale*percentage_of_importance_max_excess/ max_excess_min_worst_scenario)
    
    
    max_deficit_min_worst_scenario=8
    percentage_of_importance_max_deficit=1
    max_deficit_weight=int(scale*percentage_of_importance_max_deficit/max_deficit_min_worst_scenario)
    
    
    
    
    # 1. Excess and deficit error

    excess_diff_vars = []
    deficit_diff_vars = []

    for d in days_of_year:
        for s in real_working_shift:
            target = pessObj.get((d, s), 0)
            assigned_workers = sum(shift[(w, d, s)] for w in workers if (w, d, s) in shift)
            
            # Variables for excess and deficit
            excess = model.NewIntVar(0, len(workers), f'excess_{d}_{s}')
            deficit = model.NewIntVar(0, target, f'deficit_{d}_{s}')
            
            
            # Constrain them to represent positive and negative deviations
            model.Add(excess >= assigned_workers - target)
            model.Add(deficit >= target - assigned_workers)
            
            # Store for objective
            excess_diff_vars.append(excess)
            deficit_diff_vars.append(deficit)


    objective_excess = sum(excess_diff_vars[i] for i in range(len(excess_diff_vars)))
    objective_terms.append(objective_excess*excess_weight)

    objective_deficit = sum(deficit_diff_vars[i] for i in range(len(deficit_diff_vars)))
    objective_terms.append(objective_deficit*deficit_weight)


    # 2. No workers in a day error

    zero_assigned_vars = []

    for d in days_of_year:
        for s in real_working_shift:
            target = pessObj.get((d,s), 0)
            assigned_workers = sum(shift[(w,d,s)] for w in workers if (w, d, s) in shift)
            
            if target > 0:
                
                zero_assigned = model.NewBoolVar(f'zero_assigned_{d}_{s}')
                
                # zero_assigned = 1 iff assigned_workers == 0
                model.Add(assigned_workers == 0).OnlyEnforceIf(zero_assigned)
                model.Add(assigned_workers != 0).OnlyEnforceIf(zero_assigned.Not())
                
                zero_assigned_vars.append(zero_assigned)

    objective_zero = sum(zero_assigned_vars[i] for i in range(len(zero_assigned_vars)))
    objective_terms.append(objective_zero*no_workers_weight)


    # 3. Balancing number of sundays free days across the workers 

    list_of_sundays_per_worker = []

    for w in workers:
        sunday_free = sum(shift[(w, d, 'L')] for d in sundays if (w, d, 'L') in shift)
        list_of_sundays_per_worker.append(sunday_free)


    max_sundays = model.NewIntVar(0, len(sundays), "max_sundays")
    min_sundays = model.NewIntVar(0, len(sundays), "min_sundays")

    model.AddMaxEquality(max_sundays, list_of_sundays_per_worker)
    model.AddMinEquality(min_sundays, list_of_sundays_per_worker)

    sunday_diff = model.NewIntVar(0, len(sundays), "sunday_diff")
    model.Add(sunday_diff == max_sundays - min_sundays)

    objective_terms.append(sunday_diff*sundays_diff_weight)


    # 4. Balancing number of LQ across the workers 

    list_of_LQs_per_worker = []

    for w in workers:
        LQs = sum(shift[(w, d-1, 'LQ')] for d in sundays if (w, d-1, 'LQ') in shift)
        list_of_LQs_per_worker.append(LQs)


    max_LQs = model.NewIntVar(0, len(sundays), "max_LQs")
    min_LQs = model.NewIntVar(0, len(sundays), "min_LQs")

    model.AddMaxEquality(max_LQs, list_of_LQs_per_worker)
    model.AddMinEquality(min_LQs, list_of_LQs_per_worker)

    LQs_diff = model.NewIntVar(0, len(sundays), "LQs_diff")
    model.Add(LQs_diff == max_LQs - min_LQs)

    objective_terms.append(LQs_diff*LQs_diff_weight)


    # 5. Not consecutive free days error 
      
    list_of_consecutive_free_days_per_worker = []
    list_of_free_days_per_worker = []

    for w in workers:
        consecutive_free_vars = []
        free_day_vars = []

        for d in working_days[w]:
            
            is_free = model.NewBoolVar(f"is_free_{w}_{d}")
            if (w, d, 'L') in shift and (w, d, 'LQ') in shift:
                model.Add(is_free == shift[(w, d, 'L')] + shift[(w, d, 'LQ')])
                free_day_vars.append(is_free)

            
                if d+1 in working_days[w]:
                    next_d = d+1
                    next_is_free = model.NewBoolVar(f"next_is_free_{w}_{next_d}")
                    if (w, d+1, 'L') in shift and (w, d+1, 'LQ') in shift: 
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
    objective_terms.append(objective_consecutive_free_days*no_consec_free_days_weight)


    # 6. Balancing sundays across the year  
    
    parts = np.array_split(days_of_year, 6)
    sunday_parts = [[d for d in part if d in sundays] for part in parts]
    diff_per_worker = []

    for w in workers:
        list_of_free_sundays_per_semester = []

        for idx, part in enumerate(sunday_parts):
            sunday_vars = [shift[(w, d, 'L')] for d in part if (w, d, 'L') in shift]

            if sunday_vars:
                free_sundays_semester = sum(sunday_vars)
            else:
                # Nenhum domingo livre neste semestre → variável constante 0
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

    
      
    
  #  parts = np.array_split(days_of_year, 6) 
  #  sunday_parts=[]
  #  for part in parts:
  #      sunday_part=[d for d in part if d in sundays]
  #      sunday_parts.append(sunday_part)

  #  for w in workers:
  #      list_of_free_sundays_per_semester=[]
  #      for part in sunday_parts:
  #         free_sundays_semester=sum(shift[(w, d, 'L')] for d in part if (w, d, 'L') in shift)
  #          list_of_free_sundays_per_semester.append(free_sundays_semester)
        
  #      max_free_sundays = model.NewIntVar(0, len(sundays), f"max_free_semester_sundays_{w}")
  #      min_free_sundays = model.NewIntVar(0, len(sundays), f"min_free_semester_sundays_{w}") 
        
  #      model.AddMaxEquality(max_free_sundays, list_of_free_sundays_per_semester)
  #      model.AddMinEquality(min_free_sundays, list_of_free_sundays_per_semester)

  #      semester_diff = model.NewIntVar(0, len(sundays), f"semester_diff_{w}")
  #      model.Add(semester_diff == max_free_sundays - min_free_sundays)
        
  #      objective_terms.append(semester_diff*sunday_imbalance_weight)   
            

    # 7. Balancing LQ's across the year

        
    diff_per_worker_LQ = []

    for w in workers:
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


    #for w in workers:
    #    list_of_free_LQs_per_semester=[]
    #    for part in parts:   
    #        LQs_semester = sum(shift[(w, d-1, 'LQ')] for d in part if (w, d-1, 'LQ') in shift)
    #        list_of_free_LQs_per_semester.append(LQs_semester)    
             
    #    max_free_LQs = model.NewIntVar(0, len(sundays), f"max_free_semester_LQs_{w}")
    #    min_free_LQs = model.NewIntVar(0, len(sundays), f"min_free_semester_LQs_{w}") 
            
    #    model.AddMaxEquality(max_free_LQs, list_of_free_LQs_per_semester)
    #    model.AddMinEquality(min_free_LQs, list_of_free_LQs_per_semester)
            
    #    semester_diff = model.NewIntVar(0, len(sundays), f"semester_diff_{w}")
    #    model.Add(semester_diff == max_free_LQs - min_free_LQs)
        
    #   objective_terms.append(semester_diff*LQ_imbalance_weight)          
        
    # 8. Weeks of inconsistent shifts error
    
    
    for w in workers:
        inconsistent_weeks = []

        for week, days in week_to_days:

            shift_M = model.NewBoolVar(f"shift_M_{w}_{week}")
            M_vars = [shift[(w, d, 'M')] for d in days if (w, d, 'M') in shift]

            if M_vars:
                model.AddBoolOr(M_vars).OnlyEnforceIf(shift_M)
                model.AddBoolAnd([v.Not() for v in M_vars]).OnlyEnforceIf(shift_M.Not())
            else:
                model.Add(shift_M == 0)
                    
                    
            shift_T = model.NewBoolVar(f"shift_T_{w}_{week}")
            T_vars = [shift[(w, d, 'T')] for d in days if (w, d, 'T') in shift]
                    
            if T_vars:
                model.AddBoolOr(T_vars).OnlyEnforceIf(shift_T)
                model.AddBoolAnd([v.Not() for v in T_vars]).OnlyEnforceIf(shift_T.Not())
            else:
                model.Add(shift_T == 0)
                            
                            
            is_inconsistent = model.NewBoolVar(f"is_inconsistent_{w}_{week}")
            model.AddBoolAnd([shift_M, shift_T]).OnlyEnforceIf(is_inconsistent)
            model.AddBoolOr([shift_M.Not(), shift_T.Not()]).OnlyEnforceIf(is_inconsistent.Not())
                            
            inconsistent_weeks.append(is_inconsistent)
                            
    objective_terms.append(sum(inconsistent_weeks) * inconsistent_number_of_weeks_weight)


  #  for w in workers:
  #      inconsistent_weeks=[]
  #      for week, days in week_to_days:
  #          
  #          shift_M=model.NewBoolVar(f"shift_M_{w}_{week}")
  #          model.AddBoolOr([shift[(w, d, 'M')] for d in days if (w, d, 'M') in shift ]).OnlyEnforceIf(shift_M)
  #          model.AddBoolAnd([shift[(w, d, 'M')].Not() for d in days if (w, d, 'M') in shift]).OnlyEnforceIf(shift_M.Not())
            
  #          shift_T=model.NewBoolVar(f"shift_T_{w}_{week}")
  #          model.AddBoolOr([shift[(w, d, 'T')] for d in days if (w, d, 'T') in shift]).OnlyEnforceIf(shift_T)
  #          model.AddBoolAnd([shift[(w, d, 'T')].Not() for d in days if (w, d, 'T') in shift]).OnlyEnforceIf(shift_T.Not())
           
  #         is_inconsistent=model.NewBoolVar(f"is_inconsistent_{w}_{week}")
  #         model.AddBoolAnd([shift_M, shift_T]).OnlyEnforceIf(is_inconsistent)
  #          model.AddBoolOr([shift_M.Not(), shift_T.Not()]).OnlyEnforceIf(is_inconsistent.Not())

#            inconsistent_weeks.append(is_inconsistent)
            
            
 #       objective_terms.append(sum(inconsistent_weeks)*inconsistent_number_of_weeks_weight)   
            

    # 9. No managers/keyholders error

    workers_with_key=[w for w in workers if role_by_worker.get(w, "normal") in ["manager", "keyholder"]]
    list_shifts_no_keys=[]
    for d in days_of_year:
        for s in real_working_shift:
            if pessObj.get((d, s), 0)>0:
                no_key=model.NewBoolVar(f"no_key_{d}_{s}")
                model.AddBoolAnd([shift[(w, d, s)].Not() for w in workers_with_key if (w, d, s) in shift]).OnlyEnforceIf(no_key)
                model.AddBoolOr([shift[(w, d, s)] for w in workers_with_key if (w, d, s) in shift]).OnlyEnforceIf(no_key.Not())
                list_shifts_no_keys.append(no_key)

    objective_terms.append(sum(list_shifts_no_keys)*no_key_weight)   
        
    # 10. Same free day assigned to managers/keyholders error      

    managers=[w for w in workers if role_by_worker.get(w, "normal") =="manager"]
    keyholders=[w for w in workers if role_by_worker.get(w, "normal") =="keyholder"]
    workers_with_key=managers+keyholders 

    for d in days_of_year:
        for s in real_working_shift:
            if pessObj.get((d, s), 0)>0:
                free_manager=model.NewIntVar(0, len(managers), f"free_managers_{d}_{s}")
                model.Add(free_manager==sum([shift[(w, d, s)].Not() for w in managers if (w, d, s) in shift]))
                free_keyholders=model.NewIntVar(0, len(keyholders), f"free_keyholders_{d}_{s}")
                model.Add(free_keyholders==sum([shift[(w, d, s)].Not() for w in keyholders if (w, d, s) in shift]))
                #free_keys=model.NewIntVar(0, len(workers_with_key), f"free_keys_{d}_{s}")
                #model.Add(free_keys==sum([shift[(w, d, s)].Not() for w in workers_with_key]))
                objective_terms.append(free_manager*same_free_day_manager_weight)   
                objective_terms.append(free_keyholders*same_free_day_keyholders_weight)   
    
    # 11. Worst day excess deficit error
    
    excess_day_diff_vars = []
    deficit_day_diff_vars = []

    for d in days_of_year:  
        for s in real_working_shift:
            target = pessObj.get((d, s), 0)
            assigned_workers = sum(shift[(w, d, s)] for w in workers if (w, d, s) in shift)
            
            excess = model.NewIntVar(0, len(workers), f'excess_{d}_{s}')
            deficit = model.NewIntVar(0, target, f'deficit_{d}_{s}')
                
            model.Add(excess >= assigned_workers - target)
            model.Add(deficit >= target - assigned_workers)
            
            excess_day_diff_vars.append(excess)
            deficit_day_diff_vars.append(deficit)
            
    max_excess_shift = model.NewIntVar(0, len(workers), "max_excess_shift")
    model.AddMaxEquality(max_excess_shift, excess_day_diff_vars)

    max_deficit_shift = model.NewIntVar(0, target, "max_deficit_shift")
    model.AddMaxEquality(max_deficit_shift, deficit_day_diff_vars)

    objective_terms.append(max_excess_shift * max_excess_weight)
    objective_terms.append(max_deficit_shift * max_deficit_weight)

    
    model.Minimize(sum(objective_terms))


