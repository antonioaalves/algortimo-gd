def optimization_prediction(model,days_of_year, workers, working_shift, shift, pessObj, min_workers, closed_holidays, week_to_days,  working_days, contract_type):
    # Store the pos_diff and neg_diff variables for later access
    pos_diff_dict = {}
    neg_diff_dict = {}
    no_workers_penalties = {}
    min_workers_penalties = {}
    inconsistent_shift_penalties = {}

    # Create the objective function with heavy penalties
    objective_terms = []

    # Penalty weights
    HEAVY_PENALTY = 10  # Penalty for days with no workers
    MIN_WORKER_PENALTY = 5  # Penalty for breaking minimum worker requirements
    INCONSISTENT_SHIFT_PENALTY = 3  # Penalty for inconsistent shift types
    ADJACENT_FREE_SHIFTS_PENALTY = 5  # Adjust this value as needed





    # 1. Penalize deviations from pessObj
    for d in days_of_year:
        for s in working_shift:
            # Calculate the number of assigned workers for this day and shift
            assigned_workers = sum(shift[(w, d, s)] for w in workers)
            
            # Create variables to represent the positive and negative deviations from the target
            pos_diff = model.NewIntVar(0, len(workers), f"pos_diff_{d}_{s}")
            neg_diff = model.NewIntVar(0, len(workers), f"neg_diff_{d}_{s}")
            
            # Store the variables in dictionaries
            pos_diff_dict[(d, s)] = pos_diff
            neg_diff_dict[(d, s)] = neg_diff
            
            # Add constraints to ensure that the positive and negative deviations are correctly computed
            model.Add(pos_diff >= assigned_workers - pessObj.get((d, s), 0))  # If excess, pos_diff > 0
            model.Add(pos_diff >= 0)  # Ensure pos_diff is non-negative
            
            model.Add(neg_diff >= pessObj.get((d, s), 0) - assigned_workers)  # If shortfall, neg_diff > 0
            model.Add(neg_diff >= 0)  # Ensure neg_diff is non-negative
            
            # Add both positive and negative deviations to the objective function
            objective_terms.append(pos_diff)
            objective_terms.append(neg_diff)

    # 2. Heavily penalize days with no workers when pessObj != 0
    for d in days_of_year:
        if d not in closed_holidays:  # Skip closed holidays
            for s in working_shift:
                if pessObj.get((d, s), 0) > 0:  # Only penalize when pessObj exists
                    # Calculate the number of assigned workers for this day and shift
                    assigned_workers = sum(shift[(w, d, s)] for w in workers)
                    
                    # Create a boolean variable to indicate if there are no workers
                    no_workers = model.NewBoolVar(f"no_workers_{d}_{s}")
                    model.Add(assigned_workers == 0).OnlyEnforceIf(no_workers)
                    model.Add(assigned_workers >= 1).OnlyEnforceIf(no_workers.Not())
                    
                    # Store the variable
                    no_workers_penalties[(d, s)] = no_workers
                    
                    # Add a heavy penalty to the objective function
                    objective_terms.append(HEAVY_PENALTY * no_workers)

    # 3. Penalize breaking minimum worker requirements
    for d in days_of_year:
        for s in working_shift:
            min_req = min_workers.get((d, s), 0)
            if min_req > 0:  # Only penalize when there's a minimum requirement
                # Calculate the number of assigned workers for this day and shift
                assigned_workers = sum(shift[(w, d, s)] for w in workers)
                
                # Create a variable to represent the shortfall from the minimum
                shortfall = model.NewIntVar(0, min_req, f"min_shortfall_{d}_{s}")
                model.Add(shortfall >= min_req - assigned_workers)
                model.Add(shortfall >= 0)
                
                # Store the variable
                min_workers_penalties[(d, s)] = shortfall
                
                # Add penalty to the objective function
                objective_terms.append(MIN_WORKER_PENALTY * shortfall)

    # 4. Penalize inconsistent shift types within a week for each worker
    for w in workers:
        for week in range(1, 53):  # Iterate over all weeks
            days_in_week = week_to_days[week]
            working_days_in_week = [d for d in days_in_week if d in working_days[w]]
     
            if len(working_days_in_week) >= 2:  # Only if worker has at least 2 working days this week
                # Create variables to track if the worker has M or T shifts this week
                has_m_shift = model.NewBoolVar(f"has_m_shift_{w}_{week}")
                has_t_shift = model.NewBoolVar(f"has_t_shift_{w}_{week}")
                
                # Create expressions for total M and T shifts this week
                total_m = sum(shift.get((w, d, "M"), 0) for d in working_days_in_week)
                total_t = sum(shift.get((w, d, "T"), 0) for d in working_days_in_week)
                
                # Worker has M shifts if total_m > 0
                model.Add(total_m >= 1).OnlyEnforceIf(has_m_shift)
                model.Add(total_m == 0).OnlyEnforceIf(has_m_shift.Not())
                
                # Worker has T shifts if total_t > 0
                model.Add(total_t >= 1).OnlyEnforceIf(has_t_shift)
                model.Add(total_t == 0).OnlyEnforceIf(has_t_shift.Not())
                
                # Create a variable to indicate inconsistent shifts
                inconsistent_shifts = model.NewBoolVar(f"inconsistent_shifts_{w}_{week}")
                
                # Worker has inconsistent shifts if both M and T shifts exist
                model.AddBoolAnd([has_m_shift, has_t_shift]).OnlyEnforceIf(inconsistent_shifts)
                model.AddBoolOr([has_m_shift.Not(), has_t_shift.Not()]).OnlyEnforceIf(inconsistent_shifts.Not())
            
                # Store the variable
                inconsistent_shift_penalties[(w, week)] = inconsistent_shifts
                
                # Add penalty to the objective function
                objective_terms.append(INCONSISTENT_SHIFT_PENALTY * inconsistent_shifts)

    # 5. Reward L shifts (bonus points for each L shift assigned)
    # Penalty weight for having free day shifts next to each other for contract type 4 workers

    for w in workers:
        # Only apply to workers with contract type 4
        if contract_type[w] == 4:
            for d in working_days[w]:
                # Check if this day and the next day are both working days
                if d + 1 in working_days[w]:
                    # Create binary variables to detect free day shifts
                    has_free_shift_today = model.NewBoolVar(f"has_free_shift_today_{w}_{d}")
                    has_free_shift_tomorrow = model.NewBoolVar(f"has_free_shift_tomorrow_{w}_{d+1}")
                    
                    # Detect if the worker has any free day shift (L, LD, or LQ) today
                    free_shift_today_constraints = []
                    for shift_type in ["L", "LD", "LQ"]:
                        if (w, d, shift_type) in shift:
                            shift_var = shift[(w, d, shift_type)]
                            shift_bool = model.NewBoolVar(f"has_{shift_type}_{w}_{d}")
                            model.Add(shift_var >= 1).OnlyEnforceIf(shift_bool)
                            model.Add(shift_var == 0).OnlyEnforceIf(shift_bool.Not())
                            free_shift_today_constraints.append(shift_bool)
                    
                    # Worker has a free shift today if any of L, LD, or LQ is assigned
                    if free_shift_today_constraints:
                        model.AddBoolOr(free_shift_today_constraints).OnlyEnforceIf(has_free_shift_today)
                        model.AddBoolAnd([constraint.Not() for constraint in free_shift_today_constraints]).OnlyEnforceIf(has_free_shift_today.Not())
                    else:
                        model.Add(has_free_shift_today == 0)
                    
                    # Detect if the worker has any free day shift (L, LD, or LQ) tomorrow
                    free_shift_tomorrow_constraints = []
                    for shift_type in ["L", "LD", "LQ"]:
                        if (w, d+1, shift_type) in shift:
                            shift_var = shift[(w, d+1, shift_type)]
                            shift_bool = model.NewBoolVar(f"has_{shift_type}_{w}_{d+1}")
                            model.Add(shift_var >= 1).OnlyEnforceIf(shift_bool)
                            model.Add(shift_var == 0).OnlyEnforceIf(shift_bool.Not())
                            free_shift_tomorrow_constraints.append(shift_bool)
                    
                    # Worker has a free shift tomorrow if any of L, LD, or LQ is assigned
                    if free_shift_tomorrow_constraints:
                        model.AddBoolOr(free_shift_tomorrow_constraints).OnlyEnforceIf(has_free_shift_tomorrow)
                        model.AddBoolAnd([constraint.Not() for constraint in free_shift_tomorrow_constraints]).OnlyEnforceIf(has_free_shift_tomorrow.Not())
                    else:
                        model.Add(has_free_shift_tomorrow == 0)
                    
                    # Create a binary variable to detect if free shifts occur on adjacent days
                    adjacent_free_shifts = model.NewBoolVar(f"adjacent_free_shifts_{w}_{d}")
                    model.AddBoolAnd([has_free_shift_today, has_free_shift_tomorrow]).OnlyEnforceIf(adjacent_free_shifts)
                    model.AddBoolOr([has_free_shift_today.Not(), has_free_shift_tomorrow.Not()]).OnlyEnforceIf(adjacent_free_shifts.Not())
                    
                    # Add a penalty term to the objective for adjacent free shifts
                    # Since we're minimizing, this will discourage adjacent free shifts
                    objective_terms.append(ADJACENT_FREE_SHIFTS_PENALTY * adjacent_free_shifts)




    model.Minimize(sum(objective_terms))