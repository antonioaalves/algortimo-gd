def salsa_optimization(model, days_of_year, workers, working_shift, shift, pessObj, working_days, closed_holidays, min_workers, week_to_days):
    # Store the pos_diff and neg_diff variables for later access
    pos_diff_dict = {}
    neg_diff_dict = {}
    no_workers_penalties = {}
    min_workers_penalties = {}
    inconsistent_shift_penalties = {}

    # Create the objective function with heavy penalties
    objective_terms = []
    HEAVY_PENALTY = 300  # Penalty for days with no workers
    MIN_WORKER_PENALTY = 60  # Penalty for breaking minimum worker requirements
    INCONSISTENT_SHIFT_PENALTY = 3  # Penalty for inconsistent shift types



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
            objective_terms.append(30 * pos_diff)
            objective_terms.append(30 * neg_diff)

    # 2. NEW: Reward consecutive free days
    consecutive_free_day_bonus = []
    for w in workers:
        all_work_days = sorted(working_days[w])
        
        # Create boolean variables for each day indicating if it's a free day
        free_day_vars = {}
        for d in all_work_days:
            free_day = model.NewBoolVar(f"free_day_{w}_{d}")
            
            # Sum the L, F, LQ, A, V shifts for this day
            free_shift_sum = sum(
                shift.get((w, d, shift_type), 0) 
                for shift_type in ["L", "F", "LQ", "A", "V"]
            )
            
            # Link the boolean variable to whether any free shift is assigned
            model.Add(free_shift_sum >= 1).OnlyEnforceIf(free_day)
            model.Add(free_shift_sum == 0).OnlyEnforceIf(free_day.Not())
            
            free_day_vars[d] = free_day
        
        # For each pair of consecutive days in the worker's schedule
        for i in range(len(all_work_days) - 1):
            day1 = all_work_days[i]
            day2 = all_work_days[i+1]
            
            # Only consider consecutive calendar days
            if day2 == day1 + 1:
                # Create a boolean variable for consecutive free days
                consecutive_free = model.NewBoolVar(f"consecutive_free_{w}_{day1}_{day2}")
                
                # Both days must be free for the bonus to apply
                model.AddBoolAnd([free_day_vars[day1], free_day_vars[day2]]).OnlyEnforceIf(consecutive_free)
                model.AddBoolOr([free_day_vars[day1].Not(), free_day_vars[day2].Not()]).OnlyEnforceIf(consecutive_free.Not())
                
                # Add a negative term (bonus) to the objective function for each consecutive free day pair
                consecutive_free_day_bonus.append(consecutive_free)

    # Add the bonus term to the objective with appropriate weight (negative to minimize)
    # Using a weight of -1 to prioritize consecutive free days
    objective_terms.extend([-1 * term for term in consecutive_free_day_bonus])
    
    #3. No workers in a day penalty
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

    # 4. Penalize breaking minimum worker requirements
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

    # Balance free days (L shifts) and quality weekends (LQ shifts) across all workers
    worker_balance_penalties = []

    # 5. Calculate the total L and LQ shifts for each worker
    L_counts = {}
    LQ_counts = {}
    for w in workers:
        # Sum all L shifts for this worker
        L_counts[w] = sum(shift.get((w, d, "L"), 0) for d in working_days[w])
        
        # Sum all LQ shifts for this worker
        LQ_counts[w] = sum(shift.get((w, d, "LQ"), 0) for d in working_days[w])

    # Create penalties for imbalances between workers
    # For each pair of workers
    for w1 in workers:
        for w2 in workers:
            if w1 < w2:  # Avoid counting each pair twice
                # Calculate and penalize differences in L shifts (free days)
                L_diff = model.NewIntVar(-len(days_of_year), len(days_of_year), f"L_diff_{w1}_{w2}")
                L_abs_diff = model.NewIntVar(0, len(days_of_year), f"L_abs_diff_{w1}_{w2}")
                
                # Set L shift difference constraint
                model.Add(L_diff == L_counts[w1] - L_counts[w2])
                model.AddAbsEquality(L_abs_diff, L_diff)
                
                # Calculate and penalize differences in LQ shifts (part of quality weekends)
                LQ_diff = model.NewIntVar(-len(days_of_year), len(days_of_year), f"LQ_diff_{w1}_{w2}")
                LQ_abs_diff = model.NewIntVar(0, len(days_of_year), f"LQ_abs_diff_{w1}_{w2}")
                
                # Set LQ shift difference constraint
                model.Add(LQ_diff == LQ_counts[w1] - LQ_counts[w2])
                model.AddAbsEquality(LQ_abs_diff, LQ_diff)
                
                # Add higher weight to LQ differences to prioritize balancing quality weekends
                worker_balance_penalties.append(15 * L_abs_diff)  # Free days balance weight
                worker_balance_penalties.append(25 * LQ_abs_diff)  # Quality weekends balance weight (higher priority)

    

    # Add all balance penalties to the objective function
    objective_terms.extend(worker_balance_penalties)

    # 6. Penalize inconsistent shift types within a week for each worker
    for w in workers:
        for week in range(1, 53):  # Iterate over all weeks
            days_in_week = week_to_days[week]
            working_days_in_week = [d for d in days_in_week if d in working_days.get(w, [])]
            
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

    model.Minimize(sum(objective_terms))