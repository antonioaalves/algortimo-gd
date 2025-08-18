def salsa_optimization(model, days_of_year, workers, working_shift, shift, pessObj, working_days, closed_holidays, min_workers, week_to_days, sundays, c2d):
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
            assigned_workers = sum(shift[(w, d, s)] for w in workers if (w, d, s) in shift)
            
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
            objective_terms.append(1000 * pos_diff)
            objective_terms.append(1000 * neg_diff)

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
                    assigned_workers = sum(shift[(w, d, s)] for w in workers if (w, d, s) in shift)
                    
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
                assigned_workers = sum(shift[(w, d, s)] for w in workers if (w, d, s) in shift)
                
                # Create a variable to represent the shortfall from the minimum
                shortfall = model.NewIntVar(0, min_req, f"min_shortfall_{d}_{s}")
                model.Add(shortfall >= min_req - assigned_workers)
                model.Add(shortfall >= 0)
                
                # Store the variable
                min_workers_penalties[(d, s)] = shortfall
                
                # Add penalty to the objective function
                objective_terms.append(MIN_WORKER_PENALTY * shortfall)



       # 5.1 Balance sundays free days 
        SUNDAY_BALANCE_PENALTY = 1  # Weight for Sunday balance penalty
        sunday_balance_penalties = []
        
        
        for w in workers:
            worker_sundays = [d for d in sundays if d in working_days[w]]
            
            if len(worker_sundays) <= 1:
                continue  # Skip if worker has 0 or 1 Sunday (no balancing needed)
            
            # Create variables for Sunday free days (L shifts)
            sunday_free_vars = []
            for sunday in worker_sundays:
                sunday_free = model.NewBoolVar(f"sunday_free_{w}_{sunday}")
                
                # Link to actual L shift assignment
                model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) >= 1).OnlyEnforceIf(sunday_free)
                model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) == 0).OnlyEnforceIf(sunday_free.Not())

                sunday_free_vars.append(sunday_free)
            
            # Calculate target spacing between Sunday free days
            total_sunday_free = len(sunday_free_vars)
            
            # For even distribution, we want to minimize variance in spacing
            # We'll divide the year into segments and try to have roughly equal distribution
            num_segments = min(5, total_sunday_free)  # Use 5 segments or fewer if not enough Sundays

            if num_segments > 1:
                segment_size = total_sunday_free // num_segments

                for segment in range(num_segments):
                    start_idx = segment * segment_size
                    end_idx = (segment + 1) * segment_size if segment < num_segments - 1 else total_sunday_free
                    
                    segment_sundays = sunday_free_vars[start_idx:end_idx]
                    
                    if len(segment_sundays) > 0:
                        # Create variables for deviation from ideal distribution
                        segment_free_count = sum(segment_sundays)
                        
                        # Handle remainder when total doesn't divide evenly
                        base_ideal = total_sunday_free // num_segments
                        remainder = total_sunday_free % num_segments
                        # First 'remainder' segments get one extra
                        ideal_count = base_ideal + (1 if segment < remainder else 0)
                        
                        # Maximum possible deviation bounds
                        max_over = len(segment_sundays)  # All Sundays in segment could be free
                        max_under = ideal_count  # Could have 0 instead of ideal_count
                        
                        # Create penalty variables for over/under allocation
                        over_penalty = model.NewIntVar(0, max_over, f"sunday_over_{w}_{segment}")
                        under_penalty = model.NewIntVar(0, max_under, f"sunday_under_{w}_{segment}")
                        
                        # Correctly calculate deviations (handling negative cases)
                        model.Add(over_penalty >= segment_free_count - ideal_count)
                        model.Add(over_penalty >= 0)  # Ensure non-negative
                        
                        model.Add(under_penalty >= ideal_count - segment_free_count)
                        model.Add(under_penalty >= 0)  # Ensure non-negative
                        
                        sunday_balance_penalties.append(SUNDAY_BALANCE_PENALTY * over_penalty)
                        sunday_balance_penalties.append(SUNDAY_BALANCE_PENALTY * under_penalty)
        
        objective_terms.extend(sunday_balance_penalties)



        # 5.2 Balance c2d free days
        C2D_BALANCE_PENALTY = 8  # Weight for c2d balance penalty
        c2d_balance_penalties = []

        
        for w in workers:
            # Find all potential quality weekends (Saturday-Sunday pairs)
            quality_weekend_vars = []
            weekend_dates = []
            
            for sunday in sundays:
                saturday = sunday - 1
                
                # Check if both Saturday and Sunday are in worker's schedule
                if saturday in working_days[w] and sunday in working_days[w]:
                    # Create boolean for this quality weekend
                    quality_weekend = model.NewBoolVar(f"quality_weekend_{w}_{sunday}")
                    
                    # Quality weekend is True if LQ on Saturday AND L on Sunday
                    has_lq_saturday = model.NewBoolVar(f"has_lq_sat_{w}_{saturday}")
                    has_l_sunday = model.NewBoolVar(f"has_l_sun_{w}_{sunday}")
                    
                    # Link to actual shift assignments
                    model.Add(shift.get((w, saturday, "LQ"), 0) >= 1).OnlyEnforceIf(has_lq_saturday)
                    model.Add(shift.get((w, saturday, "LQ"), 0) == 0).OnlyEnforceIf(has_lq_saturday.Not())
                    
                    model.Add(shift.get((w, sunday, "L"), 0) >= 1).OnlyEnforceIf(has_l_sunday)
                    model.Add(shift.get((w, sunday, "L"), 0) == 0).OnlyEnforceIf(has_l_sunday.Not())
                    
                    # Quality weekend requires both conditions
                    model.AddBoolAnd([has_lq_saturday, has_l_sunday]).OnlyEnforceIf(quality_weekend)
                    model.AddBoolOr([has_lq_saturday.Not(), has_l_sunday.Not()]).OnlyEnforceIf(quality_weekend.Not())
                    
                    quality_weekend_vars.append(quality_weekend)
                    weekend_dates.append(sunday)
            
            if len(quality_weekend_vars) <= 1:
                continue  # Skip if worker has 0 or 1 potential quality weekend
            
            # Divide the year into segments and try to distribute quality weekends evenly
            num_segments = min(5, len(quality_weekend_vars))  # Use 5 segments or fewer if not enough weekends

            if num_segments > 1:
                segment_size = len(quality_weekend_vars) // num_segments
                
                for segment in range(num_segments):
                    start_idx = segment * segment_size
                    end_idx = (segment + 1) * segment_size if segment < num_segments - 1 else len(quality_weekend_vars)
                    
                    segment_weekends = quality_weekend_vars[start_idx:end_idx]
                    
                    if len(segment_weekends) > 0:
                        segment_count = sum(segment_weekends)

                        max_possible_quality = c2d.get(w,0)  # from your business logic
                        base_ideal = max_possible_quality // num_segments
                        remainder = max_possible_quality % num_segments
                        ideal_count = base_ideal + (1 if segment < remainder else 0)
                        
                        # Maximum possible deviation bounds
                        max_over = len(segment_weekends)  # All weekends in segment could be quality
                        max_under = ideal_count  # Could have 0 instead of ideal_count
                        
                        # Create penalty variables for deviation from ideal distribution
                        over_penalty = model.NewIntVar(0, max_over, f"c2d_over_{w}_{segment}")
                        under_penalty = model.NewIntVar(0, max_under, f"c2d_under_{w}_{segment}")
                        
                        # Correctly calculate deviations (handling negative cases)
                        model.Add(over_penalty >= segment_count - ideal_count)
                        model.Add(over_penalty >= 0)  # Ensure non-negative
                        
                        model.Add(under_penalty >= ideal_count - segment_count)
                        model.Add(under_penalty >= 0)  # Ensure non-negative
                        
                        c2d_balance_penalties.append(C2D_BALANCE_PENALTY * over_penalty)
                        c2d_balance_penalties.append(C2D_BALANCE_PENALTY * under_penalty)
        



    objective_terms.extend(c2d_balance_penalties)

    # 6. Penalize inconsistent shift types within a week for each worker
    for w in workers:
        for week in week_to_days.keys():  # Iterate over all weeks
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
