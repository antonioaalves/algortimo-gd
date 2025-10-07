from base_data_project.log_config import get_logger
from src.configuration_manager.manager import ConfigurationManager

_config_manager = ConfigurationManager()
logger = get_logger(_config_manager.project_name)


def salsa_esp_optimization(model, days_of_year, workers, working_shift, shift, pessObj, working_days, closed_holidays, min_workers, week_to_days, sundays, c2d, first_day, last_day, role_by_worker, work_day_hours): #role_by_worker):
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
    hours_scale = 8


    # 1. Penalize deviations from pessObj
    day_counter = 0
    for d in days_of_year:
        for s in working_shift:
            # Calculate the number of assigned workers for this day and shift
            assigned_workers = sum(shift[(w, d, s)] * work_day_hours[w][day_counter] for w in workers if (w, d, s) in shift)
            
            # Create variables to represent the positive and negative deviations from the target
            pos_diff = model.NewIntVar(0, len(workers) * hours_scale, f"pos_diff_{d}_{s}")
            neg_diff = model.NewIntVar(0, len(workers) * hours_scale, f"neg_diff_{d}_{s}")
            
            # Store the variables in dictionaries
            pos_diff_dict[(d, s)] = pos_diff
            neg_diff_dict[(d, s)] = neg_diff

            target = pessObj.get((d, s), 0)
            
            # Add constraints to ensure that the positive and negative deviations are correctly computed
            model.Add(pos_diff >= assigned_workers - target)  # If excess, pos_diff > 0
            model.Add(pos_diff >= 0)  # Ensure pos_diff is non-negative
            
            model.Add(neg_diff >= target - assigned_workers)  # If shortfall, neg_diff > 0
            model.Add(neg_diff >= 0)  # Ensure neg_diff is non-negative
            
            # Add both positive and negative deviations to the objective function
            objective_terms.append(1000 * pos_diff)
            objective_terms.append(1000 * neg_diff)
        day_counter += 1


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
    day_counter = 0
    for d in days_of_year:
        for s in working_shift:
            min_req = min_workers.get((d, s), 0)
            if min_req > 0:  # Only penalize when there's a minimum requirement
                # Calculate the number of assigned workers for this day and shift
                assigned_workers = sum(shift[(w, d, s)] * work_day_hours[w][day_counter] for w in workers if (w, d, s) in shift)
                
                # Create a variable to represent the shortfall from the minimum
                shortfall = model.NewIntVar(0, min_req, f"min_shortfall_{d}_{s}")
                model.Add(shortfall >= min_req - assigned_workers)
                model.Add(shortfall >= 0)
                
                # Store the variable
                min_workers_penalties[(d, s)] = shortfall
                
                # Add penalty to the objective function
                objective_terms.append(MIN_WORKER_PENALTY * shortfall)
        day_counter += 1


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

        quality_weekend_2_dict = {}

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

                    quality_weekend_2_dict[(w, sunday)] = quality_weekend
                    
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

   # 7 Balancing number of sundays free days across the workers (CORRECTED STRATEGY)
    # SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY = 50
    # sunday_balance_across_workers_penalties = []

    # # Create constraint variables for each worker's total Sunday free days
    # sunday_free_worker_vars = {}
    # workers_with_sundays = []

    # # SCALE_FACTOR for converting float proportions to integers
    # # SCALE_FACTOR = 1000

    # for w in workers:
    #     worker_sundays = [d for d in sundays if d in working_days[w]]
        
    #     if len(worker_sundays) == 0:
    #         continue  # Skip workers with no Sundays
        
    #     workers_with_sundays.append(w)
        
    #     # Create variables for Sunday free days
    #     sunday_free_vars = []
    #     for sunday in worker_sundays:
    #         sunday_free = model.NewBoolVar(f"sunday_free_{w}_{sunday}")
            
    #         # Link to actual L or F shift assignment
    #         model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) >= 1).OnlyEnforceIf(sunday_free)
    #         model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) == 0).OnlyEnforceIf(sunday_free.Not())
            
    #         sunday_free_vars.append(sunday_free)
        
    #     # Create constraint variable for total Sunday free days
    #     total_sunday_free_var = model.NewIntVar(0, len(worker_sundays), f"total_sunday_free_{w}")
    #     model.Add(total_sunday_free_var == sum(sunday_free_vars))
        
    #     sunday_free_worker_vars[w] = total_sunday_free_var

    # if len(workers_with_sundays) > 1:
    #     # STRATEGY 1: Create a shared pool of total Sunday free days and distribute proportionally
        
    #     # Calculate total actual Sunday free days across all workers (constraint variable)
    #     max_total_possible = sum(len([d for d in sundays if d in working_days[w]]) for w in workers_with_sundays)
    #     total_sunday_free_all = model.NewIntVar(0, max_total_possible, "total_sunday_free_all")
    #     model.Add(total_sunday_free_all == sum(sunday_free_worker_vars[w] for w in workers_with_sundays))
        
    #     # Create scaled variables for each worker and calculate expected distribution
    #     for w in workers_with_sundays:
    #         reverse_proportion = 1.0 / proportion.get(w, 1.0)
    #         #reverse_proportion_scaled = int(reverse_proportion * SCALE_FACTOR)
            
    #         # Maximum possible Sunday free days for this worker
    #         max_worker_sundays = len([d for d in sundays if d in working_days[w]])
    #         #max_scaled_value = max_worker_sundays * reverse_proportion_scaled
            
    #         # Create scaled variable: scaled_sunday_free = sunday_free_worker_vars[w] * reverse_proportion
    #         scaled_sunday_free_var = model.NewIntVar(0, max_worker_sundays, f"max_sunday_free_{w}")
    #         model.Add(scaled_sunday_free_var == int(sunday_free_worker_vars[w] * reverse_proportion))

    #         # Calculate total scaled allocation across all workers
    #         if w == workers_with_sundays[0]:  # Initialize on first worker
    #             total_scaled_allocation = model.NewIntVar(0, sum(max_worker_sundays for w in workers_with_sundays), "total_allocation")
    #             model.Add(total_scaled_allocation == sum(
    #                 int(sunday_free_worker_vars[worker] * 1 / proportion.get(worker, 1.0))
    #                 for worker in workers_with_sundays
    #             ))
            
    #         # Expected value for this worker = total_scaled_allocation / num_workers
    #         expected_scaled_var = model.NewIntVar(0, max_worker_sundays, f"expected_scaled_{w}")
            
    #         # Use constraint-based division: expected_scaled_var * num_workers ≈ total_scaled_allocation
    #         num_workers = len(workers_with_sundays)
    #         #tolerance = SCALE_FACTOR  # Allow some tolerance in division
            
            
    #         model.Add(expected_scaled_var * num_workers * reverse_proportion >= total_scaled_allocation )
    #         model.Add(expected_scaled_var * num_workers * reverse_proportion <= total_scaled_allocation )
            
    #         # Calculate deviations from expected value
    #         max_deviation = max_scaled_value
    #         over_target = model.NewIntVar(0, max_deviation, f"sunday_over_target_{w}")
    #         under_target = model.NewIntVar(0, max_deviation, f"sunday_under_target_{w}")
            
    #         # Deviation calculation: |scaled_sunday_free_var - expected_scaled_var|
    #         model.Add(over_target >= scaled_sunday_free_var - expected_scaled_var)
    #         model.Add(over_target >= 0)
            
    #         model.Add(under_target >= expected_scaled_var - scaled_sunday_free_var)
    #         model.Add(under_target >= 0)
            
    #         # Add penalties
    #         sunday_balance_across_workers_penalties.append(SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY * over_target)
    #         sunday_balance_across_workers_penalties.append(SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY * under_target)

    # ALTERNATIVE STRATEGY 2: Simpler pairwise balance approach
    # This strategy directly compares workers pairwise with proportional adjustments
    
    # 7 Balancing number of sundays free days across the workers (SIMPLIFIED - NO SCALE FACTOR)
    SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY = 50
    sunday_balance_across_workers_penalties = []

    # Create constraint variables for each worker's total Sunday free days
    sunday_free_worker_vars = {}
    workers_with_sundays = [] 

    for w in workers:
        worker_sundays = [d for d in sundays if d in working_days[w]]
        
        if len(worker_sundays) == 0:
            continue  # Skip workers with no Sundays
        
        workers_with_sundays.append(w)
        
        # Create variables for Sunday free days
        sunday_free_vars = []
        for sunday in worker_sundays:
            sunday_free = model.NewBoolVar(f"sunday_free_{w}_{sunday}")
            
            # Link to actual L or F shift assignment
            model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) >= 1).OnlyEnforceIf(sunday_free)
            model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) == 0).OnlyEnforceIf(sunday_free.Not())
            
            sunday_free_vars.append(sunday_free)
        
        # Create constraint variable for total Sunday free days
        total_sunday_free_var = model.NewIntVar(0, len(worker_sundays), f"total_sunday_free_{w}")
        model.Add(total_sunday_free_var == sum(sunday_free_vars))
        
        sunday_free_worker_vars[w] = total_sunday_free_var

    # STRATEGY: Pairwise proportional balance (simplest and most reliable)
    if len(workers_with_sundays) > 1:
        # For each pair of workers, ensure proportional fairness
        for i, w1 in enumerate(workers_with_sundays):
            for w2 in workers_with_sundays[i+1:]:
                if last_day.get(w1, 0) == 0 :
                    last_day[w1] = days_of_year[-1]
                if last_day.get(w2, 0) == 0 :
                    last_day[w2] = days_of_year[-1]
                prop1 = (last_day.get(w1, 0) - first_day.get(w1, 0) + 1) / len(days_of_year)
                prop1 = max(0.0, min(1.0, prop1))
                prop2 = (last_day.get(w2, 0) - first_day.get(w2, 0) + 1) / len(days_of_year)
                prop2 = max(0.0, min(1.0, prop2))
                #logger.info(f"Worker {w1} proportion: {prop1}, first day: {first_day.get(w1, 0)}, last day: {last_day.get(w1, 0)}, Worker {w2} proportion: {prop2}, first day: {first_day.get(w2, 0)}, last day: {last_day.get(w2, 0)}")

                if prop1 > 0 and prop2 > 0:
                    # Calculate proportion ratio as integers (multiply by 100 for precision)
                    prop1_int = int(prop1 * 100)
                    prop2_int = int(prop2 * 100)
                    
                    # Calculate maximum possible difference
                    max_sundays_w1 = len([d for d in sundays if d in working_days[w1]])
                    max_sundays_w2 = len([d for d in sundays if d in working_days[w2]])
                    max_diff = max(max_sundays_w1 * prop2_int, max_sundays_w2 * prop1_int)
                    
                    # Create variables for proportional difference
                    proportional_diff_pos = model.NewIntVar(0, max_diff, f"prop_diff_pos_{w1}_{w2}")
                    proportional_diff_neg = model.NewIntVar(0, max_diff, f"prop_diff_neg_{w1}_{w2}")
                    
                    # Proportional balance constraint:
                    # sunday_free_worker_vars[w1] / prop1 should ≈ sunday_free_worker_vars[w2] / prop2
                    # Rearranged: sunday_free_worker_vars[w1] * prop2_int should ≈ sunday_free_worker_vars[w2] * prop1_int
                    
                    model.Add(proportional_diff_pos >= 
                            sunday_free_worker_vars[w1] * prop2_int - sunday_free_worker_vars[w2] * prop1_int)
                    model.Add(proportional_diff_pos >= 0)
                    
                    model.Add(proportional_diff_neg >= 
                            sunday_free_worker_vars[w2] * prop1_int - sunday_free_worker_vars[w1] * prop2_int)
                    model.Add(proportional_diff_neg >= 0)
                    
                    # Add penalties for proportional imbalance
                    weight = SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY // 2  # Distribute penalty across pairs
                    sunday_balance_across_workers_penalties.append(weight * proportional_diff_pos)
                    sunday_balance_across_workers_penalties.append(weight * proportional_diff_neg)

    # Add to objective
    objective_terms.extend(sunday_balance_across_workers_penalties)  
 

    # STRATEGY 3: Variance minimization approach (most sophisticated)
    # This minimizes the variance of scaled Sunday free days across workers

    # if len(workers_with_sundays) > 2:  # Only useful with 3+ workers
    #     # Calculate mean scaled Sunday free days
    #     total_workers = len(workers_with_sundays)
        
    #     # For each worker, calculate their scaled value and deviation from mean
    #     scaled_worker_vars = {}
    #     for w in workers_with_sundays:
    #         reverse_proportion = 1.0 / proportion.get(w, 1.0)
    #         reverse_proportion_scaled = int(reverse_proportion * SCALE_FACTOR)
            
    #         max_worker_sundays = len([d for d in sundays if d in working_days[w]])
    #         max_scaled_value = max_worker_sundays * reverse_proportion_scaled
            
    #         scaled_worker_vars[w] = model.NewIntVar(0, max_scaled_value, f"scaled_worker_{w}")
    #         model.Add(scaled_worker_vars[w] == sunday_free_worker_vars[w] * reverse_proportion_scaled)
        
    #     # Create mean variable
    #     total_scaled = sum(scaled_worker_vars.values())
    #     max_total_scaled = sum(len([d for d in sundays if d in working_days[w]]) * int(SCALE_FACTOR / proportion.get(w, 1.0)) 
    #                         for w in workers_with_sundays)
        
    #     mean_scaled = model.NewIntVar(0, max_total_scaled // total_workers + 1, "mean_scaled")
        
    #     # mean_scaled * total_workers ≈ total_scaled (within tolerance)
    #     tolerance = SCALE_FACTOR
    #     model.Add(mean_scaled * total_workers >= total_scaled - tolerance)
    #     model.Add(mean_scaled * total_workers <= total_scaled + tolerance)
        
    #     # Minimize squared deviations from mean
    #     for w in workers_with_sundays:
    #         max_deviation = max(scaled_worker_vars[w].proto.domain[1], mean_scaled.proto.domain[1])
            
    #         deviation_pos = model.NewIntVar(0, max_deviation, f"deviation_pos_{w}")
    #         deviation_neg = model.NewIntVar(0, max_deviation, f"deviation_neg_{w}")
            
    #         model.Add(deviation_pos >= scaled_worker_vars[w] - mean_scaled)
    #         model.Add(deviation_pos >= 0)
            
    #         model.Add(deviation_neg >= mean_scaled - scaled_worker_vars[w])
    #         model.Add(deviation_neg >= 0)
            
    #         # Add squared penalty (approximate with linear penalty weighted by deviation)
    #         variance_penalty = SUNDAY_BALANCE_ACROSS_WORKERS_PENALTY // 5
    #         sunday_balance_across_workers_penalties.append(variance_penalty * deviation_pos)
    #         sunday_balance_across_workers_penalties.append(variance_penalty * deviation_neg)


    # 7B Balancing number of LQ (quality weekends) across workers (pairwise)
    # Business rule: a weekend counts as LQ iff Saturday has shift "LQ" AND Sunday has shift "L".
    LQ_BALANCE_ACROSS_WORKERS_PENALTY = 50
    lq_balance_across_workers_penalties = []

    lq_free_worker_vars = {}
    workers_with_lq = []
    saturdays = [s - 1 for s in sundays if (s - 1) in days_of_year]

    for w in workers:
        # Only consider weekends where the worker is actually exposed:
        # both Saturday and the following Sunday exist in their working_days.
        eligible_saturdays = [s for s in saturdays if (s in working_days[w] and (s + 1) in working_days[w])]
        if not eligible_saturdays:
            continue

        workers_with_lq.append(w)
        lq_free_vars = []

        for s in eligible_saturdays:
            d = s + 1  # following Sunday

            # --- Saturday LQ flag ---
            # Use the existing shift var if available; otherwise create a dummy Bool forced to 0.
            if (w, s, "LQ") in shift:
                lq_sat = shift[(w, s, "LQ")]
            else:
                lq_sat = model.NewBoolVar(f"lq_sat_{w}_{s}")
                model.Add(lq_sat == 0)  

            # --- Sunday must be  "L" ---
            if (w, d, "L") in shift:
                sun_is_L = shift[(w, d, "L")]
            else:
                sun_is_L = model.NewBoolVar(f"sun_is_L_{w}_{d}")

                model.Add(sun_is_L == 0)  
            
            if (w, d, "F") in shift:
                sun_is_F = shift[(w, d, "F")]
            else:
                sun_is_F = model.NewBoolVar(f"sun_is_F_{w}_{d}")
                model.Add(sun_is_F == 0)

            # --- Weekend LQ indicator (AND of Saturday LQ and Sunday L) ---
            
            lq_weekend = model.NewBoolVar(f"lq_weekend_{w}_{s}_{d}")
            model.AddMultiplicationEquality(lq_weekend, [lq_sat, sun_is_L])

            lq_free_vars.append(lq_weekend)

        # Total LQ weekends per worker (bounded by the number of eligible weekends)
        total_lq_free_var = model.NewIntVar(0, len(lq_free_vars), f"total_lq_free_{w}")
        model.Add(total_lq_free_var == sum(lq_free_vars))
        lq_free_worker_vars[w] = total_lq_free_var

    if len(workers_with_lq) > 1:
        for i, w1 in enumerate(workers_with_lq):
            for w2 in workers_with_lq[i+1:]:
                # Keep your existing 'proportion' for consistency.
                # If you compute a specific LQ exposure (prop_lq), you can swap it here.
                if last_day.get(w1, 0) == 0 :
                    last_day[w1] = days_of_year[-1]
                if last_day.get(w2, 0) == 0 :
                    last_day[w2] = days_of_year[-1]
                prop1 = (last_day.get(w1, 0) - first_day.get(w1, 0) + 1) / len(days_of_year)
                prop1 = max(0.0, min(1.0, prop1))
                prop2 = (last_day.get(w2, 0) - first_day.get(w2, 0) + 1) / len(days_of_year)
                prop2 = max(0.0, min(1.0, prop2))
                if prop1 <= 0 or prop2 <= 0:
                    continue

                # Integer scaling avoids division and floating-point issues
                prop1_int = int(prop1 * 100)
                prop2_int = int(prop2 * 100)

                max_w1 = len([s for s in saturdays if (s in working_days[w1] and (s + 1) in working_days[w1])])
                max_w2 = len([s for s in saturdays if (s in working_days[w2] and (s + 1) in working_days[w2])])
                max_diff = max(max_w1 * prop2_int, max_w2 * prop1_int)

                diff_pos = model.NewIntVar(0, max_diff, f"lq_prop_diff_pos_{w1}_{w2}")
                diff_neg = model.NewIntVar(0, max_diff, f"lq_prop_diff_neg_{w1}_{w2}")

                # Normalize without division by comparing: c1*prop2 ≈ c2*prop1
                # Compare normalized counts without divisions: c1*prop2 ≈ c2*prop1
                model.Add(diff_pos >= lq_free_worker_vars[w1] * prop2_int - lq_free_worker_vars[w2] * prop1_int)
                model.Add(diff_neg >= lq_free_worker_vars[w2] * prop1_int - lq_free_worker_vars[w1] * prop2_int)

                weight = LQ_BALANCE_ACROSS_WORKERS_PENALTY // 2
                lq_balance_across_workers_penalties.append(weight * diff_pos)
                lq_balance_across_workers_penalties.append(weight * diff_neg)

    # Add to objective
    objective_terms.extend(lq_balance_across_workers_penalties) 


    #######################################################################################################
    ## STRSOL 879  --- Avoid simultaneous shifts managers and keyholders --  Folgas mutuamente exclusivas

    
    # Weights (tune as needed)
    # PEN_MGR_KH_SAME_OFF = 30000   # 
    # PEN_KH_OVERLAP      = 50000  # Penalize overlap among keyholders is being used the same value as managers

    # # A day-off is any of these labels
    # OFF_LABELS = ("L", "LQ")

    # closed = set(closed_holidays)

    # def bool_or(model, lits, name):
    #     #Returns BoolVar = OR(lits). If lits is empty, returns a fixed 0 BoolVar.
    #     if not lits:
    #         v = model.NewBoolVar(name)
    #         model.Add(v == 0)
    #         return v
    #     v = model.NewBoolVar(name)
    #     model.Add(v <= sum(lits))
    #     for lit in lits:
    #         model.Add(v >= lit)
    #     return v

    # def is_off_var(model, shift, w, d, name):
    #     #Returns BoolVar=1 if worker w is off on day d (any of OFF_LABELS).
    #     lits = []
    #     for lab in OFF_LABELS:
    #         v = shift.get((w, d, lab), None)
    #         if v is not None:
    #             lits.append(v)
    #     if not lits:
    #         v = model.NewBoolVar(name)
    #         model.Add(v == 0)
    #         return v
    #     v = model.NewBoolVar(name)
    #     model.Add(v <= sum(lits))
    #     for lit in lits:
    #         model.Add(v >= lit)
    #     return v

    # # 1) Penalize if a manager and (at least one) keyholder are off on the same day
    # for d in days_of_year:
    #     if d in closed:
    #         continue
        
    #     # mgr - managers / kh - keyholders
    #     mgr_off_lits = []
    #     kh_off_lits  = []

    #     for w in workers:
    #         if d not in working_days.get(w, []):
    #             continue  # not exposed
    #         role = role_by_worker.get(w, "normal")
    #         off_w_d = is_off_var(model, shift, w, d, f"off_{w}_{d}")
    #         if role == "manager":
    #             mgr_off_lits.append(off_w_d)
    #         elif role == "keyholder":
    #             kh_off_lits.append(off_w_d)

    #     mgr_off_any = bool_or(model, mgr_off_lits, f"mgr_off_any_{d}")
    #     kh_off_any  = bool_or(model, kh_off_lits,  f"kh_off_any_{d}")

    #     both_off = model.NewBoolVar(f"mgr_kh_both_off_{d}")
    #     model.AddMultiplicationEquality(both_off, [mgr_off_any, kh_off_any])

    #     objective_terms.append(PEN_MGR_KH_SAME_OFF * both_off)

    # # 2) Penalize overlap among keyholders (>1 keyholder off on the same day)
    # for d in days_of_year:
    #     if d in closed:
    #         continue

    #     kh_off_list = []
    #     for w in workers:
    #         if role_by_worker.get(w, "normal") == "keyholder" and d in working_days.get(w, []):
    #             kh_off_list.append(is_off_var(model, shift, w, d, f"kh_off_{w}_{d}"))

    #     if kh_off_list:
    #         kh_off_sum = model.NewIntVar(0, len(kh_off_list), f"kh_off_sum_{d}")
    #         model.Add(kh_off_sum == sum(kh_off_list))

    #         over1 = model.NewIntVar(0, len(kh_off_list), f"kh_over1_{d}")
    #         model.Add(over1 >= kh_off_sum - 1)
    #         model.Add(over1 >= 0)

    #         objective_terms.append(PEN_KH_OVERLAP * over1)

    # # 3) Penalize overlap among managers (>1 manager off on the same day)
    # for d in days_of_year:
    #     if d in closed:
    #         continue

    #     mgr_off_list = []
    #     for w in workers:
    #         if role_by_worker.get(w, "normal") == "manager" and d in working_days.get(w, []):
    #             mgr_off_list.append(is_off_var(model, shift, w, d, f"mgr_off_{w}_{d}"))

    #     if mgr_off_list:
    #         mgr_off_sum = model.NewIntVar(0, len(mgr_off_list), f"mgr_off_sum_{d}")
    #         model.Add(mgr_off_sum == sum(mgr_off_list))

    #         over1 = model.NewIntVar(0, len(mgr_off_list), f"mgr_over1_{d}")
    #         model.Add(over1 >= mgr_off_sum - 1)
    #         model.Add(over1 >= 0)

    #         objective_terms.append(PEN_KH_OVERLAP * over1)



    # Pesos
    PEN_MGR_KH_SAME_OFF = 30000
    PEN_KH_OVERLAP      = 50000
    PEN_MGR_OVERLAP     = 50000  # se quiseres manter penalização de managers

    # OFF_LABELS consideradas como “folga”
    OFF_LABELS = ("L", "LQ")
    closed = set(closed_holidays)

    # Pré-listas (assume que já tens role_by_worker)
    for w in workers:
        logger.info(f"Worker: {w}, Role: {role_by_worker.get(w, 'normal')}")

    managers   = [w for w in workers if role_by_worker.get(w, "normal") == "manager"]
    keyholders = [w for w in workers if role_by_worker.get(w, "normal") == "keyholder"]
    logger.info(f"Managers: {managers}, Keyholders: {keyholders}")

    debug_vars = {}

    for d in days_of_year:
        if d in closed:
            continue

        # Somas lineares (0/1) de “folga” por grupo (não criamos BoolVar por worker)
        def off_sum(group):
            # soma direta L + LQ; com exactly-one por (w,d), isto é 0/1 por worker
            return sum(
                sum(shift.get((w, d, lab), 0) for lab in OFF_LABELS)
                for w in group
                if d in days_of_year
            )

        mgr_sum = off_sum(managers)
        kh_sum  = off_sum(keyholders)

        # --- a) Manager & Keyholder off no mesmo dia (penaliza se ambos >= 1)
        mgr_any = model.NewBoolVar(f"mgr_any_{d}")
        kh_any  = model.NewBoolVar(f"kh_any_{d}")

        model.Add(mgr_sum >= 1).OnlyEnforceIf(mgr_any)
        model.Add(mgr_sum == 0).OnlyEnforceIf(mgr_any.Not())

        model.Add(kh_sum >= 1).OnlyEnforceIf(kh_any)
        model.Add(kh_sum == 0).OnlyEnforceIf(kh_any.Not())

        both_off = model.NewBoolVar(f"mgr_kh_both_off_{d}")
        # AND(mgr_any, kh_any)
        model.AddBoolAnd([mgr_any, kh_any]).OnlyEnforceIf(both_off)
        model.AddBoolOr([mgr_any.Not(), kh_any.Not()]).OnlyEnforceIf(both_off.Not())

        objective_terms.append(PEN_MGR_KH_SAME_OFF * both_off)

        # --- b) Overlap entre keyholders (>= 2 off no mesmo dia)
        if PEN_KH_OVERLAP > 0:
            kh_overlap = model.NewBoolVar(f"kh_overlap_{d}")
            debug_vars[f"kh_overlap_{d}"] = kh_overlap

            model.Add(kh_sum >= 2).OnlyEnforceIf(kh_overlap)
            model.Add(kh_sum <= 1).OnlyEnforceIf(kh_overlap.Not())
            objective_terms.append(PEN_KH_OVERLAP * kh_overlap)

        # --- c) Overlap entre managers (>= 2 off no mesmo dia) [opcional]
        if PEN_MGR_OVERLAP > 0 and managers:
            mgr_overlap = model.NewBoolVar(f"mgr_overlap_{d}")
            debug_vars[f"mgr_overlap_{d}"] = mgr_overlap
            model.Add(mgr_sum >= 2).OnlyEnforceIf(mgr_overlap)
            model.Add(mgr_sum <= 1).OnlyEnforceIf(mgr_overlap.Not())
            objective_terms.append(PEN_MGR_OVERLAP * mgr_overlap) 


        



    model.Minimize(sum(objective_terms))

    return debug_vars