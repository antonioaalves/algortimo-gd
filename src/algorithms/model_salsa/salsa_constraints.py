def shift_day_constraint(model, shift, days_of_year, workers, shifts):
    # Constraint for workers having an assigned shift
    for w in workers:
        for d in days_of_year:
           model.add_exactly_one(shift[(w, d, s)] for s in shifts)

def week_working_days_constraint(model, shift, week_to_days, workers, working_shift, contract_type):
    # Define working shifts
    working_shift = ["M", "T"]
    # Add constraint to limit working days in a week to contract type
    for w in workers:
        for week in range(1, 53):
            days_in_week = week_to_days[week]
            # Sum shifts across days and shift types
            total_shifts = sum(shift[(w, d, s)] for d in days_in_week for s in working_shift)
            max_days = contract_type.get(w, 0)
            model.Add(total_shifts <= max_days)

def maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, maxi):
    #limits maximum continuous working days
    for w in workers:
        for d in range(1, max(days_of_year) - maxi + 1):  # Start from the first day and check each possible 10-day window
            # Sum all working shifts over a sliding window of 11 consecutive days
            consecutive_days = sum(
                shift[(w, d + i, s)] 
                for i in range(maxi + 1)  # Check 11 consecutive days
                for s in working_shift
                if (w, d + i, s) in shift  # Make sure the day exists in our model
            )
            # If all 11 days have a working shift, that would exceed our limit of 10 consecutive days
            model.Add(consecutive_days <= maxi)

def maximum_free_days(model, shift, days_of_year, workers, total_l, c3d):
    #constraint for maximum of free days in a year
    for w in workers:
        model.Add(sum(shift[(w, d, "L")] + shift[(w, d, "LQ")] + shift[(w, d, "LD")]  for d in days_of_year) == total_l.get(w, 0) - c3d.get(w, 0))

def LD_attribution(model, shift, workers, working_days, l_d):
    # #constraint for maximum of LD days in a year
    for w in workers:
        model.Add(sum(shift[(w, d, "LD")] for d in working_days[w] if (w, d, "LD") in shift) == l_d.get(w, 0))

def closed_holiday_attribution(model, shift, workers, closed_holidays):
    #assigns free day in holidays
    for w in workers:
        for d in closed_holidays:
            if (w, d, "F") in shift:
                model.Add(shift[(w, d, "F")] == 1)
            else:
                print(f"Missing shift for worker {w}, day {d}, shift F")

def free_days_special_days_salsa(model, shift, special_days, workers, working_days, total_l_dom, free_sundays_plus_c2d, c2d):
    for w in workers:
        # Only consider special days that are in this worker's working days
        worker_special_days = [d for d in special_days if d in working_days[w]]
        if free_sundays_plus_c2d == True:
            model.Add(sum(shift[(w, d, "L")] for d in worker_special_days) >= total_l_dom.get(w, 0) + c2d.get(w, 0) )
        else:
            model.Add(sum(shift[(w, d, "L")] for d in worker_special_days) >= total_l_dom.get(w, 0) )

def assign_week_shift(model, shift, workers, week_to_days, working_days, worker_week_shift):
    # Contraint for workers shifts taking into account the worker_week_shift (each week a worker can either be )
        for w in workers:
            for week in range(1, 53):  # Iterate over the 52 weeks
                # Iterate through days of the week for the current week
                for day in week_to_days[week]:
                    if day in working_days[w]:
                        # Morning shift constraint: worker can only be assigned to M if available for M
                        model.Add(shift[(w, day, "M")] <= worker_week_shift[(w, week, 'M')])
                        
                        # Afternoon shift constraint: worker can only be assigned to T if available for T
                        model.Add(shift[(w, day, "T")] <= worker_week_shift[(w, week, 'T')])

def working_day_shifts(model, shift, workers, working_days, check_shift):
# Check for the workers so that they can only have M, T, TC, L, LD and LQ in workingd days
  #  check_shift = ['M', 'T', 'L', 'LQ', "LD"]
    for w in workers:
        for d in working_days[w]:
            model.add_exactly_one(shift[(w, d, s)] for s in check_shift if (w, d, s) in shift)

def salsa_2_consecutive_free_days(model, shift, workers, working_days):
    for w in workers: 
        # Get all working days for this worker
        all_work_days = sorted(working_days[w])
        
        # Create boolean variables for each day indicating if it's a free day (L, F, or LQ)
        free_day_vars = {}
        for d in all_work_days:
            free_day = model.NewBoolVar(f"free_day_{w}_{d}")
            
            # Sum the L, F, and LQ shifts for this day
            # If F_special_day is True, consider F shifts as well
            free_shift_sum = sum(
                    shift.get((w, d, shift_type), 0) 
                    for shift_type in ["L", "F", "LQ"]
                )

           
            
            # Link the boolean variable to whether any free shift is assigned
            model.Add(free_shift_sum >= 1).OnlyEnforceIf(free_day)
            model.Add(free_shift_sum == 0).OnlyEnforceIf(free_day.Not())
            
            free_day_vars[d] = free_day
        
        # For each consecutive triplet of days in the worker's schedule
        for i in range(len(all_work_days) - 2):
            day1 = all_work_days[i]
            day2 = all_work_days[i+1]
            day3 = all_work_days[i+2]
            
            # Only apply constraint if days are actually consecutive
            if day2 == day1 + 1 and day3 == day2 + 1:
                # At least one of any three consecutive days must NOT be a free day
                model.AddBoolOr([
                    free_day_vars[day1].Not(), 
                    free_day_vars[day2].Not(), 
                    free_day_vars[day3].Not()
                ])


def salsa_2_day_quality_weekend(model, shift, workers, contract_type, working_days, sundays, c2d, F_special_day, days_of_year, closed_holidays):
    # Track quality 2-day weekends and ensure LQ is only used in this pattern
    for w in workers:
        if contract_type[w] in [4, 5, 6]:
            quality_2weekend_vars = []
            
            if F_special_day == False:
                # First, identify all potential 2-day quality weekends (Saturday + Sunday)
                for d in working_days[w]:
                    # If this is a Sunday and the previous day (Saturday) is a working day
                    if d in sundays and d - 1 in working_days[w]:  
                        # Boolean variables to check if the worker is assigned each shift
                        has_L_on_sunday = model.NewBoolVar(f"has_L_on_sunday_{w}_{d}")
                        has_LQ_on_saturday = model.NewBoolVar(f"has_LQ_on_saturday_{w}_{d-1}")

                        # Connect boolean variables to actual shift assignments
                        model.Add(shift.get((w, d, "L"), 0) >= 1).OnlyEnforceIf(has_L_on_sunday)
                        model.Add(shift.get((w, d, "L"), 0) == 0).OnlyEnforceIf(has_L_on_sunday.Not())

                        model.Add(shift.get((w, d - 1, "LQ"), 0) >= 1).OnlyEnforceIf(has_LQ_on_saturday)
                        model.Add(shift.get((w, d - 1, "LQ"), 0) == 0).OnlyEnforceIf(has_LQ_on_saturday.Not())

                        # Create a binary variable to track whether this weekend qualifies as a 2-day quality weekend
                        quality_weekend_2 = model.NewBoolVar(f"quality_weekend_2_{w}_{d}")

                        # A weekend is "quality 2" only if both conditions are met: LQ on Saturday and L on Sunday
                        model.AddBoolAnd([has_L_on_sunday, has_LQ_on_saturday]).OnlyEnforceIf(quality_weekend_2)
                        model.AddBoolOr([has_L_on_sunday.Not(), has_LQ_on_saturday.Not()]).OnlyEnforceIf(quality_weekend_2.Not())

                        # Track the quality weekend count
                        quality_2weekend_vars.append(quality_weekend_2)
                
                # Constraint: The worker should have at least c2d quality weekends
                model.Add(sum(quality_2weekend_vars) >= c2d.get(w, 0))
                
                # Now ensure LQ shifts ONLY appear on Saturdays before Sundays with L shifts
                # For every working day for this worker
                for d in working_days[w]:
                    # If the worker can be assigned an LQ shift on this day
                    if (w, d, "LQ") in shift:
                        # This boolean captures if this day could be part of a quality weekend
                        could_be_quality_weekend = model.NewBoolVar(f"could_be_quality_weekend_{w}_{d}")
                        
                        # Conditions for a day to be eligible for LQ:
                        # 1. It must not be a Sunday
                        # 2. The next day must be a Sunday in worker's working days
                        # 3. There must be an L shift on that Sunday
                        
                        eligible_conditions = []
                        
                        # Check if this is a Saturday (day before a Sunday) and the Sunday is a working day
                        if d + 1 in working_days[w] and d + 1 in sundays:
                            # Create a boolean for whether there's a Sunday L shift
                            has_sunday_L = model.NewBoolVar(f"next_day_L_{w}_{d+1}")
                            model.Add(shift.get((w, d + 1, "L"), 0) >= 1).OnlyEnforceIf(has_sunday_L)
                            model.Add(shift.get((w, d + 1, "L"), 0) == 0).OnlyEnforceIf(has_sunday_L.Not())
                            
                            eligible_conditions.append(has_sunday_L)
                        
                        # If no eligible conditions were found, this day can't be part of a quality weekend
                        if eligible_conditions:
                            model.AddBoolAnd(eligible_conditions).OnlyEnforceIf(could_be_quality_weekend)
                            model.AddBoolOr([cond.Not() for cond in eligible_conditions]).OnlyEnforceIf(could_be_quality_weekend.Not())
                        else:
                            model.Add(could_be_quality_weekend == 0)
                        
                        # Final constraint: LQ can only be assigned if this day could be part of a quality weekend
                        model.Add(shift.get((w, d, "LQ"), 0) <= could_be_quality_weekend)
            else:
                # First, identify all potential 2-day quality weekends (Saturday + Sunday)
                for d in days_of_year:
                    if d in sundays and (d in working_days[w]or d in closed_holidays) and (d - 1 in working_days[w] or d - 1 in closed_holidays):
                        # Boolean variables to check if the worker is assigned each shift
                        has_L_on_sunday = model.NewBoolVar(f"has_L_on_sunday_{w}_{d}")
                        has_LQ_on_saturday = model.NewBoolVar(f"has_LQ_on_saturday_{w}_{d-1}")
                        has_F_on_saturday = model.NewBoolVar(f"has_F_on_saturday_{w}_{d-1}")
                        has_F_on_sunday = model.NewBoolVar(f"has_F_on_sunday_{w}_{d}")


                        # Connect boolean variables to actual shift assignments
                        model.Add(shift.get((w, d, "L"), 0) >= 1).OnlyEnforceIf(has_L_on_sunday)
                        model.Add(shift.get((w, d, "L"), 0) == 0).OnlyEnforceIf(has_L_on_sunday.Not())

                        model.Add(shift.get((w, d - 1, "LQ"), 0) >= 1).OnlyEnforceIf(has_LQ_on_saturday)
                        model.Add(shift.get((w, d - 1, "LQ"), 0) == 0).OnlyEnforceIf(has_LQ_on_saturday.Not())

                        model.Add(shift.get((w, d - 1, "F"), 0) >= 1).OnlyEnforceIf(has_F_on_saturday)
                        model.Add(shift.get((w, d - 1, "F"), 0) == 0).OnlyEnforceIf(has_F_on_saturday.Not())

                        model.Add(shift.get((w, d, "F"), 0) >= 1).OnlyEnforceIf(has_F_on_sunday)
                        model.Add(shift.get((w, d, "F"), 0) == 0).OnlyEnforceIf(has_F_on_sunday.Not())

                        # Create a binary variable to track whether this weekend qualifies as a 2-day quality weekend
                        quality_weekend_2 = model.NewBoolVar(f"quality_weekend_2_{w}_{d}")

                        # A weekend is "quality 2" only if both conditions are met: LQ on Saturday and L on Sunday
                        model.AddBoolAnd([has_L_on_sunday, has_LQ_on_saturday]).OnlyEnforceIf(quality_weekend_2)
                        model.AddBoolAnd([has_L_on_sunday, has_F_on_saturday]).OnlyEnforceIf(quality_weekend_2)
                        model.AddBoolAnd([has_F_on_sunday, has_LQ_on_saturday]).OnlyEnforceIf(quality_weekend_2)

                        model.AddBoolOr([has_L_on_sunday.Not(), has_LQ_on_saturday.Not()]).OnlyEnforceIf(quality_weekend_2.Not())
                        model.AddBoolOr([has_L_on_sunday.Not(), has_F_on_saturday.Not()]).OnlyEnforceIf(quality_weekend_2.Not())
                        model.AddBoolOr([has_F_on_sunday.Not(), has_LQ_on_saturday.Not()]).OnlyEnforceIf(quality_weekend_2.Not())

                        # Track the quality weekend count
                        quality_2weekend_vars.append(quality_weekend_2)
                
                # Constraint: The worker should have at least c2d quality weekends
                model.Add(sum(quality_2weekend_vars) >= c2d.get(w, 0))
                
                # Now ensure LQ shifts ONLY appear on Saturdays before Sundays with L shifts
                # For every working day for this worker
                for d in working_days[w]:
                    # If the worker can be assigned an LQ shift on this day
                    if (w, d, "LQ") in shift:
                        # This boolean captures if this day could be part of a quality weekend
                        could_be_quality_weekend = model.NewBoolVar(f"could_be_quality_weekend_{w}_{d}")
                        
                        # Conditions for a day to be eligible for LQ:
                        # 1. It must not be a Sunday
                        # 2. The next day must be a Sunday in worker's working days
                        # 3. There must be an L shift on that Sunday
                        
                        eligible_conditions = []
                        
                        # Check if this is a Saturday (day before a Sunday) and the Sunday is a working day
                        if d + 1 in working_days[w] and d + 1 in sundays:
                            # Create a boolean for whether there's a Sunday L shift
                            has_sunday_L = model.NewBoolVar(f"next_day_L_{w}_{d+1}")
                            model.Add(shift.get((w, d + 1, "L"), 0) >= 1).OnlyEnforceIf(has_sunday_L)
                            model.Add(shift.get((w, d + 1, "L"), 0) == 0).OnlyEnforceIf(has_sunday_L.Not())
                            
                            eligible_conditions.append(has_sunday_L)
                        
                        # If no eligible conditions were found, this day can't be part of a quality weekend
                        if eligible_conditions:
                            model.AddBoolAnd(eligible_conditions).OnlyEnforceIf(could_be_quality_weekend)
                            model.AddBoolOr([cond.Not() for cond in eligible_conditions]).OnlyEnforceIf(could_be_quality_weekend.Not())
                        else:
                            model.Add(could_be_quality_weekend == 0)
                        
                        # Final constraint: LQ can only be assigned if this day could be part of a quality weekend
                        model.Add(shift.get((w, d, "LQ"), 0) <= could_be_quality_weekend)

def salsa_saturday_L_constraint(model, shift, workers, working_days, start_weekday):
    # For each worker, constrain L/LD on Friday if LQ on Saturday, and L/LD on Monday if L on Sunday
    for w in workers:
        for day in working_days[w]:
            # Get day of week (5 = Saturday)
            day_of_week = (day + start_weekday - 3 ) % 7
        
            # Case 1: Friday (day_of_week == 5) followed by LQ on Saturday
            if day_of_week == 5 and day_of_week in working_days[w]:
                # If LQ on Saturday, then no L/LD on Friday
                sunday_l = shift.get((w, day + 1, "LQ"), 0)
                saturday_l = shift.get((w, day, "L"), 0)
                
                # If Sunday has L, then Saturday can't have L 
                model.Add(saturday_l == 0).OnlyEnforceIf(sunday_l)
            
        

def salsa_2_free_days_week(model, shift, workers, week_to_days_salsa, working_days):
    for w in workers:
        
        # Create variables for free days (L, F, LQ) by week
        for week, days in week_to_days_salsa.items():
            
            # Only include workdays (excluding weekends)
            week_work_days = [
                d for d in days 
                if d in working_days[w]
            ]
            
            # Sort days to ensure they're in chronological order
            week_work_days.sort()
            
            # Skip if no working days for this worker in this week
            if not week_work_days:
                continue
            
            # Calculate proportional requirement based on actual days in the week
            # Standard week has 7 days and requires 2 free days
            # Proportion: (actual_days / 7) * 2
            actual_days_in_week = len(days)  # Total days in this week
            proportion = actual_days_in_week / 7.0
            required_free_days = int(round(proportion * 2))
            
            # Ensure at least 0 free days (in case of very short weeks)
            required_free_days = max(0, required_free_days)
            
            # Only add constraint if we require at least 1 free day
            if required_free_days > 0:
                # Create a sum of free shifts for this worker in the current week
                free_shift_sum = sum(
                    shift.get((w, d, shift_type), 0) 
                    for d in week_work_days 
                    for shift_type in ["L", "F", "LQ"]
                )
                
                # Add proportional constraint
                model.Add(free_shift_sum >= required_free_days)


#--------------------------------------------------------------------------------------------------------------------------------------
def salsa_week_cut_contraint(model, shift, workers, week_to_days_salsa, week_cut , start_weekday):
    if week_cut:
        # Calculate the ratio for the first week
        first_week_ratio = (7 - start_weekday + 1) / 7
        # Calculate the ratio for the last week
        last_week_ratio = (len(week_to_days_salsa[max(week_to_days_salsa.keys())]) / 7) 

        # Constraint for the first week
        for w in workers:
            free_days_first_week = sum(
                shift[(w, d, "L")] + shift[(w, d, "LQ")] + shift[(w, d, "F")]
                for d in week_to_days_salsa[1]
            )
            model.Add(free_days_first_week >= round(first_week_ratio * 2))

        # Constraint for the last week
        if len(week_to_days_salsa) > 52:  # Ensure there is a 53rd week
            for w in workers:
                free_days_last_week = sum(
                    shift[(w, d, "L")] + shift[(w, d, "LQ")] + shift[(w, d, "F")]
                    for d in week_to_days_salsa[max(week_to_days_salsa.keys())]
                )
                model.Add(free_days_last_week >= round(last_week_ratio * 2))