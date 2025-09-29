from math import floor, ceil
from base_data_project.log_config import get_logger

logger = get_logger('algoritmo_GD')

def compensation_days(model, shift, workers, working_days, holidays, start_weekday, week_to_days, working_shift, week_compensation_limit, fixed_days_off, fixed_LQs):
    #Define compensation days (LD) constraint for contract types 4, 5, 6
    possible_compensation_days = {}
    for w in workers:
        possible_compensation_days[w] = {}
        for d in holidays:
            fixed = set(fixed_LQs[w])
            off = set(fixed_days_off[w])
            if d in working_days[w] - off - fixed:
                # Determine the week of the special day
                special_day_week = next((wk for wk, days in week_to_days.items() if d in days), None)
            
                if special_day_week is None:
                    continue
                
                # Possible compensation weeks (current and next week)
                if week_compensation_limit.get(w, 2) == 2:
                    possible_weeks = [
                        special_day_week + 1 if special_day_week < 52 else None, 
                        special_day_week + 2 if special_day_week < 51 else special_day_week
                    ]
                else:
                    possible_weeks = [
                        special_day_week + 1 if special_day_week < 52 else None, 
                        special_day_week + 2 if special_day_week < 51 else special_day_week,
                        special_day_week + 3 if special_day_week < 50 else None,
                        special_day_week + 4 if special_day_week < 49 else None
                    ]
                
                # Collect potential compensation days
                compensation_days = []
                for week in filter(None, possible_weeks):
                    compensation_days.extend([day for day in week_to_days.get(week, [])
                        if (day in working_days[w] - off - fixed)
                    ])
                
                # Store possible compensation days for this special day
                possible_compensation_days[w][d] = compensation_days

    # Dictionary to store all compensation day variables
    all_comp_day_vars = {}

    # Dictionary to track compensation day usage
    comp_day_usage = {}

    # Main optimization loop
    for w in workers:
        # Initialize the compensation day usage tracking for this worker
        comp_day_usage[w] = {}
    
        # Track which special days were worked
        worked_holidays = {}
        fixed = set(fixed_LQs[w])
        off = set(fixed_days_off[w])
        
        # First, create all the worked_special_day variables
        # ONLY for special days in this worker's working days
        for d in [day for day in holidays if day in working_days[w] - off - fixed]:
            # Create a boolean variable to track if the worker worked on this special day
            worked_special_day = model.NewBoolVar(f'worked_special_day_{w}_{d}')
            worked_holidays[d] = worked_special_day
            special_day_shift_vars = [shift.get((w, d, s)) for s in working_shift if (w, d, s) in shift]
            
            # If there are shift variables for this day, add a constraint
            if special_day_shift_vars:
                # worked_special_day is true if any shift is assigned
                model.AddBoolOr(special_day_shift_vars).OnlyEnforceIf(worked_special_day)
                model.Add(sum(special_day_shift_vars) == 0).OnlyEnforceIf(worked_special_day.Not())
        
        # Now collect all possible compensation days for this worker
        all_possible_comp_days = set()
        for d in worked_holidays.keys():  # Use keys from worked_holidays to ensure alignment
            if d in possible_compensation_days[w]:
                all_possible_comp_days.update(possible_compensation_days[w][d])
        for d in working_days[w]:
            if (w, d, 'LD') in shift:
                if d not in all_possible_comp_days:
                    model.Add(shift[(w, d, 'LD')] == 0)  # Never allow LD outside compensation

        # For each possible compensation day, create a variable indicating if it's used as a compensation day
        for comp_day in all_possible_comp_days:
            # Create a variable to track if this compensation day is used
            comp_day_used = model.NewBoolVar(f'comp_day_used_{w}_{comp_day}')
            comp_day_usage[w][comp_day] = comp_day_used
            
            # Create variables for which special day this compensation day is for
            special_day_assignment_vars = []
            
            # Only iterate through special days that exist in worked_holidays
            for special_day in worked_holidays.keys():
                # Check if this special day has this compensation day as an option
                if comp_day in possible_compensation_days[w].get(special_day, []):
                    # Create a variable indicating this compensation day is assigned to this special day
                    assignment_var = model.NewBoolVar(f'comp_day_{w}_{special_day}_{comp_day}')
                    special_day_assignment_vars.append((special_day, assignment_var))
                    
                    # Store for later reference
                    if w not in all_comp_day_vars:
                        all_comp_day_vars[w] = {}
                    all_comp_day_vars[w][(special_day, comp_day)] = assignment_var
                    
                    # This compensation day is only assigned if the worker worked that special day
                    # Now this is safe because we know special_day is in worked_holidays
                    model.AddImplication(assignment_var, worked_holidays[special_day])
                    
                    # If this assignment is true, the compensation day is used
                    model.AddImplication(assignment_var, comp_day_used)
                    
                    # Constraint: If assignment is true, this day must be a valid day off (LD)
                    model.Add(shift[(w, comp_day, 'LD')] == comp_day_used)

            # KEY CONSTRAINT: At most one special day can be assigned to this compensation day
            if len(special_day_assignment_vars) > 1:
                # Extract just the assignment variables
                assignment_vars = [var for _, var in special_day_assignment_vars]
                # At most one assignment can be true
                model.Add(sum(assignment_vars) <= 1)

        # For each special day, ensure it gets a compensation day if worked
        for d in worked_holidays.keys():
            # Get all variables for compensation days for this special day
            comp_day_vars = [
                all_comp_day_vars[w][(d, comp_day)] 
                for comp_day in possible_compensation_days[w].get(d, [])
                if (d, comp_day) in all_comp_day_vars[w]
            ]

            # If the worker worked this special day, ensure one compensation day is assigned
            if comp_day_vars:
                # Normal case: enforce exactly 1 comp day if worked
                model.Add(sum(comp_day_vars) == 1).OnlyEnforceIf(worked_holidays[d])
                model.Add(sum(comp_day_vars) == 0).OnlyEnforceIf(worked_holidays[d].Not())
            else:
                model.Add(worked_holidays[d] == 0)
        # Total worked holidays
        total_worked_holidays = sum(worked_holidays.values())
        # Total compensation days used
        total_comp_days_used = sum(comp_day_usage[w].values())
        # Enforce equality: number of LDs == number of worked holidays
        if w == 7656:
            print(total_worked_holidays)
            print(total_comp_days_used)
            print("ola")
        model.Add(total_comp_days_used == total_worked_holidays)

def shift_day_constraint(model, shift, days_of_year, workers_complete, shifts):
    # Constraint for workers having an assigned shift
    for w in workers_complete:
        for d in days_of_year:
            total_shifts = []
            for s in shifts:
                if (w, d, s) in shift:
                    total_shifts.append(shift[(w, d, s)])
            if (total_shifts):
                model.add_exactly_one(total_shifts)

def week_working_days_constraint(model, shift, week_to_days, workers, working_shift, contract_type, work_days_per_week):
    # Define working shifts
    # Add constraint to limit working days in a week to contract type
    for w in workers:
        for week in week_to_days.keys():
            days_in_week = week_to_days[week]
            # Sum shifts across days and shift types
            total_shifts = sum(shift[(w, d, s)] for d in days_in_week for s in working_shift if (w, d, s) in shift)
            max_days = contract_type.get(w, 0)
            if max_days == 8:
                max_days = work_days_per_week[w][week - 1]
                if w == 7656:
                    print(f"1: {w}, week {week}, days worked {work_days_per_week[w][week - 1]}")
            model.Add(total_shifts <= max_days)

def maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, maxi):
    #limits maximum continuous working days
    for w in workers:
        for d in range(1, max(days_of_year) - maxi + 1):  # Start from the first day and check each possible 7-day window
            # Sum all working shifts over a sliding window of contract maximum + 1 consecutive days
            consecutive_days = sum(
                shift[(w, d + i, s)] 
                for i in range(maxi + 1)  # Check contract_maximum + 1 consecutive days
                for s in working_shift
                if (w, d + i, s) in shift  # Make sure the day exists in our model
            )
            # If all 11 days have a working shift, that would exceed our limit of 10 consecutive days
            model.Add(consecutive_days <= maxi)

def LQ_attribution(model, shift, workers, working_days, c2d):
    # #constraint for maximum of LD days in a year
    for w in workers:
        model.Add(sum(shift[(w, d, "LQ")] for d in working_days[w] if (w, d, "LQ") in shift) >= c2d.get(w, 0))

def assign_week_shift(model, shift, workers, week_to_days, working_days, worker_week_shift):
    # Contraint for workers shifts taking into account the worker_week_shift (each week a worker can either be )
        for w in workers:
            for week in week_to_days.keys():  # Iterate over the 52 weeks
                # Iterate through days of the week for the current week
                for day in week_to_days[week]:
                    if day in working_days[w]:
                        # Morning shift constraint: worker can only be assigned to M if available for M
                        if ((w, day, "M") in shift):
                            model.Add(shift[(w, day, "M")] <= worker_week_shift[(w, week, 'M')])
                        
                        # Afternoon shift constraint: worker can only be assigned to T if available for T
                        if ((w, day, "T") in shift):
                            model.Add(shift[(w, day, "T")] <= worker_week_shift[(w, week, 'T')])

def working_day_shifts(model, shift, workers, working_days, check_shift, workers_complete_cycle, working_shift):
# Check for the workers so that they can only have M, T, TC, L, LD and LQ in working days
  #  check_shift = ['M', 'T', 'L', 'LQ', "LD"]
    for w in workers:
        for d in working_days[w]:
            total_shifts = []
            for s in check_shift:
                if (w, d, s) in shift:
                    total_shifts.append(shift[(w, d, s)])
            if total_shifts:
                model.add_exactly_one(total_shifts)

    for w in workers_complete_cycle:
        for d in working_days[w]:
            # Ensure that the worker can only have M, T, L, LQ, LD and F in working days
            total_shifts = []
            for s in working_shift:
                if (w, d, s) in shift:
                    total_shifts.append(shift[(w, d, s)])
            if total_shifts:
                model.add_exactly_one(total_shifts)

def salsa_esp_2_consecutive_free_days(model, shift, workers, working_days):
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


def salsa_esp_2_day_quality_weekend(model, shift, workers, contract_type, working_days, sundays, c2d, F_special_day, days_of_year, closed_holidays):
    # Track quality 2-day weekends and ensure LQ is only used in this pattern
    debug_vars = {}  # Store debug variables to return
    for w in workers:

        if contract_type[w] in [4, 5, 6, 8]:
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

                        debug_vars[f"could_be_quality_weekend_{w}_{d}"] = could_be_quality_weekend
                        
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
                        
                #         # Final constraint: LQ can only be assigned if this day could be part of a quality weekend


                        model.Add(shift.get((w, d, "LQ"), 0) <= could_be_quality_weekend)
            else:
                # First, identify all potential 2-day quality weekends (Saturday + Sunday)
                for d in days_of_year:
                    if d in sundays and (d in working_days[w] or d in closed_holidays) and (d - 1 in working_days[w] or d - 1 in closed_holidays):
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
    
    return debug_vars

def salsa_esp_saturday_L_constraint(model, shift, workers, working_days, start_weekday, days_of_year, non_working_days):
    # For each worker, constrain L on Saturday if L on Sunday
    for w in workers:
        for day in working_days[w]:
            # Get day of week (5 = Saturday)
            day_of_week = (day + start_weekday - 2) % 7
            
            # Case 1: Saturday (day_of_week == 5)
            if day_of_week == 5:
                sunday_day = day + 1
                
                # Check if Sunday exists and is within the year bounds
                if sunday_day in working_days[w]:
                    # Check if both Saturday and Sunday shifts exist for this worker
                    saturday_l_key = (w, day, "L")
                    sunday_l_key = (w, sunday_day, "L")
                    
                    if saturday_l_key in shift and sunday_l_key in shift:
                        saturday_l = shift[saturday_l_key]
                        sunday_l = shift[sunday_l_key]
                        #logger.debug(f"DEBUG: Adding constraint for Worker {w}, Saturday {day}, Sunday {sunday_day}")
                        # If Sunday has L, then Saturday can't have L
                        # This translates to: sunday_l == 1 → saturday_l == 0
                        # Which is equivalent to: saturday_l + sunday_l <= 1
                        model.Add(saturday_l + sunday_l <= 1)

def salsa_esp_2_free_days_week(model, shift, workers, week_to_days_salsa_esp, working_days, admissao_proporcional, data_admissao, data_demissao, fixed_days_off, fixed_LQs, contract_type, work_days_per_week):
    for w in workers:
        worker_admissao = data_admissao.get(w, 0)
        worker_demissao = data_demissao.get(w, 0)
        #logger.info(f"Worker {w}, Admissao: {worker_admissao}, Demissao: {worker_demissao}, Working Days: {working_days[w]}, Week Days: {week_to_days_salsa_esp}")


        # Create variables for free days (L, F, LQ) by week
        for week, days in week_to_days_salsa_esp.items():
            
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
            
            week_work_days_set = set(week_work_days)

            fixed_days_week = week_work_days_set.intersection(set(fixed_days_off[w]))
            fixed_lqs_week = week_work_days_set.intersection(set(fixed_LQs[w]))

            # Check if admissao or demissao day falls within this week
            is_admissao_week = (worker_admissao > 0 and worker_admissao in days)
            is_demissao_week = (worker_demissao > 0 and worker_demissao in days)
            
            # If this is an admissao or demissao week, apply proportional calculation
            actual_days_in_week = len(week_work_days)  # Total days in this week
            if is_admissao_week or is_demissao_week:
                # Calculate proportional requirement based on actual days in the week
                # Standard week has 7 days and requires 2 free days
                # Proportion: (actual_days / 7) * 2

                if 4 <= actual_days_in_week <= 5:
                    required_free_days = 1
                elif actual_days_in_week < 4:
                    required_free_days = 0
                else:
                    if contract_type.get(w, 0) == 8 and work_days_per_week[w][week - 1] == 6:
                        required_free_days = 1
                    else:
                        required_free_days = 2
                #logger.info(f"Worker {w}, Week {week} (Admissao/Demissao week), Days: {week_work_days}"
                #           f", Required Free Days = {required_free_days}")
            
            else:
                if week_work_days[0] > 243 and w == 7656:
                    print(f"ai os dias {week_work_days} e descansos {work_days_per_week[w][week - 1]} dias fixos {fixed_days_week} {fixed_lqs_week}")
                if contract_type.get(w, 0) == 8 and work_days_per_week[w][week - 1] == 6 and actual_days_in_week >= 1:
                    required_free_days = 1
                elif actual_days_in_week >= 2:
                    required_free_days = 2
                elif actual_days_in_week == 1:
                    # Partial week with 4+ days: require 1 free day
                    required_free_days = 1
                else:
                     # Very short week: no requirement
                     required_free_days = 0
                #logger.info(f"Worker {w}, Week {week} (Regular week), Days {week_work_days}: "
                #           f"Required Free Days = {required_free_days}")
            if w == 7656:
                print(f"2: {w}, week {week}, days worked {work_days_per_week[w][week - 1]}")

            if required_free_days < (len(fixed_days_week) + len(fixed_lqs_week)):
                required_free_days = len(fixed_days_week) + len(fixed_lqs_week)
                logger.info(f" Worker {w} - Adjusted Required Free Days to {required_free_days} due to fixed days off: {fixed_days_week} on week {week}")

            # Only add constraint if we require at least 1 free day
            if required_free_days >= 0:
                # Create a sum of free shifts for this worker in the current week
                free_shift_sum = sum(
                    shift.get((w, d, shift_type), 0) 
                    for d in week_work_days 
                    for shift_type in ["L", "LQ"]
                )

                if required_free_days == 2:
                    if (len(week_work_days) >= 2):
                        model.Add(free_shift_sum == required_free_days)
                elif required_free_days == 1:
                    if (len(week_work_days) >= 1):
                        #logger.info(f"Adding constraint for Worker {w}, Week {week}, Required Free Days: {required_free_days}, Free Shift Sum Variable: {free_shift_sum}")
                        #if week_work_days[0] > 243 and w == 7656:
                        #    model.Add(free_shift_sum >= required_free_days)
                        #else:
                        model.Add(free_shift_sum == required_free_days)
                elif required_free_days == 0:
                    model.Add(free_shift_sum == 0)
                #else:
                #    model.Add(free_shift_sum <= required_free_days)

#-----------------------------------------------------------------------------------------------
def first_day_not_free(model, shift, workers, working_days, first_registered_day, working_shift):
    """Ensures that workers contracted in the middle of the period have a working shift on their first registered day."""
    # Find the earliest first registered day across all workers
    earliest_first_day = min(first_registered_day.get(w, float('inf')) for w in workers if first_registered_day.get(w, 0) > 0)
    
    for w in workers:
        # Get the worker's first registered day
        worker_first_day = first_registered_day.get(w, 0)
        
        # Only apply constraint if:
        # 1. The worker has a valid first registered day
        # 2. That day is within their working days
        # 3. The worker was contracted after the earliest worker (i.e., in the middle of the period)
        if (worker_first_day > 0 and 
            worker_first_day in working_days[w] and 
            worker_first_day > earliest_first_day):
            # Ensure the worker has exactly one working shift on their first registered day
            model.Add(sum(shift.get((w, worker_first_day, shift_type), 0) 
                    for shift_type in working_shift) == 1)
            
#-------------------------------------------------------------------------------------------------------------------------------------
def free_days_special_days(model, shift, sundays, workers, working_days, total_l_dom):
    for w in workers:
        # Only consider special days that are in this worker's working days
        worker_sundays = [d for d in sundays if d in working_days[w]]
        logger.info(f"Worker {w}, Special Days {worker_sundays}")
        model.Add(sum(shift[(w, d, "L")] for d in worker_sundays) >= total_l_dom.get(w, 0))

