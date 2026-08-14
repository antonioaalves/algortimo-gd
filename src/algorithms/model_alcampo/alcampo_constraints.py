from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config
from src.algorithms.model_alcampo.auxiliar_functions_alcampo import compensation_days_calc, compensation_days_calc_with_contract_changes, get_dummy, get_annual_variables

_config_manager = get_config()
logger = get_logger(_config_manager.project_name)


"""This file contains the constraints for the Alcampo shift scheduler."""

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


def week_working_days_constraint(model, shift, week_to_days, workers, working_shift, work_days_per_week, period, complete_cycle_days):
    # Define working shifts
    # Add constraint to limit working days in a week to contract type
    for w in workers:
        for week in week_to_days.keys():
            days_in_week = week_to_days[week]
            if days_in_week[-1] < period[0] or days_in_week[0] > period[1] or any(d in complete_cycle_days[w] for d in days_in_week):
                continue
            # Sum shifts across days and shift types
            total_shifts = sum(shift[(w, d, s)] for d in days_in_week for s in working_shift if (w, d, s) in shift)
            max_days = work_days_per_week[w][week - 1]
            model.Add(total_shifts <= max_days)
        
def maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, max_days, period, dummy_workers, workers_with_dummy, complete_cycle_days):
    #limits maximum continuous working days
    for w in workers:
        for d in range(1, max(days_of_year) - max_days + 1):  # Start from the first day and check each possible 7-day window
            # Sum all working shifts over a sliding window of contract maximum + 1 consecutive days
            if d + max_days < period[0] or d > period[1]:
                continue
            if len([days_comp for days_comp in range(max_days + 1) if days_comp + d not in complete_cycle_days[w]]) == 0:
                #logger.info(f"skipping max consec work days for {w} around day {d}")
                continue
            consecutive_days = sum(
                shift[(w, d + i, s)] 
                for i in range(max_days + 1)  # Check contract_maximum + 1 consecutive days
                for s in working_shift
                if (w, d + i, s) in shift  # Make sure the day exists in our model
            )
            # If all 11 days have a working shift, that would exceed our limit of 10 consecutive days
            model.Add(consecutive_days <= max_days)
    if dummy_workers:
        for w in workers_with_dummy:
            dummies = sorted(workers_with_dummy.get(w, {}).values())
            change_date = dummy_workers[dummies[0]]["start_date"] - 1
            if len([days_comp for days_comp in range(max_days // 2 + 1) if change_date - days_comp not in complete_cycle_days[w]]) == 0 and \
               len([days_comp for days_comp in range(1, max_days // 2 + 1) if change_date + days_comp not in complete_cycle_days[dummies[0]]]) == 0:
                logger.info(f"worker {w} and dummie {dummies[0]} skipped restriction on day {change_date}")
                continue
            consecutive_days = sum(
                shift[(w, change_date - i, s)]
                for i in range(max_days // 2 + 1)
                for s in working_shift
                if (w, change_date - i, s) in shift
            ) + sum(
                shift[(dummies[0], change_date + j, s)]
                for j in range(max_days // 2 + 1)
                for s in working_shift
                if (dummies[0], change_date + j, s) in shift)
            model.Add(consecutive_days <= max_days)
            length_dummies = len(dummies)
            if length_dummies < 1:
                continue
            for a in range(length_dummies - 1):
                dummy = dummies[a]
                dummy_second = dummies[a + 1]
                if dummy_workers[dummy]["end_date"] + 1 == dummy_workers[dummy_second]["start_date"]:
                    change_date = dummy_workers[dummy]["end_date"]
                    # We check windows that cross the change boundary:
                    # some days before change_date (original)
                    # and some days after (dummy)
                    if len([days_comp for days_comp in range(max_days // 2 + 1) if change_date - days_comp not in complete_cycle_days[dummy]]) == 0 and \
                       len([days_comp for days_comp in range(1, max_days // 2 + 1) if change_date + days_comp not in complete_cycle_days[dummy_second]]) == 0:
                        continue
                    consecutive_days = sum(
                        shift[(dummy, change_date - i, s)]
                        for i in range(max_days // 2 + 1)
                        for s in working_shift
                        if (dummy, change_date - i, s) in shift
                    ) + sum(
                        shift[(dummy_second, change_date + j, s)]
                        for j in range(max_days // 2 + 1)
                        for s in working_shift
                        if (dummy_second, change_date + j, s) in shift)
                    model.Add(consecutive_days <= max_days)

def maximum_continuous_working_special_days(model, shift, special_days, workers, working_shift, contract_type, max_days, period, complete_cycle_days, dummy_workers, workers_with_dummy):
    #limits maximum continuous working sundays and holidays
    for w in workers:
        if contract_type[w] in [4,5,6]:  # Check contract type for worker w
            for d in range(len(special_days) - max_days):  # Start from the first day and check each possible 7-day window
                # Sum all working shifts over a sliding window of contract maximum + 1 consecutive days
                if d + max_days < period[0] or d > period[1]:
                    continue
                next_special_days = special_days[d:d+max_days + 1]
                if len([days_comp for days_comp in next_special_days if days_comp not in complete_cycle_days[w]]) == 0:
                    continue
                special_days_shifts = sum(shift[(w, day, s)] for day in next_special_days for s in working_shift if (w, day, s) in shift )
                model.Add(special_days_shifts <= max_days)
    if dummy_workers:
        for w in workers_with_dummy:
            dummies = sorted(workers_with_dummy.get(w, {}).values())
            change_date = dummy_workers[dummies[0]]["start_date"] - 1
            next_special_days_pre = special_days[change_date - max_days : change_date]
            next_special_days_pos = special_days[change_date : change_date + max_days + 1]
            if len([days_comp for days_comp in next_special_days_pre if days_comp not in complete_cycle_days[w]]) == 0 and \
               len([days_comp for days_comp in next_special_days_pos if days_comp not in complete_cycle_days[dummies[0]]]) == 0:
                logger.info(f"worker {w} and dummie {dummies[0]} skipped restriction on day {change_date}")
                continue
            special_days_shifts = sum(shift[(get_dummy(workers_with_dummy, w, d), day, s)] 
                                      for day in special_days[d - max_days : d + max_days + 1] for s in working_shift 
                                      if (get_dummy(workers_with_dummy, w, d), day, s) in shift)
            model.Add(special_days_shifts <= max_days)
            length_dummies = len(dummies)
            if length_dummies < 1:
                continue
            for a in range(length_dummies - 1):
                dummy = dummies[a]
                dummy_second = dummies[a + 1]
                if dummy_workers[dummy]["end_date"] + 1 == dummy_workers[dummy_second]["start_date"]:
                    change_date = dummy_workers[dummy]["end_date"]
                    # We check windows that cross the change boundary:
                    # some days before change_date (original)
                    # and some days after (dummy)
                    next_special_days_pre = special_days[change_date - max_days : change_date]
                    next_special_days_pos = special_days[change_date : change_date + max_days + 1]
                    if len([days_comp for days_comp in next_special_days_pre if days_comp not in complete_cycle_days[dummy]]) == 0 and \
                       len([days_comp for days_comp in next_special_days_pos if days_comp not in complete_cycle_days[dummy_second]]) == 0:
                        continue
                    special_days_shifts = sum(shift[(get_dummy(workers_with_dummy, w, d), day, s)] 
                                             for day in special_days[d - max_days : d + max_days + 1] for s in working_shift 
                                             if (get_dummy(workers_with_dummy, w, d), day, s) in shift)
                    model.Add(special_days_shifts <= max_days)

def maximum_free_days(model, shift, days_of_year, workers, total_l, c3d): #restriçao desativada
    #constraint for maximum of free days in a year
    for w in workers:
        logger.info(f"maximum_free_days: workers {w}, total_l {total_l[w]}, c3d {c3d[w]}")
        # Build the sum by checking each shift type separately and only including existing keys
        free_day_shifts = []
        for d in days_of_year:
            if (w, d, "L") in shift:
                free_day_shifts.append(shift[(w, d, "L")])
            if (w, d, "LQ") in shift:
                free_day_shifts.append(shift[(w, d, "LQ")])
            if (w, d, "LD") in shift:
                free_day_shifts.append(shift[(w, d, "LD")])
        
        if free_day_shifts:  # Only add constraint if there are free day shifts
            model.Add(sum(free_day_shifts) == total_l.get(w, 0) - c3d.get(w, 0))

def free_days_sundays(model, shift, sundays, workers_no_contract_changes, working_days, total_l_dom, year_range, annual_variables, workers_with_dummy):
    for w in workers_no_contract_changes:
        if total_l_dom.get(w, 0) == 0:
            continue
        # Only consider special days that are in this worker's working days
        worker_sundays = [d for d in sundays if d in working_days[w] and year_range[0] <= d <= year_range[1] and get_annual_variables(annual_variables, w, d, "l_dom") == True]
        logger.info(f"Worker {w}, Sundays {worker_sundays}, total {total_l_dom.get(w, 0)}")
        model.Add(sum(shift[(w, d, "L")] for d in worker_sundays if (w, d, 'L') in shift) >= total_l_dom.get(w, 0))
    for w in workers_with_dummy:
        if total_l_dom.get(w, 0) == 0:
            continue
        worker_sundays = [d for d in sundays if d in working_days[get_dummy(workers_with_dummy, w, d)] \
                          and year_range[0] <= d <= year_range[1] and get_annual_variables(annual_variables, w, d, "l_dom") == True]
        logger.info(f"Worker contract changes {w}, Sundays {worker_sundays}, total {total_l_dom.get(w, 0)}")
        model.Add(sum(shift[(get_dummy(workers_with_dummy, w, d), d, "L")] for d in worker_sundays if (get_dummy(workers_with_dummy, w, d), d, 'L') in shift) >= total_l_dom.get(w, 0))

def tc_atribution(model, shift, workers, tc, special_days, working_days): #está a aplicar a regra a todos os colabs em vez de so tipo  6,
                                                                                        #mas pode ser ok se os contadores vierem a 0 para outros tipos de contrato
    # Constraint for TC shifts: only on special days and total equals tc[w]
    for w in workers:
        # Get special days that are in this worker's working days AND have TC variables
        worker_special_days = [d for d in special_days if d in working_days[w] and (w, d, "TC") in shift]
        if worker_special_days:
            model.Add(sum(shift[(w, d, "TC")] for d in worker_special_days) == tc.get(w, 0))

def working_days_special_days(model, shift, special_days, workers, working_days, l_d, contract_type):
    for w in workers:
        worker_special_days = [d for d in special_days if d in working_days[w]]
        if contract_type[w] in [4, 5]:
            # Only consider special days that are in this worker's working days
            model.Add(sum(shift[(w, d, "M")] + shift[(w, d, "T")] for d in worker_special_days if (w, d, "M") in shift or (w, d, "T") in shift) == l_d.get(w, 0))
        elif contract_type[w] == 6:
            model.Add(sum(shift[(w, d, "M")] + shift[(w, d, "T")] + shift[(w, d, "TC")] for d in worker_special_days if (w, d, "M") in shift or (w, d, "T") in shift or (w, d, "TC") in shift) == l_d.get(w, 0))

def LQ_attribution(model, shift, workers_no_contract_changes, working_days, l_q, c2d, year_range, annual_variables, workers_with_dummy):
    #preciso confirmar se tem de ser == ou manter como tem na salsa de >=
    for w in workers_no_contract_changes:
        if c2d.get(w, 0) == 0 and l_q.get(w, 0) == 0:
            continue
        model.Add(sum(shift[(w, d, "LQ")] for d in working_days[w] if (w, d, 'LQ') in shift) == l_q.get(w, 0) + c2d.get(w, 0))
    for w in workers_with_dummy:
        if c2d.get(w, 0) == 0 and l_q.get(w, 0) == 0:
            continue
        worker_saturdays = [d for d in range(year_range) if d in working_days[get_dummy(workers_with_dummy, w, d)] \
                            and get_annual_variables(annual_variables, w, d, "c2d") == True and (get_dummy(workers_with_dummy, w, d), d, "LQ") in shift]
        model.Add(sum(shift[(get_dummy(workers_with_dummy, w, d), d, "LQ")] for d in worker_saturdays if (get_dummy(workers_with_dummy, w, d), d, 'LQ') in shift) == l_q.get(w, 0) + c2d.get(w, 0))
    

def LD_attribution(model, shift, workers_no_contract_changes, working_days, l_d, year_range, workers_with_dummy):
    # #constraint for maximum of LD days in a year
    for w in workers_no_contract_changes:
        if l_d.get(w, 0) == 0:
            continue
        model.Add(sum(shift[(w, d, "LD")] for d in working_days[w] if (w, d, 'LD') in shift) == l_d.get(w, 0))
    for w in workers_with_dummy:
        if l_d.get(w, 0) == 0:
            continue
        worker_possible_days = [d for d in range(year_range) if d in working_days[get_dummy(workers_with_dummy, w, d)]]
        model.Add(sum(shift[(get_dummy(workers_with_dummy, w, d), d, "LD")] for d in worker_possible_days if (get_dummy(workers_with_dummy, w, d), d, 'LD') in shift) == l_d.get(w, 0))

def assign_week_shift(model, shift, workers_complete, week_to_days, working_days, worker_week_shift):
    # Contraint for workers shifts taking into account the worker_week_shift (each week a worker can either be )
        for w in workers_complete:
            for week in week_to_days.keys():  # Use only existing weeks instead of range(1, 53)
                # Iterate through days of the week for the current week
                for day in week_to_days[week]:
                    if day in working_days[w]:
                        # Morning shift constraint: worker can only be assigned to M if available for M
                        model.Add(shift[(w, day, "M")] <= worker_week_shift[(w, week, 'M')])
                        
                        # Afternoon shift constraint: worker can only be assigned to T if available for T
                        model.Add(shift[(w, day, "T")] <= worker_week_shift[(w, week, 'T')])

def working_day_shifts(model, shift, workers, working_days, check_shift, period):
# Check for the workers so that they can only have M, T, TC, L, LD and LQ in workingd days
  #  check_shift = ['M', 'T', 'L', 'LQ', "LD"]
    for w in workers:
        for d in working_days[w]:
            if not (period[0] < d < period[1]):
                continue
            model.add_exactly_one(shift[(w, d, s)] for s in check_shift if (w, d, s) in shift)

def free_day_next_2c(model, shift, workers, working_days, closed_holidays):
    for w in workers:
        for day in working_days[w]:
            # Get day of week (1 = Monday, 7 = Sunday)
            day_of_week = day % 7 + 1
            
            # Case 1: Friday (day_of_week == 5) followed by LQ on Saturday
            if (day_of_week == 5) and ((day + 1 in working_days[w]) or (day + 1 in closed_holidays)):
                    has_saturday_lq = model.NewBoolVar(f"has_saturday_lq_{w}_{day+1}")
                    has_saturday_f = model.NewBoolVar(f"has_saturday_f_{w}_{day+1}")
                    
                    # Link boolean variables to actual shift assignments
                    model.Add(shift.get((w, day + 1, "LQ"), 0) >= 1).OnlyEnforceIf(has_saturday_lq)
                    model.Add(shift.get((w, day + 1, "LQ"), 0) == 0).OnlyEnforceIf(has_saturday_lq.Not())
                    
                    model.Add(shift.get((w, day + 1, "F"), 0) >= 1).OnlyEnforceIf(has_saturday_f)
                    model.Add(shift.get((w, day + 1, "F"), 0) == 0).OnlyEnforceIf(has_saturday_f.Not())
                    
                    # Create a boolean for when either LQ or F is assigned on Saturday
                    has_saturday_special = model.NewBoolVar(f"has_saturday_special_{w}_{day+1}")
                    model.AddBoolOr([has_saturday_lq, has_saturday_f]).OnlyEnforceIf(has_saturday_special)
                    model.AddBoolAnd([has_saturday_lq.Not(), has_saturday_f.Not()]).OnlyEnforceIf(has_saturday_special.Not())
                    
                    # If Saturday has LQ or F, then Friday can't have L or LD
                    model.Add(shift.get((w, day, "L"), 0) + shift.get((w, day, "LD"), 0) == 0).OnlyEnforceIf(has_saturday_special)

        
            # Case 2: Monday (day_of_week == 0) preceded by L on Sunday
            if (day_of_week == 1) and ((day - 1 in working_days[w]) or (day - 1 in closed_holidays)):
                # Create boolean variables for Sunday shifts
                    has_sunday_l = model.NewBoolVar(f"has_sunday_l_{w}_{day-1}")
                    has_sunday_f = model.NewBoolVar(f"has_sunday_f_{w}_{day-1}")
                    
                    # Link boolean variables to actual shift assignments
                    model.Add(shift.get((w, day - 1, "L"), 0) >= 1).OnlyEnforceIf(has_sunday_l)
                    model.Add(shift.get((w, day - 1, "L"), 0) == 0).OnlyEnforceIf(has_sunday_l.Not())
                    
                    model.Add(shift.get((w, day - 1, "F"), 0) >= 1).OnlyEnforceIf(has_sunday_f)
                    model.Add(shift.get((w, day - 1, "F"), 0) == 0).OnlyEnforceIf(has_sunday_f.Not())
                    
                    # Create a boolean for when either L or F is assigned on Sunday
                    has_sunday_special = model.NewBoolVar(f"has_sunday_special_{w}_{day-1}")
                    model.AddBoolOr([has_sunday_l, has_sunday_f]).OnlyEnforceIf(has_sunday_special)
                    model.AddBoolAnd([has_sunday_l.Not(), has_sunday_f.Not()]).OnlyEnforceIf(has_sunday_special.Not())
                    
                    # If Sunday has L or F, then Monday can't have L or LD
                    model.Add(shift.get((w, day, "L"), 0) + shift.get((w, day, "LD"), 0) == 0).OnlyEnforceIf(has_sunday_special)

def no_free__days_close(model, shift, workers, working_days, cxx, contract_type, closed_holidays, days_of_year):
    for w in workers:
        # Only apply this constraint for workers with contract_type 6
        if cxx[w] == 0:
            # Collect all workdays for this worker
            all_work_days = [d for d in working_days[w] if d % 7 not in [6, 7]]
            # Add closed holidays and convert back to a list (keeping it sorted)
            all_work_days = sorted(list(set(all_work_days) | set(closed_holidays)))
            
            # Create variables for free days (L, LD, LQ)
            free_day_vars = {}
            for d in days_of_year:
                free_day = model.NewBoolVar(f"free_day_{w}_{d}")

                # A day is free if any free shift type is assigned
                free_shift_sum = sum(
                    shift.get((w, d, shift_type), 0) for shift_type in ["L", "LD", "LQ", "F"]
                )

                model.Add(free_shift_sum >= 1).OnlyEnforceIf(free_day)
                model.Add(free_shift_sum == 0).OnlyEnforceIf(free_day.Not())

                free_day_vars[d] = free_day
            # Constraint to prevent consecutive free days (L or LD)
            for i in range(len(all_work_days) - 1):
                d = all_work_days[i]
                # Check if the next day is actually consecutive in the calendar
                if i + 1 < len(all_work_days) and all_work_days[i + 1] == d + 1:
                    next_d = all_work_days[i + 1]
                    model.AddBoolOr([free_day_vars[d].Not(), free_day_vars[next_d].Not()])
        else:
            if contract_type[w] in [4, 5]:
                # For contract type 5, limit consecutive free days differently
                # Collect all workdays for this worker (excluding weekends)
                all_work_days = [d for d in working_days[w] if d % 7 not in [6, 7]]

                all_work_days = sorted(list(set(all_work_days) | set(closed_holidays)))

                # Create variables for consecutive free days
                # consecutive_free_days_count = model.NewIntVar(0, 100, f"consecutive_free_days_count_{w}")
                free_day_groups = []

                # A day is free if any free shift type is assigned
                free_day_vars = {}
                for d in all_work_days:
                    free_day = model.NewBoolVar(f"free_day_{w}_{d}")
                    
                    # Sum of all free shift types
                    free_shift_sum = sum(
                        shift.get((w, d, shift_type), 0) 
                        for shift_type in ["L", "LD", "LQ", "F"]
                        if (w, d, shift_type) in shift
                    )
                    
                    model.Add(free_shift_sum >= 1).OnlyEnforceIf(free_day)
                    model.Add(free_shift_sum == 0).OnlyEnforceIf(free_day.Not())
                    
                    free_day_vars[d] = free_day

                consecutive_pair = {} 
                # Count groups of consecutive free days
                for i in range(len(all_work_days) - 1):
                    current_day = all_work_days[i]
                    next_day = all_work_days[i + 1]
                    
                    # Check if these days are consecutive in the calendar
                    if next_day == current_day + 1:
                        # Create a variable that's true if both days are free
                        consecutive_pair[(w, current_day, next_day)] = model.NewBoolVar(f"consecutive_pair_{w}_{current_day}_{next_day}")
                        model.AddBoolAnd([free_day_vars[current_day], free_day_vars[next_day]]).OnlyEnforceIf(consecutive_pair[(w, current_day, next_day)])
                        model.AddBoolOr([free_day_vars[current_day].Not(), free_day_vars[next_day].Not()]).OnlyEnforceIf(consecutive_pair[(w, current_day, next_day)].Not())

                        free_day_groups.append(consecutive_pair[(w, current_day, next_day)])

                # Set the total count of consecutive free day pairs to be equal to cxx[w]

                if contract_type[w] == 5:
                    model.Add(sum(free_day_groups) == cxx[w])
                elif contract_type[w] == 4:
                    model.Add(sum(free_day_groups) >= cxx[w])

def space_LQs (model, shift, workers, working_days, t_lq, cal=None):
    # Constraint for LQs per month (0 <= LQ <= 2) for workers with LQ = 12
    # Approximate month mapping: day 1-31 = month 1, day 32-59 = month 2, etc.
    # This assumes days are numbered sequentially from start of year
    
    def day_to_month(day):
        """Convert day number to month (approximate)"""
        # Approximate days per month: 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
        month_starts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
        for i, start in enumerate(month_starts):
            if i == len(month_starts) - 1:  # December
                return 12
            elif day < month_starts[i + 1]:
                return i + 1
        return 12  # Default to December if beyond expected range
    
    for w in workers:
        if t_lq[w] == 12:
            for month in range(1, 13):  # Loop over all 12 months
                # Create a variable to count the number of LQ shifts for the worker in this month
                lq_in_month = model.NewIntVar(0, 2, f"lq_in_month_{w}_{month}")

                # Sum the LQ shifts for the worker in the current month
                lq_shifts_in_month = []
                for d in working_days[w]:
                    if day_to_month(d) == month:
                        lq_shifts_in_month.append(shift.get((w, d, "LQ"), 0))

                # Enforce the sum of LQ shifts in the month to be between 0 and 2
                model.Add(sum(lq_shifts_in_month) == lq_in_month)

                # The number of LQ shifts per month should be between 0 and 2
                model.Add(lq_in_month <= 2)
                model.Add(lq_in_month >= 0)


def day2_quality_weekend(model, shift, workers, working_days, sundays, c2d, contract_type, closed_holidays):
    for w in workers:
        if contract_type[w] in [4,5,6]:
            quality_2weekend_vars = []

            for d in working_days[w]:
                # Check if d is a Sunday and d-1 (Saturday) is in worker's working days or is a closed holiday
                if d in sundays and (d - 1 in working_days[w] or d - 1 in closed_holidays):  
                    # Boolean variables to check if the worker is assigned each shift
                    has_L_on_sunday = model.NewBoolVar(f"has_L_on_sunday_{w}_{d}")
                    has_LQ_on_saturday = None
                    
                    # Check if Sunday is a regular working day or a closed holiday
                    is_sunday_closed_holiday = d in closed_holidays
                    
                    # Enforce Sunday L shift condition (only if it's not a closed holiday)
                    if not is_sunday_closed_holiday:
                        model.Add(shift.get((w, d, "L"), 0) >= 1).OnlyEnforceIf(has_L_on_sunday)
                        model.Add(shift.get((w, d, "L"), 0) == 0).OnlyEnforceIf(has_L_on_sunday.Not())
                    else:
                        # If Sunday is a closed holiday, the worker automatically gets the day off
                        model.Add(has_L_on_sunday == 1)  # Automatically true since it's a holiday
                    
                    # Only check for LQ on Saturday if it's a working day for this worker
                    if d - 1 in working_days[w]:
                        has_LQ_on_saturday = model.NewBoolVar(f"has_LQ_on_saturday_{w}_{d-1}")
                        model.Add(shift.get((w, d - 1, "LQ"), 0) >= 1).OnlyEnforceIf(has_LQ_on_saturday)
                        model.Add(shift.get((w, d - 1, "LQ"), 0) == 0).OnlyEnforceIf(has_LQ_on_saturday.Not())
                    
                    # Create a binary variable to track whether this weekend qualifies
                    quality_weekend_2 = model.NewBoolVar(f"quality_weekend_2_{w}_{d}")
                    
                    # Different conditions based on whether Saturday is a working day or a closed holiday
                    if d - 1 in working_days[w]:
                        if is_sunday_closed_holiday:
                            # Sunday is a closed holiday, so we only need LQ on Saturday
                            model.Add(has_LQ_on_saturday == quality_weekend_2)
                        else:
                            # Both conditions need to be met: L on Sunday and LQ on Saturday
                            model.AddBoolAnd([has_L_on_sunday, has_LQ_on_saturday]).OnlyEnforceIf(quality_weekend_2)
                            model.AddBoolOr([has_L_on_sunday.Not(), has_LQ_on_saturday.Not()]).OnlyEnforceIf(quality_weekend_2.Not())
                    else:  # Saturday is a closed holiday
                        if is_sunday_closed_holiday:
                            # Both Saturday and Sunday are closed holidays, which automatically counts as a quality weekend
                            model.Add(quality_weekend_2 == 1)
                        else:
                            # Only need L on Sunday since Saturday is automatically a day off
                            model.Add(has_L_on_sunday == quality_weekend_2)

                    # Track the quality weekend count
                    quality_2weekend_vars.append(quality_weekend_2)

            # Constraint: The total number of quality weekends should equal c2d for the worker
            model.Add(sum(quality_2weekend_vars) == c2d.get(w, 0))


#----------------------------------------------------------------------------------------------------
def prio_2_3_workers(model, shift, workers, working_days, special_days, start_weekday, week_to_days, contract_type, working_shift):
    # Add constraint to prioritize workers with contract types 2 or 3 to work on Sundays and holidays
    for w in workers:
        if contract_type[w] in [2, 3]:
            # Create a variable to track if the worker works on each Sunday/holiday
            for special_day in special_days:
                if special_day in working_days[w]:
                    works_special_day = model.NewBoolVar(f"works_special_day_{w}_{special_day}")
                    
                    # Worker works on special day if assigned M or T shift
                    sum_work_shifts = sum(shift.get((w, special_day, s), 0) for s in working_shift)
                    model.Add(sum_work_shifts >= 1).OnlyEnforceIf(works_special_day)
                    model.Add(sum_work_shifts == 0).OnlyEnforceIf(works_special_day.Not())
                    
                    # Get the week number for this special day
                    week_number = (special_day + start_weekday - 2) // 7 + 1
                    week_days = [d for d in week_to_days.get(week_number, []) 
                                if d in working_days[w] and d not in special_days]
                    
                    # For each regular weekday in the same week
                    for regular_day in week_days:
                        works_regular_day = model.NewBoolVar(f"works_regular_day_{w}_{regular_day}")
                        
                        # Worker works on regular day if assigned M or T shift
                        sum_regular_shifts = sum(shift.get((w, regular_day, s), 0) for s in working_shift)
                        model.Add(sum_regular_shifts >= 1).OnlyEnforceIf(works_regular_day)
                        model.Add(sum_regular_shifts == 0).OnlyEnforceIf(works_regular_day.Not())
                        
                        # Prioritize special days: If worker doesn't work on special day,
                        # they shouldn't work on regular day (unless all special days are covered)
                        model.AddImplication(works_regular_day, works_special_day)


def compensation_days(model, shift, workers, working_days, special_days, week_to_days, contract_type, working_shift):
    #Define compensation days (LD) constraint for contract types 4, 5, 6
    possible_compensation_days = {}
    for w in workers:
        if contract_type[w] in [4, 5, 6]:
            possible_compensation_days[w] = {}
            for d in special_days:
                if d in working_days[w]:
                    # Determine the week of the special day
                    special_day_week = next((wk for wk, days in week_to_days.items() if d in days), None)
                
                    if special_day_week is None:
                        continue
                    
                    # Possible compensation weeks (current and next week)
                    possible_weeks = [
                        special_day_week, 
                        special_day_week + 1 if special_day_week < 52 else None
                    ]
                    
                    # Collect potential compensation days
                    compensation_days = []
                    for week in filter(None, possible_weeks):
                        compensation_days.extend([
                            day for day in week_to_days.get(week, [])
                            if (day in working_days[w] and 
                                day != d and 
                            day not in special_days
                            and day % 7 != 6)
                        ])
                    
                    # Store possible compensation days for this special day
                    possible_compensation_days[w][d] = compensation_days

    # Dictionary to store all compensation day variables
    all_comp_day_vars = {}

    # Dictionary to track compensation day usage
    comp_day_usage = {}

    # Main optimization loop
    for w in workers:
        if contract_type[w] in [4, 5, 6]:
            # Initialize the compensation day usage tracking for this worker
            comp_day_usage[w] = {}
        
            # Track which special days were worked
            worked_special_days = {}
            
            # First, create all the worked_special_day variables
            # ONLY for special days in this worker's working days
            for d in [day for day in special_days if day in working_days[w]]:
                # Create a boolean variable to track if the worker worked on this special day
                worked_special_day = model.NewBoolVar(f'worked_special_day_{w}_{d}')
                worked_special_days[d] = worked_special_day
                
                # Constraint to determine if worker worked on this special day
                if contract_type[w] == 6:
                    special_day_shift_vars = [
                    shift.get((w, d, s)) for s in working_shift 
                    if (w, d, s) in shift
                ]
                else:
                    special_day_shift_vars = [
                        shift.get((w, d, s)) for s in ["M", "T"] 
                        if (w, d, s) in shift
                    ]
                
                # If there are shift variables for this day, add a constraint
                if special_day_shift_vars:
                    # worked_special_day is true if any shift is assigned
                    model.AddBoolOr(special_day_shift_vars).OnlyEnforceIf(worked_special_day)
                    model.Add(sum(special_day_shift_vars) == 0).OnlyEnforceIf(worked_special_day.Not())
            
            # Now collect all possible compensation days for this worker
            all_possible_comp_days = set()
            for d in worked_special_days.keys():  # Use keys from worked_special_days to ensure alignment
                if d in possible_compensation_days[w]:
                    all_possible_comp_days.update(possible_compensation_days[w][d])
            
            # For each possible compensation day, create a variable indicating if it's used as a compensation day
            for comp_day in all_possible_comp_days:
                # Create a variable to track if this compensation day is used
                comp_day_used = model.NewBoolVar(f'comp_day_used_{w}_{comp_day}')
                comp_day_usage[w][comp_day] = comp_day_used
                
                # Create variables for which special day this compensation day is for
                special_day_assignment_vars = []
                
                # Only iterate through special days that exist in worked_special_days
                for special_day in worked_special_days.keys():
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
                        # Now this is safe because we know special_day is in worked_special_days
                        model.AddImplication(assignment_var, worked_special_days[special_day])
                        
                        # If this assignment is true, the compensation day is used
                        model.AddImplication(assignment_var, comp_day_used)
                        
                        # Constraint: If assignment is true, this day must be a valid day off (LD)
                        model.Add(shift[(w, comp_day, 'LD')] == 1).OnlyEnforceIf(assignment_var)
                
                # KEY CONSTRAINT: At most one special day can be assigned to this compensation day
                if len(special_day_assignment_vars) > 1:
                    # Extract just the assignment variables
                    assignment_vars = [var for _, var in special_day_assignment_vars]
                    # At most one assignment can be true
                    model.Add(sum(assignment_vars) <= 1)
            
            # For each special day, ensure it gets a compensation day if worked
            for d in worked_special_days.keys():
                # Get all variables for compensation days for this special day
                comp_day_vars = [
                    all_comp_day_vars[w][(d, comp_day)] 
                    for comp_day in possible_compensation_days[w].get(d, [])
                    if (d, comp_day) in all_comp_day_vars[w]
                ]
                
            # If the worker worked this special day, ensure one compensation day is assigned
                if comp_day_vars:
                    model.Add(sum(comp_day_vars) == 1).OnlyEnforceIf(worked_special_days[d])
                    model.Add(sum(comp_day_vars) == 0).OnlyEnforceIf(worked_special_days[d].Not())  

def limits_LDs_week(model, shift, week_to_days, workers, special_days):
    # Constraint: Weeks with only 1 special day can only be attributed 1 LD
    for week, days_in_week in week_to_days.items():
        # Count special days in this week
        special_days_in_week = [d for d in days_in_week if d in special_days]
        
        # If there's 1 or less special days in the week
        if len(special_days_in_week) <= 1:
            # Collect all LD shifts for all workers for the special days in this week
            ld_shifts_for_week = []
            for special_day in special_days_in_week:
                for w in workers:
                    if (w, special_day, "LD") in shift:
                        ld_shifts_for_week.append(shift[(w, special_day, "LD")])
            
            # Add constraint: sum of all LD shifts for this week must be <= 1
            model.Add(sum(ld_shifts_for_week) <= 1)


def one_free_day_weekly(model, shift, week_to_days, workers, working_days, contract_type, closed_holidays):
    # Constraint to ensure each worker has at least one free day per week
    for w in workers:
        # Only apply to workers with contract type not 0
        if contract_type[w] in [4,5,6]:
            # For each week in the worker's schedule
            for week, days_in_week in week_to_days.items():
                # Skip weeks 1 and 53 as indicated in the comment
                if week in [1, 53]:
                    continue
                
                # Only consider days that are in the worker's working days or closed holidays
                working_days_in_week = [d for d in days_in_week if d in working_days[w] or d in closed_holidays]
                
                # Skip weeks with no working days for this worker
                if not working_days_in_week:
                    continue

                # Sum all free day types (F, L, LQ, LD, A, V) for this worker in this week
                free_days_in_week = []
                for d in working_days_in_week:
                    for shift_type in ['F', 'L', 'LQ', 'LD', 'A', 'V']:
                        if (w, d, shift_type) in shift:
                            free_days_in_week.append(shift[(w, d, shift_type)])
                
                # Ensure at least one free day in the week if there are any working days
                if free_days_in_week:
                    model.Add(sum(free_days_in_week) >= 1)

#--------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------CONSTRAINTS 3-DAY-QUALITY-WEEKEND-------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------
def maxi_free_days_c3d(model, shift, workers, days_of_year, total_l, period):
    for w in workers:
        model.Add(sum(shift[(w, d, s)]  for d in days_of_year for s in ['L', 'LQ', 'LD'] if (w, d, s) in shift and period[0] <= d <= period[1]) == total_l.get(w, 0))
#--------------------------------------------------------------------------------------------------------------------------------------
def maxi_LQ_days_c3d(model, shift, workers, working_days, l_q, c2d, c3d):
#constraint for maximum of LQ days in a year
    for w in workers:
        for d in working_days[w]:
            model.Add(sum(shift[(w, d, "LQ")] for d in working_days[w] if (w, d, "LQ") in shift) == l_q.get(w, 0) + c2d.get(w, 0) + c3d.get(w, 0))



#--------------------------------------------------------------------------------------------------------------------------------------
def day3_quality_weekend(model, shift, workers, working_days, start_weekday, schedule_df, c3d, contract_type, closed_holidays):
    # Add constraints to ensure 3-day weekends for Fridays and Mondays
    quality_weekend_map = {}  # Dictionary to store quality weekend variables per worker
    for w in workers:
        if contract_type[w] in [4, 5, 6]:
            quality_3weekend_vars = []  # Store the 3-day weekend variables for this worker
            all_days = set(working_days[w]) | set(closed_holidays)
            for d in all_days:
                # Check for Saturday (day 6 of the week, index 5)
                if (d + start_weekday - 2 ) % 7 == 5:  # Saturday
                    # Check if Sunday is also a working day
                    if d + 1 in working_days[w] or d + 1 in closed_holidays:
                        # Check if Saturday and Sunday already form a 2-day quality weekend
                        saturday_shift = schedule_df.loc[schedule_df['Worker'] == w, f"Day_{d}"].values[0]
                        sunday_shift = schedule_df.loc[schedule_df['Worker'] == w, f"Day_{d+1}"].values[0]
                        
                        # Only proceed if Saturday has "LQ" and Sunday has "L"
                        if (saturday_shift == "LQ" and sunday_shift == "L") or (saturday_shift == "LQ" and sunday_shift == "F") or (saturday_shift == "F" and sunday_shift == "L") or (saturday_shift == "F" and sunday_shift == "F"):
                                # Check if Thursday isn't L or LD (if it's in working days)
                                thursday_constraint = True
                                if d - 2 in working_days[w]:
                                    thursday_shift = schedule_df.loc[schedule_df['Worker'] == w, f"Day_{d-2}"].values[0]
                                    if thursday_shift in ["L", "LD"]:
                                        thursday_constraint = False
                                
                                if thursday_constraint:
                                    # Boolean variable for this 3-day weekend
                                    quality_weekend_3_fri = model.NewBoolVar(f"quality_weekend_3_fri_{w}_{d-1}")
                                    
                                    # If this 3-day weekend is chosen, Friday must be LQ
                                    model.Add(shift.get((w, d-1, "LQ"), 0) >= 1).OnlyEnforceIf(quality_weekend_3_fri)
                                    
                                    # Calculate the month for this weekend
                                    month_of_weekend = ((d-1) // 30) % 12  # Using Friday's date
                                    
                                    quality_3weekend_vars.append((quality_weekend_3_fri, d-1, "Fri-Sat-Sun", month_of_weekend))
                                # Check if Tuesday isn't L or LD (if it's in working days)
                                tuesday_constraint = True
                                if d + 3 in working_days[w]:
                                    tuesday_shift = schedule_df.loc[schedule_df['Worker'] == w, f"Day_{d+3}"].values[0]
                                    if tuesday_shift in ["L", "LD"]:
                                        tuesday_constraint = False
                                
                                if tuesday_constraint:
                                    # Boolean variable for this 3-day weekend
                                    quality_weekend_3_mon = model.NewBoolVar(f"quality_weekend_3_mon_{w}_{d+2}")                              
                                    # If this 3-day weekend is chosen, Monday must be LQ
                                    model.Add(shift.get((w, d+2, "LQ"), 0) >= 1).OnlyEnforceIf(quality_weekend_3_mon)
                                    
                                    # Calculate the month for this weekend
                                    month_of_weekend = (d // 30) % 12  # Using Saturday's date
                                    
                                    quality_3weekend_vars.append((quality_weekend_3_mon, d, "Sat-Sun-Mon", month_of_weekend))
                
            # Add 5-month spacing constraints between quality weekends
            for i in range(len(quality_3weekend_vars)):
                for j in range(i+1, len(quality_3weekend_vars)):
                    weekend1 = quality_3weekend_vars[i]
                    weekend2 = quality_3weekend_vars[j]
                    
                    # Extract variables and months
                    quality_weekend_var1, _, _, month1 = weekend1
                    quality_weekend_var2, _, _, month2 = weekend2
                    
                    # Calculate month difference (considering year wraparound)
                    month_diff = min(
                        abs(month1 - month2),       # Direct difference
                        12 - abs(month1 - month2)   # Wraparound difference
                    )
                    
                    # If months are less than 5 apart, both weekends can't be active
                    if month_diff < 5:
                        # Create a constraint that at most one of these weekends can be active
                        model.AddBoolOr([quality_weekend_var1.Not(), quality_weekend_var2.Not()])
            
            # Constraint: The total number of 3-day quality weekends should match c3d[w]
            if quality_3weekend_vars:
                model.Add(sum(quality_weekend_3 for quality_weekend_3, _, _, _ in quality_3weekend_vars) == c3d.get(w, 0))
            
            # Store variables for later printing
            quality_weekend_map[w] = quality_3weekend_vars

    return quality_weekend_map
