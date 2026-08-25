from math import floor, ceil
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

def maximum_continuous_working_special_days(model, shift, special_days, workers, working_shift, contract_type, max_days, period, complete_cycle_days, dummy_workers, workers_with_dummy, locked_days):
    #limits maximum continuous working sundays and holidays
    for w in workers:
        if contract_type[w] in [4,5,6]:  # Check contract type for worker w
            for d in range(len(special_days) - max_days):  # Start from the first day and check each possible 7-day window
                # Sum all working shifts over a sliding window of contract maximum + 1 consecutive days
                if d + max_days < period[0] or d > period[1]:
                    continue
                next_special_days = special_days[d:d+max_days + 1]
                if len([days_comp for days_comp in next_special_days if days_comp not in complete_cycle_days[w] and days_comp not in locked_days[w]]) == 0:
                    continue
                special_days_shifts = sum(shift[(w, day, s)] for day in next_special_days for s in working_shift if (w, day, s) in shift)
                model.Add(special_days_shifts <= max_days)
    if dummy_workers:
        for w in workers_with_dummy:
            dummies = sorted(workers_with_dummy.get(w, {}).values())
            change_date = dummy_workers[dummies[0]]["start_date"] - 1
            next_special_days_pre = special_days[change_date - max_days : change_date]
            next_special_days_pos = special_days[change_date : change_date + max_days + 1]
            if len([days_comp for days_comp in next_special_days_pre if days_comp not in complete_cycle_days[w] and days_comp not in locked_days[w]]) == 0 and \
               len([days_comp for days_comp in next_special_days_pos if days_comp not in complete_cycle_days[dummies[0]] and days_comp not in locked_days[dummies[0]]]) == 0:
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
                    if len([days_comp for days_comp in next_special_days_pre if days_comp not in complete_cycle_days[dummy] and days_comp not in locked_days[dummy]]) == 0 and \
                       len([days_comp for days_comp in next_special_days_pos if days_comp not in complete_cycle_days[dummy_second] and days_comp not in locked_days[dummy_second]]) == 0:
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

def tc_atribution(model, shift, workers, tc, special_days, working_days, year_range): #está a aplicar a regra a todos os colabs em vez de so tipo  6,
                                                                                        #mas pode ser ok se os contadores vierem a 0 para outros tipos de contrato
    # Constraint for TC shifts: only on special days and total equals tc[w]
    for w in workers:
        # Get special days that are in this worker's working days AND have TC variables
        worker_special_days = [d for d in special_days if d in working_days[w] and year_range[0] <= d <= year_range[1] and (w, d, "TC") in shift]
        if worker_special_days:
            model.Add(sum(shift[(w, d, "TC")] for d in worker_special_days) == tc.get(w, 0))

def working_days_special_days(model, shift, special_days, workers_no_contract_changes, working_days, l_d, contract_type, real_working_shift, workers_with_dummy, year_range, annual_variables, complete_cycle_days, locked_days):
    for w in workers_no_contract_changes:
        if l_d.get(w, 0) == 0:
            continue
        worker_special_days = [d for d in special_days if d in working_days[w] and year_range[0] < d < year_range[1]]
        if len([d for d in worker_special_days if d in locked_days[w] or d in complete_cycle_days[w]]) >= l_d.get(w, 0):
            continue
        if contract_type[w] in [4, 5]:
            # Only consider special days that are in this worker's working days
            model.Add(sum(shift[(w, d, s)] for d in worker_special_days  for s in real_working_shift if (w, d, s) in shift) == l_d.get(w, 0))
        elif contract_type[w] == 6:
            model.Add(sum(shift[(w, d, s)] for d in worker_special_days for s in real_working_shift + ['TC'] if (w, d, s) in shift) == l_d.get(w, 0))
    for w in workers_with_dummy:
        if l_d.get(w, 0) == 0:
            continue
        worker_special_days = [d for d in special_days for s in real_working_shift if d in working_days[get_dummy(workers_with_dummy, w, d)] and year_range[0] < d < year_range[1] \
                               and get_annual_variables(annual_variables, w, d, "l_d") == True and (get_dummy(workers_with_dummy, w, d), d, s) in shift]
        if len([d for d in worker_special_days if d in locked_days[get_dummy(workers_with_dummy, w, d)] or d in complete_cycle_days[get_dummy(workers_with_dummy, w, d)]]) >= l_d.get(w, 0):
            continue
        if contract_type[w] in [4, 5]:
            # Only consider special days that are in this worker's working days
            model.Add(sum(shift[(get_dummy(workers_with_dummy, w, d), d, s)] for d in worker_special_days for s in real_working_shift if (get_dummy(workers_with_dummy, w, d), d, s) in shift) == l_d.get(w, 0))
        elif contract_type[w] == 6:
            model.Add(sum(shift[(get_dummy(workers_with_dummy, w, d), d, s)] for d in worker_special_days for s in real_working_shift + ['TC'] if (get_dummy(workers_with_dummy, w, d), d, s) in shift) == l_d.get(w, 0))

def saturday_L_constraint(model, shift, workers, working_days, period, working_shifts):
    # For each worker, constrain L on Saturday if L on Sunday
    for w in workers:
        for day in working_days[w]:
            if not (period[0] < day < period[1]):
                continue
            # Get day of week (6 = Saturday)
            if day % 7 == 6:
                if day + 1 in working_days[w]:
                    model.Add(shift.get((w, day, "LQ"), 0) + sum([shift.get((w, day + 1, s), 0) for s in working_shifts + ['LD']]) <= 1)

def LQ_attribution(model, shift, workers_no_contract_changes, working_days, t_lq, year_range, annual_variables, workers_with_dummy):
    #preciso confirmar se tem de ser == ou manter como tem na salsa de >=
    for w in workers_no_contract_changes:
        if t_lq.get(w, 0) == 0:
            continue
        model.Add(sum(shift[(w, d, "LQ")] for d in working_days[w] if (w, d, 'LQ') in shift and year_range[0] < d < year_range[1]) == t_lq.get(w, 0))
    for w in workers_with_dummy:
        if t_lq.get(w, 0) == 0:
            continue
        worker_saturdays = [d for d in range(year_range) if d in working_days[get_dummy(workers_with_dummy, w, d)] \
                            and get_annual_variables(annual_variables, w, d, "c2d") == True and (get_dummy(workers_with_dummy, w, d), d, "LQ") in shift]
        model.Add(sum(shift[(get_dummy(workers_with_dummy, w, d), d, "LQ")] for d in worker_saturdays if (get_dummy(workers_with_dummy, w, d), d, 'LQ') in shift) == t_lq.get(w, 0))
    

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
            day_of_week = day % 7 
            if day_of_week == 5 and ((day + 1 in working_days[w]) or (day + 1 in closed_holidays)) and ((day + 2 in working_days[w]) or (day + 2 in closed_holidays)):
                has_saturday_lq = model.NewBoolVar(f"has_saturday_lq_{w}_{day + 1}")
                has_saturday_f = model.NewBoolVar(f"has_saturday_f_{w}_{day + 1}")
                has_sunday_l = model.NewBoolVar(f"has_sunday_l_{w}_{day + 2}")
                has_sunday_f = model.NewBoolVar(f"has_sunday_f_{w}_{day + 2}")
                
                # Link boolean variables to actual shift assignments
                model.Add(shift.get((w, day + 2, "L"), 0) >= 1).OnlyEnforceIf(has_sunday_l)
                model.Add(shift.get((w, day + 2, "L"), 0) == 0).OnlyEnforceIf(has_sunday_l.Not())
                model.Add(shift.get((w, day + 2, "F"), 0) >= 1).OnlyEnforceIf(has_sunday_f)
                model.Add(shift.get((w, day + 2, "F"), 0) == 0).OnlyEnforceIf(has_sunday_f.Not())
                
                # Create a boolean for when either L or F is assigned on Sunday
                has_sunday_special = model.NewBoolVar(f"has_sunday_special_{w}_{day + 1}")
                model.AddBoolOr([has_sunday_l, has_sunday_f]).OnlyEnforceIf(has_sunday_special)
                model.AddBoolAnd([has_sunday_l.Not(), has_sunday_f.Not()]).OnlyEnforceIf(has_sunday_special.Not())
                
                # Link boolean variables to actual shift assignments
                model.Add(shift.get((w, day + 1, "LQ"), 0) >= 1).OnlyEnforceIf(has_saturday_lq)
                model.Add(shift.get((w, day + 1, "LQ"), 0) == 0).OnlyEnforceIf(has_saturday_lq.Not())
                model.Add(shift.get((w, day + 1, "F"), 0) >= 1).OnlyEnforceIf(has_saturday_f)
                model.Add(shift.get((w, day + 1, "F"), 0) == 0).OnlyEnforceIf(has_saturday_f.Not())
                
                # Create a boolean for when either LQ or F is assigned on Saturday
                has_saturday_special = model.NewBoolVar(f"has_saturday_special_{w}_{day + 1}")
                model.AddBoolOr([has_saturday_lq, has_saturday_f]).OnlyEnforceIf(has_saturday_special)
                model.AddBoolAnd([has_saturday_lq.Not(), has_saturday_f.Not()]).OnlyEnforceIf(has_saturday_special.Not())

                has_weekend_special = model.NewBoolVar(f"has_weekend_special_{w}_{day + 1}")
                model.AddBoolAnd([has_saturday_special, has_sunday_special]).OnlyEnforceIf(has_weekend_special)
                model.AddBoolOr([has_saturday_special.Not(), has_sunday_special.Not()]).OnlyEnforceIf(has_weekend_special.Not())
                
                # If Saturday has LQ or F, then Friday can't have L or LD
                model.Add(shift.get((w, day, "L"), 0) + shift.get((w, day, "LD"), 0) == 0).OnlyEnforceIf(has_weekend_special)
                model.Add(shift.get((w, day, "LQ"), 0) == 0).OnlyEnforceIf(has_weekend_special.Not())

            if day_of_week == 1 and ((day - 1 in working_days[w]) or (day - 1 in closed_holidays)) and ((day - 2 in working_days[w]) or (day - 2 in closed_holidays)):
                # Create boolean variables for Sunday shifts
                has_sunday_l = model.NewBoolVar(f"has_sunday_l_{w}_{day - 1}")
                has_sunday_f = model.NewBoolVar(f"has_sunday_f_{w}_{day - 1}")
                has_saturday_lq = model.NewBoolVar(f"has_saturday_lq_{w}_{day - 2}")
                has_saturday_f = model.NewBoolVar(f"has_saturday_f_{w}_{day - 2}")
                
                # Link boolean variables to actual shift assignments
                model.Add(shift.get((w, day - 1, "L"), 0) >= 1).OnlyEnforceIf(has_sunday_l)
                model.Add(shift.get((w, day - 1, "L"), 0) == 0).OnlyEnforceIf(has_sunday_l.Not())
                model.Add(shift.get((w, day - 1, "F"), 0) >= 1).OnlyEnforceIf(has_sunday_f)
                model.Add(shift.get((w, day - 1, "F"), 0) == 0).OnlyEnforceIf(has_sunday_f.Not())
                
                # Create a boolean for when either L or F is assigned on Sunday
                has_sunday_special = model.NewBoolVar(f"has_sunday_special_{w}_{day - 1}")
                model.AddBoolOr([has_sunday_l, has_sunday_f]).OnlyEnforceIf(has_sunday_special)
                model.AddBoolAnd([has_sunday_l.Not(), has_sunday_f.Not()]).OnlyEnforceIf(has_sunday_special.Not())

                # Link boolean variables to actual shift assignments
                model.Add(shift.get((w, day - 2, "LQ"), 0) >= 1).OnlyEnforceIf(has_saturday_lq)
                model.Add(shift.get((w, day - 2, "LQ"), 0) == 0).OnlyEnforceIf(has_saturday_lq.Not())
                model.Add(shift.get((w, day - 2, "F"), 0) >= 1).OnlyEnforceIf(has_saturday_f)
                model.Add(shift.get((w, day - 2, "F"), 0) == 0).OnlyEnforceIf(has_saturday_f.Not())

                # Create a boolean for when either LQ or F is assigned on Saturday
                has_saturday_special = model.NewBoolVar(f"has_saturday_special_{w}_{day - 2}")
                model.AddBoolOr([has_saturday_lq, has_saturday_f]).OnlyEnforceIf(has_saturday_special)
                model.AddBoolAnd([has_saturday_lq.Not(), has_saturday_f.Not()]).OnlyEnforceIf(has_saturday_special.Not())

                has_weekend_special = model.NewBoolVar(f"has_weekend_special_{w}_{day - 2}")
                model.AddBoolAnd([has_saturday_special, has_sunday_special]).OnlyEnforceIf(has_weekend_special)
                model.AddBoolOr([has_saturday_special.Not(), has_sunday_special.Not()]).OnlyEnforceIf(has_weekend_special.Not())
                
                model.Add(shift.get((w, day, "L"), 0) + shift.get((w, day, "LD"), 0) == 0).OnlyEnforceIf(has_weekend_special)
                model.Add(shift.get((w, day, "LQ"), 0) == 0).OnlyEnforceIf(has_weekend_special.Not())

            if day_of_week == 6 and ((day + 1 in working_days[w]) or (day + 1 in closed_holidays)):
                has_sunday_l = model.NewBoolVar(f"has_sunday_l_{w}_{day + 1}")
                has_sunday_f = model.NewBoolVar(f"has_sunday_f_{w}_{day + 1}")
                has_saturday_lq = model.NewBoolVar(f"has_saturday_lq_{w}_{day}")
                has_saturday_f = model.NewBoolVar(f"has_saturday_f_{w}_{day}")
                
                # Link boolean variables to actual shift assignments
                model.Add(shift.get((w, day + 1, "L"), 0) >= 1).OnlyEnforceIf(has_sunday_l)
                model.Add(shift.get((w, day + 1, "L"), 0) == 0).OnlyEnforceIf(has_sunday_l.Not())
                model.Add(shift.get((w, day + 1, "F"), 0) >= 1).OnlyEnforceIf(has_sunday_f)
                model.Add(shift.get((w, day + 1, "F"), 0) == 0).OnlyEnforceIf(has_sunday_f.Not())
                
                # Create a boolean for when either L or F is assigned on Sunday
                has_sunday_special = model.NewBoolVar(f"has_sunday_special_{w}_{day + 1}")
                model.AddBoolOr([has_sunday_l, has_sunday_f]).OnlyEnforceIf(has_sunday_special)
                model.AddBoolAnd([has_sunday_l.Not(), has_sunday_f.Not()]).OnlyEnforceIf(has_sunday_special.Not())

                # Link boolean variables to actual shift assignments
                model.Add(shift.get((w, day, "LQ"), 0) >= 1).OnlyEnforceIf(has_saturday_lq)
                model.Add(shift.get((w, day, "LQ"), 0) == 0).OnlyEnforceIf(has_saturday_lq.Not())
                model.Add(shift.get((w, day, "F"), 0) >= 1).OnlyEnforceIf(has_saturday_f)
                model.Add(shift.get((w, day, "F"), 0) == 0).OnlyEnforceIf(has_saturday_f.Not())

                # Create a boolean for when either LQ or F is assigned on Saturday
                has_saturday_special = model.NewBoolVar(f"has_saturday_special_{w}_{day}")
                model.AddBoolOr([has_saturday_lq, has_saturday_f]).OnlyEnforceIf(has_saturday_special)
                model.AddBoolAnd([has_saturday_lq.Not(), has_saturday_f.Not()]).OnlyEnforceIf(has_saturday_special.Not())

                has_weekend_special = model.NewBoolVar(f"has_weekend_special_{w}_{day}")
                model.AddBoolAnd([has_saturday_special, has_sunday_special]).OnlyEnforceIf(has_weekend_special)
                model.AddBoolOr([has_saturday_special.Not(), has_sunday_special.Not()]).OnlyEnforceIf(has_weekend_special.Not())
                model.Add(shift.get((w, day, "LQ"), 0) == 1).OnlyEnforceIf(has_weekend_special)



def no_free__days_close(model, shift, workers, working_days, cxx, contract_type, closed_holidays, days_of_year, period):
    for w in workers:
        # Only apply this constraint for workers with contract_type 6
        # Collect all workdays for this worker
        all_work_days = set(d for d in working_days[w] if d % 7 not in [6, 7])
        all_work_days = sorted(list(all_work_days | closed_holidays))
        if cxx[w] == 0:
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
        month_starts = [11, 42, 70, 101, 131, 162, 192, 223, 254, 284, 315, 345]
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


def day2_quality_weekend(model, shift, workers, working_days, sundays, c2d, contract_type, closed_holidays, year_range):
    for w in workers:
        if contract_type[w] in [4,5,6]:
            quality_2weekend_vars = []
            for d in working_days[w]:
                # Check if d is a Sunday and d-1 (Saturday) is in worker's working days or is a closed holiday
                if d in sundays and (d - 1 in working_days[w] or d - 1 in closed_holidays) and year_range[0] < d <= year_range[1]:  
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
                        model.Add(has_L_on_sunday == 1)

                    # Create a binary variable to track whether this weekend qualifies
                    quality_weekend_2 = model.NewBoolVar(f"quality_weekend_2_{w}_{d}")
                    # Different conditions based on whether Saturday is a working day or a closed holiday
                    if d - 1 in working_days[w]:
                        has_LQ_on_saturday = model.NewBoolVar(f"has_LQ_on_saturday_{w}_{d-1}")
                        model.Add(shift.get((w, d - 1, "LQ"), 0) >= 1).OnlyEnforceIf(has_LQ_on_saturday)
                        model.Add(shift.get((w, d - 1, "LQ"), 0) == 0).OnlyEnforceIf(has_LQ_on_saturday.Not())
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


def global_compensation_days(model, shift, workers, working_days, holidays, sundays, week_to_days, working_shift, holiday_rules, sunday_rules, fixed_days_off, fixed_LQs, worker_absences,
                             vacation_days, period, override_holiday_sunday, fixed_lds, holiday_past_lds, sunday_past_lds, closed_days, dummy_workers, workers_with_dummy):

    contingent_f = total_lds_f = contingent_d = total_lds_d = []

    last_compensation_f = 1
    last_compensation_d = 1
    for w in workers:
        last_day = period[1]
        if w in holiday_rules:
            last_compensation_f = holiday_rules[w]["compensation_limit"][max(holiday_rules[w]["compensation_limit"])]
        if w in sunday_rules:
            last_compensation_d = sunday_rules[w]["compensation_limit"][max(sunday_rules[w]["compensation_limit"])]
        biggest_limit = last_compensation_f if last_compensation_f > last_compensation_d else last_compensation_d

        last_day = max(working_days[w])
        for d in range(last_day + 1, last_day + biggest_limit + 1):
            shift[(w, d, 'LD')] = model.NewBoolVar(f"{w}_Day{d}_LD")

    contingent_f, total_lds_f = compensation_days(model, shift, workers, working_days, set(holidays), set(sundays), override_holiday_sunday, week_to_days, working_shift, holiday_rules, fixed_lds,
                                                      fixed_days_off, fixed_LQs, worker_absences, vacation_days, period, "holiday", holiday_past_lds, closed_days, dummy_workers, workers_with_dummy)

    contingent_d, total_lds_d = compensation_days(model, shift, workers, working_days, set(sundays), set(holidays), override_holiday_sunday, week_to_days, working_shift, sunday_rules, fixed_lds,
                                                      fixed_days_off, fixed_LQs, worker_absences, vacation_days, period, "sunday", sunday_past_lds, closed_days, dummy_workers, workers_with_dummy)
    ld_restriction(model, shift, workers, period, total_lds_f, total_lds_d, fixed_lds, contingent_f, contingent_d, dummy_workers, workers_with_dummy)
    return contingent_f, contingent_d

def compensation_days(model, shift, workers, working_days, special_days, special_days_2, override_holiday_sunday, week_to_days, working_shift, special_day_rules, fixed_lds,
                      fixed_days_off, fixed_LQs, worker_absences, vacation_days, period, day_type, past_special_days_worked, closed_days, dummy_workers, workers_with_dummy):
    possible_compensation_days = {}
    worked_special_days = {}
    amount_lds = {}
    for w in workers:
        original = w
        if w in dummy_workers:
            w = dummy_workers[w]['parent']
        if w not in amount_lds:
            amount_lds[w] = {}
            worked_special_days[w] = {}
            possible_compensation_days[w] = {}
        off = set(fixed_days_off[original])
        LQs = set(fixed_LQs[original])
        if w in special_day_rules:
            for d in [day for day in special_days if (day in working_days[original] - off - LQs) and period[0] <= day <= period[1]]:
                if d not in special_day_rules[w]["compensation_limit"]:
                    continue
                if d in special_days_2:
                    if w in override_holiday_sunday:
                        if day_type == "holiday":
                            if override_holiday_sunday[w][d] == 'N':  
                                continue
                        elif day_type == "sunday":
                            if override_holiday_sunday[w][d] == 'Y':
                                continue
                # Create a boolean variable to track if the worker worked on this special day
                amount_lds[w][d] = special_day_rules[w]["amount"][d]
                worked_special_day = model.NewBoolVar(f'worked_{day_type}_{w}_{d}')
                worked_special_days[w][d] = worked_special_day
                special_day_shift_vars = [shift.get((original, d, s)) for s in working_shift if (original, d, s) in shift]

                # If there are shift variables for this day, add a constraint
                if special_day_shift_vars:
                    # worked_special_day is true if any shift is assigned
                    model.AddBoolOr(special_day_shift_vars).OnlyEnforceIf(worked_special_day)
                    model.Add(sum(special_day_shift_vars) == 0).OnlyEnforceIf(worked_special_day.Not())
                # Determine the week of the special day
                special_day_week = next((wk for wk, days in week_to_days.items() if d in days), 1) - 1

                if special_day_week is None:
                    continue
                # Store possible compensation days for this special day
                if w not in dummy_workers and w not in workers_with_dummy:
                    possible_compensation_days[w][d] = compensation_days_calc(special_day_week, off, LQs, worker_absences[w], vacation_days[w], week_to_days,
                                                                              special_day_rules[w]["compensation_limit"][d], working_days[w], shift, w, fixed_lds, closed_days, period, d)
                    ammount = len(possible_compensation_days[w][d])
                    if ammount <= 2:
                        logger.warning(f"For {w}: day {d} got {ammount} possible_compensation_days!! Changing possible days to after year ends")
                        possible_compensation_days[w][d].extend(compensation_days_calc(max(week_to_days), off, LQs, worker_absences[w], vacation_days[w], week_to_days,
                                                                              special_day_rules[w]["compensation_limit"][d], working_days[w], shift, w, fixed_lds, closed_days, period, d))
                    logger.info(f"For {w}: day {d} got ammount = {ammount} possible_compensation_days: {possible_compensation_days[w][d]}")
                else:
                    possible_compensation_days[w][d] = compensation_days_calc_with_contract_changes(special_day_week, fixed_days_off, fixed_LQs, worker_absences, vacation_days, week_to_days,
                                                                              special_day_rules[w]["compensation_limit"][d], working_days, shift, w, fixed_lds, closed_days, period, d, workers_with_dummy)
                    ammount = len(possible_compensation_days[w][d])
                    if ammount <= 2:
                        logger.warning(f"For {w}: day {d} got {ammount} possible_compensation_days!! Changing possible days to after year ends")
                        possible_compensation_days[w][d].extend(compensation_days_calc_with_contract_changes(max(week_to_days), fixed_days_off, fixed_LQs, worker_absences, vacation_days, week_to_days,
                                                                              special_day_rules[w]["compensation_limit"][d], working_days, shift, w, fixed_lds, closed_days, period, d, workers_with_dummy))
                    logger.info(f"For {w}: day {d} got ammount = {ammount} possible_compensation_days: {possible_compensation_days[w][d]}")

        if w in past_special_days_worked:
            logger.info(f"past special days {w} worked: {past_special_days_worked[w]}")
            for d in past_special_days_worked[w]["days_&_limit"]:
                logger.info(f"calculated time: {past_special_days_worked[w]['days_&_limit'][d]} from day {d} to start date {period[0]}")
                if past_special_days_worked[w]["days_&_limit"][d] <= 0:
                    logger.warning(f"for Worker {w}: compensation for day {d}, before {period[0]}, will be impossible because there's no time remaing: {past_special_days_worked[w]['days_&_limit'][d]}")
                    continue
                else:
                    special_day_week = next((wk for wk, days in week_to_days.items() if period[0] in days), 1) - 1
                    if w not in dummy_workers and w not in workers_with_dummy:
                        possible_compensation_days[w][d] = compensation_days_calc(special_day_week, off, LQs, worker_absences[w], vacation_days[w], week_to_days,
                                                                                  past_special_days_worked[w]["days_&_limit"][d], working_days[w], shift, w, fixed_lds, closed_days, period, d)
                    else:
                        possible_compensation_days[w][d] = compensation_days_calc_with_contract_changes(special_day_week, fixed_days_off, fixed_LQs, worker_absences, vacation_days,
                                                                                                         week_to_days, past_special_days_worked[w]["days_&_limit"][d], working_days,
                                                                                                         shift, w, fixed_lds, closed_days, period, d, workers_with_dummy)
                    if len(possible_compensation_days[w][d]) != 0:
                        worked_special_day = model.NewConstant(1)
                    else:
                        logger.error(f"Worker {w} wont receive compensation for this day because no possible compensation days were available")
                        worked_special_day = model.NewBoolVar(f'worked_{day_type}_{w}_{d}')
                    worked_special_days[w][d] = worked_special_day
                    amount_lds[w][d] = past_special_days_worked[w]["days_&_amount"][d]       
                    logger.info(f"For {w}: day {d} before period {period[0]} got ammount = {len(possible_compensation_days[w][d])} possible_compensation_days: {possible_compensation_days[w][d]}")

    # Dictionary to track compensation day usage
    # Dictionary to store all compensation day variables
    comp_day_usage = {}
    contingent = {}
    total_lds = {}
    for w in workers:
        original = w
        if w in dummy_workers:
            w = dummy_workers[w]['parent']
        if len(worked_special_days[w]) == 0:
            continue
        # Initialize the compensation day usage tracking for this worker
        comp_day_usage[w] = {}
        all_possible_comp_days = set()
        # Now collect all possible compensation days for this worker
        for d in worked_special_days[w].keys():  # Use keys from worked_special_days[w] to ensure alignment
            if d in possible_compensation_days[w]:
                all_possible_comp_days.update(possible_compensation_days[w][d])

        # For each possible compensation day, create a variable indicating if it's used as a compensation day
        for comp_day in all_possible_comp_days:
            # Create a variable to track if this compensation day is used
            comp_day_used = model.NewBoolVar(f'comp_day_used_{w}_{comp_day}_{day_type}')
            comp_day_usage[w][comp_day] = comp_day_used
            
            # Create variables for which special day this compensation day is for
            d_assignment_vars = []
            
            # Only iterate through special days that exist in worked_special_days[w]
            for d in worked_special_days[w].keys():
                # Check if this special day has this compensation day as an option
                if comp_day in possible_compensation_days[w].get(d, []):
                    # Create a variable indicating this compensation day is assigned to this special day
                    assignment_var = model.NewBoolVar(f'worker_{w}_ld_{comp_day}_for_{day_type}_{d}')
                    d_assignment_vars.append((d, assignment_var))
                    # Store for later reference
                    if w not in contingent:
                        contingent[w] = {}
                    contingent[w][(d, comp_day)] = assignment_var
                    
                    # This compensation day is only assigned if the worker worked that special day
                    # Now this is safe because we know d is in worked_special_days[w]
                    model.AddImplication(assignment_var, worked_special_days[w][d])
                    
                    # If this assignment is true, the compensation day is used
                    model.AddImplication(assignment_var, comp_day_used)
                    
                    # Constraint: If assignment is true, this day must be a valid day off (LD)
                    if w in dummy_workers or w in workers_with_dummy:
                        current = get_dummy(workers_with_dummy, w, comp_day)
                        #if w != current:
                        #    logger.info(f"Worker {w} contract changes to {current} for LD {comp_day} and special work day {d}")
                        model.AddImplication(assignment_var, shift[(current, comp_day, 'LD')])
                    else:
                        model.AddImplication(assignment_var, shift[(w, comp_day, 'LD')])

            # KEY CONSTRAINT: At most one special day can be assigned to this compensation day
            if len(d_assignment_vars) > 1:
                # Extract just the assignment variables
                assignment_vars = [var for _, var in d_assignment_vars]
                # At most one assignment can be true
                model.Add(sum(assignment_vars) <= 1)

        # For each special day, ensure it gets a compensation day if worked
        for d in worked_special_days[w].keys():
            # Get all variables for compensation days for this special day
            comp_day_vars = [
                contingent[w][(d, comp_day)] 
                for comp_day in possible_compensation_days[w].get(d, [])
                if (d, comp_day) in contingent[w]
            ]

            # If the worker worked this special day, ensure one compensation day is assigned
            if comp_day_vars:
                # Normal case: enforce exactly 1 comp day if worked
                model.Add(sum(comp_day_vars) == amount_lds[w][d]).OnlyEnforceIf(worked_special_days[w][d])
                model.Add(sum(comp_day_vars) == 0).OnlyEnforceIf(worked_special_days[w][d].Not())
            else:
                model.Add(worked_special_days[w][d] == 0)
        # Total worked special_days
        total_lds[w] = sum([amount_lds[w][d] * worked_special_days[w][d] for d in worked_special_days[w]])
        # Total compensation days used
        total_comp_days_used = sum(comp_day_usage[w].values())
         
        # Enforce equality: number of LDs == number of worked special_days
        model.Add(total_comp_days_used == total_lds[w])
    return contingent, total_lds

def ld_restriction(model, shift, workers, period, total_lds_holidays_everyone, total_lds_sundays_everyone, fixed_lds, contingente_h, contingente_d, dummy_workers, workers_with_dummy):
    if workers_with_dummy:
        workers_no_changes = [w for w in workers if w not in dummy_workers and w not in workers_with_dummy]
    else:
        workers_no_changes = workers
    if total_lds_holidays_everyone is not None and total_lds_sundays_everyone is not None:
        for w in workers_no_changes:
            all_assignment_vars = {}
            if w in contingente_h:
                for (d, comp_day), var in contingente_h[w].items():
                    if comp_day not in all_assignment_vars:
                        all_assignment_vars[comp_day] = []
                    all_assignment_vars[comp_day].append(var)

            # Add Sunday assignments
            if w in contingente_d:
                for (d, comp_day), var in contingente_d[w].items():
                    if comp_day not in all_assignment_vars:
                        all_assignment_vars[comp_day] = []
                    all_assignment_vars[comp_day].append(var)
            for comp_day, vars_list in all_assignment_vars.items():
                if vars_list:
                    model.Add(sum(vars_list) <= 1)
            if fixed_lds[w] == []:
                past_lds = 0
            else:
                past_lds = len([d for d in fixed_lds[w] if d > period[0]])
            if w in total_lds_holidays_everyone and w in total_lds_sundays_everyone:
                model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == total_lds_holidays_everyone[w] + total_lds_sundays_everyone[w] + past_lds)
            elif w in total_lds_holidays_everyone:
                model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == total_lds_holidays_everyone[w] + past_lds)
            elif w in total_lds_sundays_everyone:
                model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == total_lds_sundays_everyone[w] + past_lds)
            else:
                model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == past_lds)
    elif total_lds_holidays_everyone is not None:
        for w in workers_no_changes:
            if fixed_lds[w] == []:
                past_lds = 0
            else:
                past_lds = len([d for d in fixed_lds[w] if d > period[0]])
            if w in total_lds_holidays_everyone:
                model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == total_lds_holidays_everyone[w] + past_lds)
            else:
                model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == past_lds)
    elif total_lds_sundays_everyone is not None:
        for w in workers_no_changes:
            if fixed_lds[w] == []:
                past_lds = 0
            else:
                past_lds = len([d for d in fixed_lds[w] if d > period[0]])
            if w in total_lds_sundays_everyone:
                model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == total_lds_sundays_everyone[w] + past_lds)
            else:
                model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == past_lds)
    else:
        for w in workers_no_changes:
            if fixed_lds[w] == []:
                past_lds = 0
            else:
                past_lds = len([d for d in fixed_lds[w] if d > period[0]])
            model.Add(sum(shift[(w, d, 'LD')] for d in range(period[0], 500) if (w, d, 'LD') in shift) == past_lds)


    if workers_no_changes != workers:
        if total_lds_holidays_everyone is not None and total_lds_sundays_everyone is not None:
            for w in workers_with_dummy:
                dummies = sorted(workers_with_dummy.get(w, {}).values())
                dummies.append(w)
                all_assignment_vars = {}
                if w in contingente_h:
                    for (d, comp_day), var in contingente_h[w].items():
                        if comp_day not in all_assignment_vars:
                            all_assignment_vars[comp_day] = []
                        all_assignment_vars[comp_day].append(var)

                    # Add Sunday assignments
                if w in contingente_d:
                    for (d, comp_day), var in contingente_d[w].items():
                        if comp_day not in all_assignment_vars:
                            all_assignment_vars[comp_day] = []
                        all_assignment_vars[comp_day].append(var)
                for comp_day, vars_list in all_assignment_vars.items():
                    if vars_list:
                        model.Add(sum(vars_list) <= 1)
                if fixed_lds[w] == []:
                    past_lds = 0
                else:
                    past_lds = len([d for d in fixed_lds[w] if d > period[0]])
                if w in total_lds_holidays_everyone and w in total_lds_sundays_everyone:
                    model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == total_lds_holidays_everyone[w] + total_lds_sundays_everyone[w] + past_lds)
                elif w in total_lds_holidays_everyone:
                    model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == total_lds_holidays_everyone[w] + past_lds)
                elif w in total_lds_sundays_everyone:
                    model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == total_lds_sundays_everyone[w] + past_lds)
                else:
                    model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == past_lds)
        elif total_lds_holidays_everyone is not None:
            for w in workers_with_dummy:
                if fixed_lds[w] == []:
                    past_lds = 0
                else:
                    past_lds = len([d for d in fixed_lds[w] if d > period[0]])
                if w in total_lds_holidays_everyone:
                    model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == total_lds_holidays_everyone[w] + past_lds)
                else:
                    model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == past_lds)
        elif total_lds_sundays_everyone is not None:
            for w in workers_with_dummy:
                if fixed_lds[w] == []:
                    past_lds = 0
                else:
                    past_lds = len([d for d in fixed_lds[w] if d > period[0]])
                if w in total_lds_sundays_everyone:
                    model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == total_lds_sundays_everyone[w] + past_lds)
                else:
                    model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == past_lds)
        else:
            for w in workers_with_dummy:
                if fixed_lds[w] == []:
                    past_lds = 0
                else:
                    past_lds = len([d for d in fixed_lds[w] if d > period[0]])
                model.Add(sum(shift[(dum, d, 'LD')] for dum in dummies for d in range(period[0], 500) if (dum, d, 'LD') in shift) == past_lds)

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


def free_days_week(model, shift, workers, week_to_days, working_days, admissao_proporcional, data_admissao,
                   data_demissao, fixed_days_off, fixed_LQs, contract_type, work_days_per_week, period, complete_cycle_days):
    for w in workers:
        if contract_type[w] <= 3:
            continue
        worker_admissao = data_admissao.get(w, 0)
        worker_demissao = data_demissao.get(w, 0)
        #logger.info(f"Worker {w}, Admissao: {worker_admissao}, Demissao: {worker_demissao}, Working Days: {working_days[w]}, Week Days: {week_to_days}")

        # Create variables for free days (L, F, LQ) by week
        for week, days in week_to_days.items():
            
            # Only include workdays (excluding weekends)
            week_work_days = [d for d in days  if d in working_days[w]]
            
            # Sort days to ensure they're in chronological order
            week_work_days.sort()
            # Skip if no working days for this worker in this week
            if not week_work_days:
                continue
            if week_work_days[-1] < period[0] or week_work_days[0] > period[1] or any(d in complete_cycle_days[w] for d in week_work_days):
                continue
            week_work_days_set = set(week_work_days)

            fixed_days_week = week_work_days_set.intersection(set(fixed_days_off[w]))
            fixed_lqs_week = week_work_days_set.intersection(set(fixed_LQs[w]))

            # Check if admissao or demissao day falls within this week
            is_admissao_week = (worker_admissao > 0 and worker_admissao in days)
            is_demissao_week = (worker_demissao > 0 and worker_demissao in days)

            tipo_contrato = contract_type.get(w, 0)
            actual_days_in_week = len(week_work_days)  # Total days in this week
            # If this is an admissao or demissao week, apply proportional calculation
            if is_admissao_week or is_demissao_week:
                # Calculate proportional requirement based on actual days in the week
                # Standard week has 7 days and requires 2 free days
                # Proportion: (actual_days / 7) * 2
                
                if tipo_contrato >= 5:
                    if 4 <= actual_days_in_week <= 5:
                        required_free_days = 1
                    elif actual_days_in_week < 4:
                        required_free_days = 0
                    else:
                        if work_days_per_week[w][week - 1] == 6:
                            required_free_days = 1
                        else:
                            required_free_days = 2
                else:
                    free_days_weekly = 2
                    proportion = actual_days_in_week / 7.0
                    proportion_days = proportion * free_days_weekly
                    if admissao_proporcional == 'floor':
                        required_free_days = max(0, int(floor(proportion_days)))

                    elif admissao_proporcional == 'ceil':
                        required_free_days = max(0, int(ceil(proportion_days)))
                    else:
                        required_free_days = max(0, int(floor(proportion_days)))
                logger.info(f"Worker {w}, Week {week} (Admissao/Demissao week), Days {week_work_days}: "
                            f" Required Free Days = {required_free_days}")
            
            else:
                if tipo_contrato >= 5:
                    if (tipo_contrato == 8 and work_days_per_week[w][week - 1] == 6 and actual_days_in_week >= 1) or tipo_contrato == 6:
                        required_free_days = 1
                    elif actual_days_in_week >= 2:
                        required_free_days = 2
                    elif actual_days_in_week == 1:
                        required_free_days = 1
                    else:
                         required_free_days = 0
                else:
                    if actual_days_in_week >= 2:
                        required_free_days = 2
                    elif actual_days_in_week == 1:
                            required_free_days = 1
                    else:
                         required_free_days = 0
            
                # logger.info(f"Worker {w}, Week {week} (Regular week), Days {week_work_days}: "
                #            f"Required Free Days = {required_free_days}")

            if required_free_days < (len(fixed_days_week) + len(fixed_lqs_week)):
                required_free_days = len(fixed_days_week) + len(fixed_lqs_week)
                logger.info(f" Worker {w} - Adjusted Required Free Days to {required_free_days} due to fixed days off: {fixed_days_week}")
            # Only add constraint if we require at least 1 free day
            if required_free_days >= 0:
                # Create a sum of free shifts for this worker in the current week
                if contract_type[w] == 6:
                    free_shift_sum = sum(shift.get((w, d, 'L'), 0) for d in week_work_days)
                else:
                    free_shift_sum = sum(shift.get((w, d, shift_type), 0) for d in week_work_days for shift_type in ["L", "LQ"])
                #logger.info(f"Adding constraint for Worker {w}, Week {week}, Required Free Days: {required_free_days}, Free Shift Sum Variable: {free_shift_sum}")
                if required_free_days == 2:
                    if (len(week_work_days) >= 2):
                        model.Add(free_shift_sum == required_free_days)
                elif required_free_days == 3:
                    if (len(week_work_days) >= 3):
                        model.Add(free_shift_sum == required_free_days)
                        #logger.info(f"Adding constraint for Worker {w}, Week {week}, Required Free Days: {required_free_days}, Free Shift Sum Variable: {free_shift_sum}")
                elif required_free_days == 1:
                    if (len(week_work_days) >= 1):
                        #logger.info(f"Adding constraint for Worker {w}, Week {week}, Required Free Days: {required_free_days}, Free Shift Sum Variable: {free_shift_sum}")
                        model.Add(free_shift_sum == required_free_days)
                elif required_free_days == 0:
                    model.Add(free_shift_sum == 0)

#--------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------CONSTRAINTS 3-DAY-QUALITY-WEEKEND-------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------------
def day3_quality_weekend(model, shift, workers, working_days, c3d, contract_type, closed_holidays):
    # Add constraints to ensure 3-day weekends for Fridays and Mondays
    for w in workers:
        if contract_type[w] in [4, 5, 6]:
            quality_3weekend_vars = []
            all_days = set(working_days[w]) | closed_holidays
            for d in all_days:
                if d % 7 != 6:  # Saturday
                    continue
                # Check if Sunday is also a working day
                if d + 1 in working_days[w] or d + 1 in closed_holidays:
                    thursday, friday, saturday, sunday, monday, tuesday = d - 2, d - 1, d, d + 1, d + 2, d + 3
                    if friday in working_days[w]:
                        friday_candidate = model.NewBoolVar(f"quality_weekend_3_fri_{w}_{friday}")
                        model.Add(shift.get((w, friday, "LQ"), 0) == 1).OnlyEnforceIf(friday_candidate)
                        # Saturday must be LQ/F
                        model.Add(shift.get((w, saturday, "LQ"), 0) + shift.get((w, saturday, "F"), 0) == 1).OnlyEnforceIf(friday_candidate)
                        # Sunday must be L/F
                        model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) == 1).OnlyEnforceIf(friday_candidate)
                        # Only proceed if Saturday has "LQ" and Sunday has "L"
                        # Thursday cannot be L/LD
                        if thursday in working_days[w]:
                            model.Add(shift.get((w, thursday, "L"), 0) + shift.get((w, thursday, "LD"), 0) == 0).OnlyEnforceIf(friday_candidate)
                        quality_3weekend_vars.append((friday_candidate, friday, "Fri-Sat-Sun", friday // 30,))
                        model.Add(shift.get((w, monday, "LQ"), 0)  + shift.get((w, monday, "L"), 0) == 0).OnlyEnforceIf(friday_candidate)
                    if monday in working_days[w]:
                        monday_candidate = model.NewBoolVar(f"quality_weekend_3_mon_{w}_{monday}")
                        # Monday must be LQ
                        model.Add(shift.get((w, monday, "LQ"), 0) == 1).OnlyEnforceIf(monday_candidate)
                        # Saturday must be LQ/F
                        model.Add(shift.get((w, saturday, "LQ"), 0) + shift.get((w, saturday, "F"), 0) == 1).OnlyEnforceIf(monday_candidate)
                        # Sunday must be L/F
                        model.Add(shift.get((w, sunday, "L"), 0) + shift.get((w, sunday, "F"), 0) == 1).OnlyEnforceIf(monday_candidate)
                        # Tuesday cannot be L/LD
                        if tuesday in working_days[w]:
                            model.Add(shift.get((w, tuesday, "L"), 0) + shift.get((w, tuesday, "LD"), 0) == 0).OnlyEnforceIf(monday_candidate)
                        quality_3weekend_vars.append((monday_candidate, monday, "Sat-Sun-Mon", saturday // 30,))
                        model.Add(shift.get((w, friday, "LQ"), 0) + shift.get((w, friday, "L"), 0) == 0).OnlyEnforceIf(monday_candidate)

            # Add 5-month spacing constraints between quality weekends
            for i in range(len(quality_3weekend_vars)):
                for j in range(i + 1, len(quality_3weekend_vars)):

                    var1, _, _, month1 = quality_3weekend_vars[i]
                    var2, _, _, month2 = quality_3weekend_vars[j]
                    month_diff = min(abs(month1 - month2), 12 - abs(month1 - month2))
                    if month_diff < 1:
                        model.AddBoolOr([var1.Not(), var2.Not()])

            if quality_3weekend_vars:
                model.Add(sum(weekend_var for weekend_var, _, _, _ in quality_3weekend_vars) == c3d.get(w, 0))