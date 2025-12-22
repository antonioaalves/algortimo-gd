import numpy as np
from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config as get_config_manager

logger = get_logger(get_config_manager().system.project_name)

#read salsa funcs

def consecutive_days(vacations_in_week, nbr_vacations, cut_off, days):
    if nbr_vacations <= 2:
        #print("week too short")
        return False
    if cut_off == 5:
        if not all(day in vacations_in_week for day in days[2:5]):
            print(f"holidays not in a row {vacations_in_week}")
            return False
        if vacations_in_week[-1] != days[4]:
            print(f"holidays dont end on friday {vacations_in_week[-1]} {days[4]}")
            return False
    elif cut_off == 6:
        if not all(day in vacations_in_week for day in days[3:6]):
            print(f"holidays not in a row {vacations_in_week}")
            return False
        if vacations_in_week[-1] != days[5]:
            print(f"holidays dont end on saturday {vacations_in_week[-1]} {days[5]}")
            return False
    return True

def mixed_absences_days_off(absences, vacations, absences_in_week, nbr_absences, vacations_in_week, fixed_days_off, fixed_LQs, year_range, days_off, total, flag):
    if flag == 5:
        if total == 6 and len(days_off) == 0:
            last = absences_in_week[-1]
            if not(year_range[0] <= last <= year_range[1]):
                last = vacations_in_week[-1]
            if year_range[0] <= last <= year_range[1]:
                absences -= {last}
                vacations -= {last}
        elif len(days_off) == 1:
            last = absences_in_week[-1]
            if not(year_range[0] <= last <= year_range[1]):
                last = vacations_in_week[-1]
            if year_range[0] <= last <= year_range[1]:
                if days_off[-1] % 7 == 6 and last % 7 == 7:
                    absences -= {last}
                    vacations -= {last}
                    fixed_LQs |= {last}
                elif days_off[-1] % 7 == 7 and last % 7 == 6:
                    absences -= {last}
                    vacations -= {last}
                    fixed_days_off |= {last}
                    fixed_days_off -= {days_off[-1]}
                    fixed_LQs |= {days_off[-1]}
                else:
                    absences -= {last}
                    vacations -= {last}
                    fixed_days_off |= {last}
        else:
            if 13 in absences and 14 not in absences:
                print("3")
            if nbr_absences > 1:
                was_vacation_used = False
                l1 = absences_in_week[-1]
                if not(year_range[0] <= l1 <= year_range[1]):
                    l1 = vacations_in_week[-1]
                    was_vacation_used = True
                if year_range[0] <= l1 <= year_range[1]:
                    absences -= {l1}
                    fixed_days_off |= {l1}
                l2 = absences_in_week[-2]
                if not(year_range[0] <= l2 <= year_range[1]):
                    if was_vacation_used == True:
                        l2 = vacations_in_week[-2]
                    else:
                        l2 = vacations_in_week[-1]
                if year_range[0] <= l2 <= year_range[1]:
                    absences -= {l2}
                    fixed_days_off |= {l2}
            else:
                was_vacation_used = False
                l1 = absences_in_week[-1]
                if not(year_range[0] <= l1 <= year_range[1]):
                    l1 = vacations_in_week[-1]
                    was_vacation_used = True
                if year_range[0] <= l1 <= year_range[1]:
                    absences -= {l1}
                    vacations -= {l1}
                    fixed_days_off |= {l1}
                if was_vacation_used == True:
                    l2 = vacations_in_week[-2]
                else:
                    l2 = vacations_in_week[-1]
                if year_range[0] <= l2 <= year_range[1]:
                    absences -= {l1}
                    vacations -= {l2}
                    fixed_days_off |= {l2}
    else:
        l1 = absences_in_week[-1]
        if not(year_range[0] <= l1 <= year_range[1]):
            l1 = vacations_in_week[-1]
        if year_range[0] <= l1 <= year_range[1]:
            absences -= {l1}
            vacations -= {l1}
            fixed_days_off |= {l1}
            
    return absences, vacations, fixed_days_off, fixed_LQs

def days_off_atributtion(w, absences, vacations, fixed_days_off, fixed_LQs, week_to_days_salsa, closed_holidays, work_days_per_week, year_range):
    for week, days in week_to_days_salsa.items():
        if len(days) <= 6:
            continue

        days_set = set(days)
        days_off = days_set.intersection(fixed_days_off.union(fixed_LQs))
        absences_in_week = days_set.intersection(absences.union(closed_holidays))
        nbr_absences = len(absences_in_week)
        vacations_in_week = days_set.intersection(vacations.union(closed_holidays))
        nbr_vacations = len(vacations_in_week)
        total = nbr_vacations + nbr_absences - len(days_set.intersection(closed_holidays))

        if work_days_per_week is None or work_days_per_week[week - 1] == 5:

            if len(days_off) >= 2:
                #logger.warning(f"For week with absences {week}, {w} already has {days_off} day off, not changing anything")
                continue
            
            if nbr_absences < 5 and nbr_vacations < 6:
                if total > 5:
                    return mixed_absences_days_off(absences, vacations, sorted(absences_in_week), nbr_absences, sorted(vacations_in_week), fixed_days_off, fixed_LQs, year_range, sorted(days_off), total ,5)
                
                if consecutive_days(sorted(vacations_in_week), nbr_vacations, 5, days) == False:
                    continue

            atributing_days = sorted(days_set - closed_holidays)
            if len(days_off) == 1:
                logger.warning(f"For week with absences {week}, {w} already has {days_off} day off")
                only_day_off = sorted(days_off)[0]
                if only_day_off == atributing_days[-1] and only_day_off == days[6] and atributing_days[-2] == days[5]:
                    l2 = atributing_days[-2]
                    absences -= {l2}
                    vacations -= {l2}
                    fixed_LQs |= {l2}

                elif only_day_off == atributing_days[-2] and only_day_off == days[5] and atributing_days[-1] == days[6]:
                    l1 = atributing_days[-1]
                    absences -= {l1}
                    vacations -= {l1}
                    fixed_days_off |= {l1}
                    fixed_days_off -= {only_day_off}
                    fixed_LQs |= {only_day_off}
                else:
                    #last day insured not to be an already fixed day off
                    l1 = sorted(set(atributing_days) - {only_day_off})[-1]
                    absences -= {l1}
                    vacations -= {l1}
                    fixed_days_off |= {l1}
            else:
                l1 = atributing_days[-1]
                l2 = atributing_days[-2]

                if l1 == days[6] and l2 == days[5]:
                    absences -= {l2, l1}
                    vacations -= {l2, l1}
                    fixed_days_off |= {l1}
                    fixed_LQs |= {l2}
                else:
                    absences -= {l2,l1}
                    vacations -= {l2,l1}
                    fixed_days_off |= {l2,l1}
                
        else:
            if len(days_off) > 0:
                logger.warning(f"For week with absences {week}, {w} already has {days_off} day off, not changing. (6 working days week)")
                continue
            if nbr_absences <= 6 and nbr_vacations < 7:
                if total > 6:
                    return mixed_absences_days_off(absences, vacations, sorted(absences_in_week), nbr_absences, sorted(vacations_in_week), fixed_days_off, fixed_LQs, year_range, sorted(days_off), None,6)

                if consecutive_days(sorted(vacations_in_week), nbr_vacations, 6, days) == False:
                    continue
            atributing_days = sorted(days_set - closed_holidays)
            l1 = atributing_days[-1]
            absences -= {l1}
            vacations -= {l1}
            fixed_days_off |= {l1}
    
    return absences, vacations, fixed_days_off, fixed_LQs

def populate_week_seed_5_6(first_week_5_6, data_admissao, week_to_days):
    nbr_weeks = len(week_to_days)
    work_days_per_week = np.full(nbr_weeks, 5)

    # Find starting week, default to week 0 if not found
    week = next((wk for wk, val in week_to_days.items() if data_admissao in val), 1) - 1
    other = 6 if first_week_5_6 == 5 else 5
    work_days_per_week[week:] = np.tile(np.array([first_week_5_6, other]), (nbr_weeks // 2) + 1)[:nbr_weeks - week]

    return work_days_per_week.astype(int)

def populate_week_fixed_days_off(fixed_days_off, fixed_LQs, week_to_days):
    nbr_weeks = len(week_to_days)
    work_days_per_week = np.full(nbr_weeks, 5)
    week_5_days = 0
    for week, days in week_to_days.items():
        days_set = set(days)
        days_off_week = days_set.intersection(fixed_days_off.union(fixed_LQs))
        if len(days_off_week) > 1:
            week_5_days = week - 1
            break

    if week_5_days % 2 == 0:
        logger.info(f"Found week that has to be of 5 working days in week {week_5_days + 1}, "
                    f"with days {days_off_week} since its even, first week will start with 5")
        work_days_per_week= np.tile(np.array([5, 6]), (nbr_weeks // 2) + 1)[:nbr_weeks]
    else:
        logger.info(f"Found week that has to be of 5 working days in week {week_5_days + 1}, "
                    f"with days {days_off_week} since its odd, first week will start with 6")
        work_days_per_week= np.tile(np.array([6, 5]), (nbr_weeks // 2) + 1)[:nbr_weeks]

    return work_days_per_week.astype(int)

def check_5_6_pattern_consistency(w, fixed_days_off, fixed_LQs, week_to_days, work_days_per_week):
    for week, days in week_to_days.items():

        days_set = set(days)
        actual_days_off = len(days_set.intersection(fixed_days_off.union(fixed_LQs)))
        expected_days_off = 7 - work_days_per_week[week - 1]

        if actual_days_off > expected_days_off:
            logger.error(f"For worker {w}, in week {week} by contract they're supposed to work "
                         f"{work_days_per_week[week - 1]} days but have received {actual_days_off} "
                         f"days off: {days_set.intersection(fixed_days_off.union(fixed_LQs))}. Process will be Infeasible!")
            
#salsa_constraints funcs:

def compensation_days_calc(special_day_week, fixed_days_off, fixed_LQs, worker_absences, vacation_days, week_to_days, week_compensation_limit, working_days, period):
    compensation_days = []
    weeks_added = 0
    current_week = special_day_week

    while weeks_added < week_compensation_limit and current_week < 53:
        current_week += 1

        week_days = set(week_to_days.get(current_week, []))

        all_days_off = vacation_days.union(worker_absences.union(fixed_days_off.union(fixed_LQs)))

        available_days = working_days.intersection(week_days - all_days_off)
        if sorted(available_days)[0] >= period[1]:
            break
        if len(available_days) > 0:
            weeks_added += 1
            compensation_days.extend(available_days)

    return compensation_days

def ld_counter(shift_T, shift_M, fixed_ld, period, holidays):
    holidays_worked_before = []
    lds = 0
    for day in range(1, period[0] + 1):
        if day in holidays:
            if day in shift_T and day not in shift_M:
                holidays_worked_before.append(day)
            elif day in shift_M and day not in shift_T:
                holidays_worked_before.append(day)
        if day in fixed_ld:
            lds += 1
    del holidays_worked_before[:lds]
    
    return holidays_worked_before
