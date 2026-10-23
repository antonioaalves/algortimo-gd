import numpy as np
import pandas as pd
import datetime
from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config as get_config_manager
from collections import defaultdict

logger = get_logger(get_config_manager().system.project_name)

#read salsa funcs

def consecutive_days(vacations_in_week, nbr_vacations, cut_off, days):
    if nbr_vacations <= 2:
        #print("week too short")
        return False
    if cut_off == 5:
        if not all(day in vacations_in_week for day in days[2:5]):
            logger.info(f"holidays not in a row {vacations_in_week}")
            return False
        if vacations_in_week[-1] != days[4]:
            logger.info(f"holidays dont end on friday {vacations_in_week[-1]} {days[4]} but still changing to weekends days off")
            #return False
    elif cut_off == 6:
        if not all(day in vacations_in_week for day in days[3:6]):
            logger.info(f"holidays not in a row {vacations_in_week}")
            return False
        if vacations_in_week[-1] != days[5]:
            logger.info(f"holidays dont end on saturday {vacations_in_week[-1]} {days[5]} but still changing to weekends days off")
            #return False
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
        if len(days_set.intersection(vacations)) > 0 and len(days_set.intersection(absences)) > 0:
            total = nbr_vacations + nbr_absences - len(days_set.intersection(closed_holidays))
        else:
            total = 0
        if work_days_per_week is None or work_days_per_week[week - 1] == 5:
            if len(days_off) >= 2 or (nbr_absences + nbr_vacations < 2):
                #logger.warning(f"For week with absences {week}, {w} already has {days_off} day off, not changing anything")
                continue
            if total > 5:
                absences, vacations, fixed_days_off, fixed_LQs = mixed_absences_days_off(absences, vacations, sorted(absences_in_week), nbr_absences, sorted(vacations_in_week), fixed_days_off, fixed_LQs, year_range, sorted(days_off), total, 5)
                continue
            elif nbr_vacations > 2:
                if consecutive_days(sorted(vacations_in_week), nbr_vacations, 5, days) == False and nbr_vacations < 6:
                    continue
            elif nbr_absences < 5:
                continue
            atributing_days = sorted(days_set - closed_holidays)
            if len(days_off) == 1:
                logger.warning(f"For week with absences or holidays {week}, {w} already has {days_off} day off")
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
            if len(days_off) > 0 or (nbr_absences + nbr_vacations < 2):
                #logger.warning(f"For week with absences {week}, {w} already has {days_off} day off, not changing. (6 working days week)")
                continue
            if total > 6:
                absences, vacations, fixed_days_off, fixed_LQs =  mixed_absences_days_off(absences, vacations, sorted(absences_in_week), nbr_absences, sorted(vacations_in_week), fixed_days_off, fixed_LQs, year_range, sorted(days_off), None,6)
            elif nbr_vacations > 2:
                if consecutive_days(sorted(vacations_in_week), nbr_vacations, 6, days) == False:
                    continue
            elif nbr_absences < 6:
                continue
            atributing_days = sorted(days_set - closed_holidays)
            l1 = atributing_days[-1]
            absences -= {l1}
            vacations -= {l1}
            fixed_days_off |= {l1}
    
    return absences, vacations, fixed_days_off, fixed_LQs

def joining_template_with_contract_per_week(work_days_per_week, week_template, worker, contract_type):
    for week in range(len(work_days_per_week)):
        if week_template[week + 1] != 'A':
                work_days_per_week[week] = int(week_template[week + 1])
    return work_days_per_week

#salsa_constraints funcs:

def compensation_days_calc(special_day_week, fixed_days_off, fixed_LQs, worker_absences, vacation_days, week_to_days, compensation_limit, working_days, shift, w, fixed_lds, closed_days, period, day):
    compensation_days = []
    days_analysed = 0
    current_week = special_day_week
    absences = worker_absences.union(vacation_days.union(closed_days))
    all_days_off = fixed_days_off.union(fixed_LQs.union(absences.union(fixed_lds)))
    while days_analysed <= compensation_limit and current_week < len(week_to_days) + compensation_limit // 7:
        current_week += 1

        week_days = set(week_to_days.get(current_week, range(current_week * 7 - 7, current_week * 7)))
        if len(week_days.intersection(absences)) >= 5:
            continue
        available_days = {d for d in working_days.intersection(week_days - all_days_off) if (w, d, 'LD') in shift and d >= period[0] and d > day}

        days_analysed += len({d for d in week_days - absences if d >= period[0] and d > day})
        if days_analysed > compensation_limit:
            diff = days_analysed - compensation_limit
            if diff > len(available_days):
                break
            diff -= len(week_days.intersection(all_days_off))
            if diff > 0:
                available_days = set(sorted(available_days)[:-diff])

        if min(week_days) >= period[1]:
            available_days = {d for d in week_days - all_days_off if (w, d, 'LD') in shift}

        if len(available_days) > 0:
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

# optimization salsa

def group_creator(workers, grouper):
    groups = defaultdict(list)

    for w in workers:
        groups[grouper.get(w, 0)].append(w)

    return list(groups.values())
 
 #solver

def get_dummy(workers_with_dummy, w, d):
    if w in workers_with_dummy:
        for range, new_w in workers_with_dummy.get(w, {}).items():
            if d in range:
                if d == range[0]:
                    logger.info(f"For worker {w}, day {d} is in beginning range of dummy worker {new_w}")
                if d == range[-1]:
                    logger.info(f"For worker {w}, day {d} is in ending range of dummy worker {new_w}")
                return new_w
    return w

def get_annual_variables(annual_variables, w, d, variable):
    for range, new_w in annual_variables.get(w, {}).items():
        if d in range:
            if variable == "l_dom":
                #logger.info(f"Getting variable l_dom {annual_variables[w][range]['apply_l_dom']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_l_dom"]
            elif variable == "c2d":
                #logger.info(f"Getting variable c2d {annual_variables[w][range]['apply_c2d']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_c2d"]
            elif variable == "l_sab":
                #logger.info(f"Getting variable l_sab {annual_variables[w][range]['apply_l_sab']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_l_sab"]
            elif variable == "l_dom_or_sab":
                #logger.info(f"Getting variable l_dom_or_sab {annual_variables[w][range]['apply_l_dom_or_sab']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_l_dom_or_sab"]
            elif variable == "total_l":
                #logger.info(f"Getting variable total_l {annual_variables[w][range]['apply_total_l']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_total_l"]
            elif variable == "c3d":
                #logger.info(f"Getting variable c3d {annual_variables[w][range]['apply_c3d']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_c3d"]
            elif variable == "l_d":
                #logger.info(f"Getting variable l_d {annual_variables[w][range]['apply_l_d']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_l_d"]
            elif variable == "cxx":
                #logger.info(f"Getting variable cxx {annual_variables[w][range]['apply_cxx']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_cxx"]
            elif variable == "tc":
                #logger.info(f"Getting variable tc {annual_variables[w][range]['apply_tc']}, for {w} in day {d}, that got range{range}")
                return annual_variables[w][range]["apply_tc"]
    return True
                
def compensation_days_calc_with_contract_changes(special_day_week, fixed_days_off, fixed_LQs, worker_absences, vacation_days,
                                                 week_to_days, compensation_limit, working_days, shift, w, fixed_lds,
                                                 closed_days, period, day, workers_with_dummy):
    
    dummies = sorted(workers_with_dummy.get(w, {}).values())
    compensation_days = []
    days_analysed = 0
    current_week = special_day_week
    absences = worker_absences[w].union(vacation_days[w].union(closed_days))
    for dum in dummies:
        if w != dum:
            absences |= worker_absences[dum].union(vacation_days[dum])

    all_days_off = fixed_days_off[w].union(fixed_LQs[w].union(absences.union(fixed_lds[w])))
    for dum in dummies:
        if w != dum:
            all_days_off |= fixed_days_off[dum].union(fixed_LQs[dum].union(absences.union(fixed_lds[dum])))

    while days_analysed <= compensation_limit and current_week < len(week_to_days) + compensation_limit // 7:
        current_week += 1

        week_days = set(week_to_days.get(current_week, range(current_week * 7 - 7, current_week * 7)))
        if len(week_days.intersection(absences)) >= 5:
            continue
        available_days = {d for d in week_days - all_days_off if (get_dummy(workers_with_dummy, w, d), d, 'LD') in shift and d >= period[0] and d > day}
        days_analysed += len({d for d in week_days - absences if d >= period[0] and d > day})
        if days_analysed > compensation_limit:
            diff = days_analysed - compensation_limit
            if diff > len(available_days):
                break
            diff -= len(week_days.intersection(all_days_off))
            if diff > 0:
                available_days = set(sorted(available_days)[:-diff])

        if min(week_days) >= period[1]:
            available_days = {d for d in week_days - all_days_off if (get_dummy(workers_with_dummy, w, d), d, 'LD') in shift}

        if len(available_days) > 0:
            compensation_days.extend(available_days)

    return compensation_days