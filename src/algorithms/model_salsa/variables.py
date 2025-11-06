from src.config import PROJECT_NAME
from base_data_project.log_config import get_logger

logger = get_logger(PROJECT_NAME)

#----------------------------------------DECISION VARIABLES----------------------------------------

def add_var(model, shift, w, days, code, start_weekday):
    for d in days:
        if (code == 'L' and (d + start_weekday - 2) % 7 == 5 and d + 1 in days):
            shift[(w, d, 'LQ')] = model.NewBoolVar(f"{w}_Day{d}_'LQ'")
            model.Add(shift[(w, d, 'LQ')] == 1)
        else:
            shift[(w, d, code)] = model.NewBoolVar(f"{w}_Day{d}_{code}")
            model.Add(shift[(w, d, code)] == 1)


def decision_variables(model, workers, shifts, first_day, last_day, absences, vacation_days, 
                       empty_days, closed_holidays, fixed_days_off, fixed_LQs, fixed_M, fixed_T,
                       start_weekday, past_workers, fixed_compensation_days):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}
    shifts2 = shifts.copy()
    shifts2.remove('A')
    shifts2.remove('V')
    shifts2.remove('F')
    shifts2.remove('-')

 
    for w in past_workers:
        empty_set = empty_days[w]
        vacation = vacation_days[w] - empty_set
        fixed_LQs_set = set(fixed_LQs[w]) - vacation - closed_holidays
        fixed_days_set = fixed_days_off[w] - vacation - fixed_LQs_set
        absence_set = absences[w] - fixed_days_set - fixed_LQs_set - vacation - empty_set
        fixed_M_set = set(fixed_M[w]) - fixed_days_set - fixed_LQs_set - vacation - absence_set
        fixed_T_set = set(fixed_T[w]) - fixed_days_set - fixed_LQs_set - vacation - absence_set - fixed_M_set
        fixed_c_set = set(fixed_compensation_days[w]) - fixed_days_set - fixed_LQs_set - vacation - absence_set - fixed_M_set - fixed_T_set

        logger.info(f"For PAST WORKER {w}:")
        logger.info(f"\tDEBUG empty days {sorted(empty_set)}")
        logger.info(f"\tDEBUG vacation {sorted(vacation)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}")
        logger.info(f"\tDEBUG fixed M {sorted(fixed_M_set)}")
        logger.info(f"\tDEBUG fixed T {sorted(fixed_T_set)}")
        logger.info(f"\tDEBUG fixed LDs {sorted(fixed_c_set)}")

        add_var(model, shift, w, fixed_c_set, 'LD', start_weekday)
        add_var(model, shift, w, fixed_T_set, 'T', start_weekday)
        add_var(model, shift, w, fixed_M_set, 'M', start_weekday)
        add_var(model, shift, w, vacation, 'V', start_weekday)
        add_var(model, shift, w, absence_set, 'A', start_weekday)
        add_var(model, shift, w, fixed_days_set, 'L', start_weekday)
        add_var(model, shift, w, fixed_LQs_set, 'LQ', start_weekday)
        add_var(model, shift, w, closed_holidays, 'F', start_weekday)
        add_var(model, shift, w, empty_set, '-', start_weekday)

    for w in workers:
 
        empty_set = empty_days[w]
        vacation = vacation_days[w] - empty_set
        fixed_LQs_set = set(fixed_LQs[w]) - vacation - closed_holidays
        fixed_days_set = fixed_days_off[w] - vacation - fixed_LQs_set
        absence_set = absences[w] - fixed_days_set - fixed_LQs_set - vacation - empty_set

        logger.info(f"For worker {w}:")
        logger.info(f"\tDEBUG empty days {sorted(empty_set)}")
        logger.info(f"\tDEBUG vacation {sorted(vacation)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}")
 
        blocked_days = absence_set | vacation | empty_set | closed_holidays | fixed_days_set | fixed_LQs_set | absence_set

        for d in range(first_day[w], last_day[w] + 1):
            if d not in blocked_days:
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")

        add_var(model, shift, w, absence_set, 'A', start_weekday)
        add_var(model, shift, w, vacation - fixed_days_set - fixed_LQs_set, 'V', start_weekday)
        add_var(model, shift, w, fixed_days_set - empty_set, 'L', start_weekday)
        add_var(model, shift, w, fixed_LQs_set - empty_set, 'LQ', start_weekday)
        add_var(model, shift, w, closed_holidays - empty_set, 'F', start_weekday)
        add_var(model, shift, w, empty_set, '-', start_weekday)
    return shift
