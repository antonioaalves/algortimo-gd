from src.configuration_manager.instance import get_config
from base_data_project.log_config import get_logger

_config_manager = get_config()
logger = get_logger(_config_manager.project_name)

#----------------------------------------DECISION VARIABLES----------------------------------------

def add_var(model, shift, w, days, code, start_weekday):
    for d in days:
        if (code == 'L' and (d + start_weekday - 2) % 7 == 5 and d + 1 in days):
            shift[(w, d, 'LQ')] = model.NewBoolVar(f"{w}_Day{d}_LQ")
            model.Add(shift[(w, d, 'LQ')] == 1)
        else:
            shift[(w, d, code)] = model.NewBoolVar(f"{w}_Day{d}_{code}")
            model.Add(shift[(w, d, code)] == 1)


def decision_variables(model, workers, shifts, first_day, last_day, absences, missing_days, 
                       empty_days, closed_holidays, fixed_days_off, fixed_LQs, shift_M, shift_T,
                       start_weekday, past_workers, fixed_compensation_days):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}

    closed_set = set(closed_holidays)
    for w in past_workers:
        empty_days_set = set(empty_days[w])
        missing_set = set(missing_days[w]) - closed_set
        fixed_LQs_set = set(fixed_LQs[w]) - missing_set - closed_set
        fixed_days_set = set(fixed_days_off[w]) - missing_set - closed_set - fixed_LQs_set
        absence_set = set(absences[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set
        shift_M_set = set(shift_M[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set - absence_set
        shift_T_set = set(shift_T[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set - absence_set - shift_M_set
        fixed_LD_set = set(fixed_compensation_days[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set - absence_set - shift_M_set - shift_T_set

        logger.info(f"For PAST WORKER {w}:")
        logger.info(f"\tDEBUG empty days {sorted(empty_days_set)}")
        logger.info(f"\tDEBUG missing {sorted(missing_set)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}")
        logger.info(f"\tDEBUG fixed M {sorted(shift_M_set)}")
        logger.info(f"\tDEBUG fixed T {sorted(shift_T_set)}")
        logger.info(f"\tDEBUG fixed LDs {sorted(fixed_LD_set)}")

        add_var(model, shift, w, fixed_LD_set, 'LD', start_weekday)
        add_var(model, shift, w, shift_T_set, 'T', start_weekday)
        add_var(model, shift, w, shift_M_set, 'M', start_weekday)
        add_var(model, shift, w, missing_set, 'V', start_weekday)
        add_var(model, shift, w, absence_set, 'A', start_weekday)
        add_var(model, shift, w, fixed_days_set, 'L', start_weekday)
        add_var(model, shift, w, fixed_LQs_set, 'LQ', start_weekday)
        add_var(model, shift, w, closed_set, 'F', start_weekday)
        add_var(model, shift, w, empty_days_set, '-', start_weekday)

    shifts2 = shifts.copy()
    shifts2.remove('A')
    shifts2.remove('V')
    shifts2.remove('F')
    shifts2.remove('-')
    shifts2.remove('M')
    shifts2.remove('T')

    for w in workers:
 
        empty_days_set = set(empty_days[w])
        missing_set = (set(missing_days[w]) | empty_days_set) - closed_set
        fixed_LQs_set = set(fixed_LQs[w]) - missing_set - closed_set
        fixed_days_set = set(fixed_days_off[w]) - missing_set - closed_set - fixed_LQs_set
        absence_set = set(absences[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set
        shift_M_set = set(shift_M[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set - absence_set
        shift_T_set = set(shift_T[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set - absence_set

        logger.info(f"For worker {w}:")
        logger.info(f"\tDEBUG empty days {sorted(empty_days_set)}")
        logger.info(f"\tDEBUG missing {sorted(missing_set)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}\n")
 
        blocked_days = absence_set | missing_set | empty_days_set | closed_set | fixed_days_set | fixed_LQs_set | absence_set

        for d in range(first_day[w], last_day[w] + 1):
            if d not in blocked_days:
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")
                if d in shift_M_set:
                    shift[(w, d, 'M')] = model.NewBoolVar(f"{w}_Day{d}_M")
                if d in shift_T_set:
                    shift[(w, d, 'T')] = model.NewBoolVar(f"{w}_Day{d}_T")

        add_var(model, shift, w, missing_set - absence_set - closed_set - fixed_days_set - fixed_LQs_set - empty_days_set, 'V', start_weekday)
        add_var(model, shift, w, absence_set - closed_set - fixed_days_set - fixed_LQs_set - empty_days_set, 'A', start_weekday)
        add_var(model, shift, w, fixed_days_set - closed_set - fixed_LQs_set - empty_days_set, 'L', start_weekday)
        add_var(model, shift, w, fixed_LQs_set - closed_set - empty_days_set, 'LQ', start_weekday)
        add_var(model, shift, w, closed_set - empty_days_set, 'F', start_weekday)
        add_var(model, shift, w, empty_days_set, '-', start_weekday)
    return shift
