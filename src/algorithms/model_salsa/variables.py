from src.configuration_manager.manager import ConfigurationManager
from base_data_project.log_config import get_logger

_config_manager = ConfigurationManager()
logger = get_logger(_config_manager.project_name)

#----------------------------------------DECISION VARIABLES----------------------------------------

def add_var(model, shift, w, days, code, start_weekday):
    for d in days:
        if (code == 'L' and (d + start_weekday - 2) % 7 == 5 and d + 1 in days):
            shift[(w, d, 'LQ')] = model.NewBoolVar(f"{w}_Day{d}_'LQ'")
            model.Add(shift[(w, d, 'LQ')] == 1)
        else:
            shift[(w, d, code)] = model.NewBoolVar(f"{w}_Day{d}_{code}")
            model.Add(shift[(w, d, code)] == 1)


def decision_variables(model, days_of_year, workers, shifts, first_day, last_day, absences, missing_days, empty_days, closed_holidays, fixed_days_off, fixed_LQs, start_weekday):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}
    shifts2 = shifts.copy()
    shifts2.remove('A')
    shifts2.remove('V')
    shifts2.remove('F')
    shifts2.remove('-')
 
    closed_set = set(closed_holidays)
    for w in workers:
 
        
        empty_days_set = set(empty_days[w])
        missing_set = (set(missing_days[w]) | empty_days_set) - closed_set
        fixed_LQs_set = set(fixed_LQs[w])- missing_set - closed_set
        fixed_days_set = set(fixed_days_off[w]) - missing_set - closed_set - fixed_LQs_set
        absence_set = set(absences[w]) - fixed_days_set - closed_set - fixed_LQs_set - missing_set
        logger.info(f"For worker {w}:")
        logger.info(f"\tDEBUG empty days {sorted(empty_days_set)}")
        logger.info(f"\tDEBUG missing {sorted(missing_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}")
 
        blocked_days = absence_set | missing_set | empty_days_set | closed_set | fixed_days_set | fixed_LQs_set

 
        for d in range(first_day[w], last_day[w] + 1):
            if d not in blocked_days:
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")

        add_var(model, shift, w, missing_set - absence_set - closed_set - fixed_days_set - fixed_LQs_set - empty_days_set, 'V', start_weekday)
        add_var(model, shift, w, absence_set - closed_set - fixed_days_set - fixed_LQs_set - empty_days_set, 'A', start_weekday)
        add_var(model, shift, w, fixed_days_set - closed_set - fixed_LQs_set - empty_days_set, 'L', start_weekday)
        add_var(model, shift, w, fixed_LQs_set - closed_set - empty_days_set, 'LQ', start_weekday)
        add_var(model, shift, w, closed_set - empty_days_set, 'F', start_weekday)
        add_var(model, shift, w, empty_days_set, '-', start_weekday)
    #52332 vs 31555 vs 25489
    return shift
