from src.configuration_manager.instance import get_config
from base_data_project.log_config import get_logger

_config_manager = get_config()
logger = get_logger(_config_manager.project_name)

#----------------------------------------DECISION VARIABLES----------------------------------------

def add_var(model, shift, w, days, code):
    for d in days:
        if (code == 'L' and d % 7 == 6 and d + 1 in days):
            shift[(w, d, 'LQ')] = model.NewBoolVar(f"{w}_Day{d}_LQ")
            model.Add(shift[(w, d, 'LQ')] == 1)
        else:
            shift[(w, d, code)] = model.NewBoolVar(f"{w}_Day{d}_{code}")
            model.Add(shift[(w, d, code)] == 1)


def decision_variables(model, workers, shifts, first_day, last_day, absences, vacation_days,
                       empty_days, closed_holidays, fixed_days_off, fixed_LQs, shift_M, 
                       shift_T, past_workers, fixed_compensation_days, locked_days):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}

    closed_set = set(closed_holidays)
    for w in past_workers:
        empty_set = empty_days[w]
        vacation = vacation_days[w]
        fixed_LQs_set = fixed_LQs[w]
        fixed_days_set = fixed_days_off[w]
        absence_set = absences[w]
        shift_M_set = shift_M[w]
        shift_T_set = shift_T[w]
        fixed_LD_set = fixed_compensation_days[w]

        logger.info(f"For PAST WORKER {w}:")
        logger.info(f"\tDEBUG closed days {sorted(closed_set)}")
        logger.info(f"\tDEBUG empty days {sorted(empty_set)}")
        logger.info(f"\tDEBUG vacation {sorted(vacation)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}")
        logger.info(f"\tDEBUG fixed M {sorted(shift_M_set)}")
        logger.info(f"\tDEBUG fixed T {sorted(shift_T_set)}")
        logger.info(f"\tDEBUG fixed LDs {sorted(fixed_LD_set)}")

        add_var(model, shift, w, fixed_LD_set, 'LD')
        add_var(model, shift, w, shift_T_set, 'T')
        add_var(model, shift, w, shift_M_set, 'M')
        add_var(model, shift, w, vacation, 'V')
        add_var(model, shift, w, absence_set, 'A')
        add_var(model, shift, w, fixed_days_set, 'L')
        add_var(model, shift, w, fixed_LQs_set, 'LQ')
        add_var(model, shift, w, closed_set, 'F')
        add_var(model, shift, w, empty_set, '-')

    shifts2 = shifts.copy()
    shifts2.remove('A')
    shifts2.remove('V')
    shifts2.remove('F')
    shifts2.remove('-')
    shifts2.remove('M')
    shifts2.remove('T')
    shifts2.remove('LQ')
    print(f"locked days {locked_days}")
    for w in workers:

        empty_set = empty_days[w]
        vacation = vacation_days[w] - empty_set
        fixed_LQs_set = fixed_LQs[w] - vacation - closed_holidays
        fixed_days_set = fixed_days_off[w] - vacation - fixed_LQs_set
        absence_set = absences[w] - fixed_days_set - fixed_LQs_set - vacation - empty_set
        shift_M_set = set(shift_M[w]) - fixed_days_set - closed_set - fixed_LQs_set - vacation - absence_set
        shift_T_set = set(shift_T[w]) - fixed_days_set - closed_set - fixed_LQs_set - vacation - absence_set
        fixed_LD_set = set(fixed_compensation_days[w]) - fixed_days_set - fixed_LQs_set - vacation - absence_set

        SET_CODE_PRIORITY = [
            ("-", empty_set),
            ("V", vacation),
            ("LQ", fixed_LQs_set),
            ("L", fixed_days_set),
            ("A", absence_set),
            ("LD", fixed_LD_set),
            ("M", shift_M_set),
            ("T", shift_T_set),
        ]

        logger.info(f"For worker {w}:")
        logger.info(f"\tDEBUG empty days {sorted(empty_set)}")
        logger.info(f"\tDEBUG vacation {sorted(vacation)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}")
        #logger.info(f"\tDEBUG M shift {sorted(shift_M_set)}")
        #logger.info(f"\tDEBUG T shift {sorted(shift_T_set)}\n")
        logger.info(f"\tDEBUG fixed lds {sorted(fixed_LD_set)}\n")
        if len(locked_days[w]) > 0:
            logger.info(f"\tDEBUG locked days {sorted(locked_days[w])}\n")
 
        blocked_days = absence_set | vacation | empty_set | closed_holidays | fixed_days_set | fixed_LQs_set | absence_set | fixed_LD_set

        for d in range(first_day[w], last_day[w] + 1):
            if d not in blocked_days:
                if d in locked_days[w]:
                    for code, shift_set in SET_CODE_PRIORITY:
                        if d in shift_set:
                            shift[(w, d, code)] = model.NewBoolVar(f"{w}_Day{d}_{code}")
                            model.Add(shift[(w, d, code)] == 1)
                            break
                    continue
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")
                if d in shift_M_set:
                    shift[(w, d, 'M')] = model.NewBoolVar(f"{w}_Day{d}_M")
                if d in shift_T_set:
                    shift[(w, d, 'T')] = model.NewBoolVar(f"{w}_Day{d}_T")
                if d % 7 == 6:
                    shift[(w, d, 'LQ')] = model.NewBoolVar(f"{w}_Day{d}_LQ")

        add_var(model, shift, w, absence_set, 'A')
        add_var(model, shift, w, vacation - fixed_days_set - fixed_LQs_set, 'V')
        add_var(model, shift, w, fixed_days_set - empty_set, 'L')
        add_var(model, shift, w, fixed_LQs_set - empty_set, 'LQ')
        add_var(model, shift, w, fixed_LD_set - empty_set, 'LD')
        add_var(model, shift, w, closed_holidays - empty_set, 'F')
        add_var(model, shift, w, empty_set, '-')
    return shift
