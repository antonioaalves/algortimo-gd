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


def decision_variables(model, workers, shifts, first_day, last_day, absences, vacation_days, empty_days,
                       closed_holidays, fixed_days_off, fixed_LQs, shift_data, past_workers, fixed_compensation_days,
                       locked_days, forced_work_days, contract_type, dynamic_empty, complete_cycle_days, real_working_shift):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}

    closed_set = set(closed_holidays)
    logger.info(f"\tDEBUG closed days (everyone) {sorted(closed_set)}")
    for w in past_workers:
        empty_set = empty_days[w]
        vacation = vacation_days[w]
        fixed_LQs_set = fixed_LQs[w]
        fixed_days_set = fixed_days_off[w]
        absence_set = absences[w]
        shift_set = {f"shift_{value}_set": set(shift_data[f"shift_{value}"][w]) for value in real_working_shift}
        fixed_LD_set = fixed_compensation_days[w]

        mot = shift_M_set & shift_T_set #cuidado aqui, tenho de juntar todos os shifts
        if mot is not None:
        
            shift_M_set -= mot
            shift_T_set -= mot
            for d in mot:
                for value in real_working_shift:
                    shift[(w, d, value)] = model.NewBoolVar(f"{w}_Day{d}_{value}")
                model.add_exactly_one([shift[(w, d, value)] for value in real_working_shift])

        logger.info(f"For PAST WORKER {w}:")
        logger.info(f"\tDEBUG empty days {sorted(empty_set)}")
        logger.info(f"\tDEBUG vacation {sorted(vacation)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}")
        logger.info(f"\tDEBUG fixed M {sorted(shift_M_set)}")
        logger.info(f"\tDEBUG fixed T {sorted(shift_T_set)}")
        logger.info(f"\tDEBUG fixed MoT {sorted(mot)}")
        logger.info(f"\tDEBUG fixed LDs {sorted(fixed_LD_set)}")

        add_var(model, shift, w, fixed_LD_set, 'LD')
        for value in real_working_shift:
            add_var(model, shift, w, shift_set[f"shift_{value}_set"], value)
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
    shifts2.remove('LQ')
    shifts2.remove(real_working_shift)

    for w in workers:

        empty_set = empty_days[w]
        vacation = vacation_days[w] - empty_set
        fixed_LQs_set = fixed_LQs[w] - vacation - closed_holidays
        fixed_days_set = fixed_days_off[w] - vacation - fixed_LQs_set
        absence_set = absences[w] - fixed_days_set - fixed_LQs_set - vacation - empty_set
        forced_set = set(forced_work_days[w])
        shift_set = {f"shift_{value}_set": set(shift_data[f"shift_{value}"][w]) - fixed_days_set - closed_set - fixed_LQs_set - vacation - absence_set for value in real_working_shift}
        fixed_LD_set = set(fixed_compensation_days[w]) - fixed_days_set - fixed_LQs_set - vacation - absence_set
        complete_set = set(complete_cycle_days[w])

        SET_CODE_PRIORITY = [
            ("-", empty_set),
            ("V", vacation),
            ("LQ", fixed_LQs_set),
            ("L", fixed_days_set),
            ("A", absence_set),
            ("LD", fixed_LD_set),
        ]
        for value in real_working_shift:
            SET_CODE_PRIORITY.append((value, shift_set[f"shift_{value}_set"]))

        logger.info(f"For worker {w}:")
        logger.info(f"\tDEBUG empty days {sorted(empty_set)}")
        logger.info(f"\tDEBUG vacation {sorted(vacation)}")
        logger.info(f"\tDEBUG fixed lqs {sorted(fixed_LQs_set)}")
        logger.info(f"\tDEBUG fixed days {sorted(fixed_days_set)}")
        logger.info(f"\tDEBUG absence {sorted(absence_set)}")
        logger.info(f"\tDEBUG forced work days {sorted(forced_set)}")
        logger.info(f"\tDEBUG fixed lds {sorted(fixed_LD_set)}\n")
        if len(locked_days[w]) > 0:
            logger.info(f"\tDEBUG locked days {sorted(locked_days[w])}\n")
        if len(complete_set) > 0:
            logger.info(f"\tDEBUG complete cycle days {sorted(complete_set)}\n")
 
        if contract_type.get(w, 0) <= 4 and w in dynamic_empty:
            fixed_dynamic_empty = dynamic_empty[w]
            logger.info(f"\tDEBUG fixed dynamic empty days {sorted(fixed_dynamic_empty)}\n")
            blocked_days = absence_set | vacation | empty_set | closed_holidays | fixed_days_set | fixed_LQs_set | absence_set | fixed_LD_set | fixed_dynamic_empty
            add_var(model, shift, w, fixed_dynamic_empty, '-')
        else:
            blocked_days = absence_set | vacation | empty_set | closed_holidays | fixed_days_set | fixed_LQs_set | absence_set | fixed_LD_set

        for d in range(first_day[w], last_day[w] + 1):
            if d not in blocked_days:
                if d in locked_days[w]:
                    for code, possible_shift in SET_CODE_PRIORITY:
                        if d in possible_shift:
                            shift[(w, d, code)] = model.NewBoolVar(f"{w}_Day{d}_{code}")
                            model.Add(shift[(w, d, code)] == 1)
                            break
                    continue
                if d in forced_set:
                    for value in real_working_shift:
                        if d in shift_set[f"shift_{value}_set"]:
                            shift[(w, d, value)] = model.NewBoolVar(f"{w}_Day{d}_{value}")
                    #logger.info(f"{w} has forced work day {d} with shift {True if d in shift_M_set else False} M and  {True if d in shift_T_set else False} M")
                    continue
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")
                if contract_type.get(w, 0) <= 4:
                    shift[(w, d, '-')] = model.NewBoolVar(f"{w}_Day{d}_-")
                for value in real_working_shift:
                    if d in shift_set[f"shift_{value}_set"]:
                        shift[(w, d, value)] = model.NewBoolVar(f"{w}_Day{d}_{value}")
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
