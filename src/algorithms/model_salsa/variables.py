#----------------------------------------DECISION VARIABLES----------------------------------------

def add_var(model, shift, w, days, code):
    for d in days:
        shift[(w, d, code)] = model.NewBoolVar(f"{w}_Day{d}_{code}")
        model.Add(shift[(w, d, code)] == 1)

def decision_variables(model, days_of_year, workers, shifts, first_day, last_day, absences, missing_days, empty_days, closed_holidays, fixed_days_off, fixed_LQs):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}
    shifts2 = shifts.copy()
    shifts2.remove('A')
    shifts2.remove('V')
    shifts2.remove('F')

    closed_set = set(closed_holidays)
    for w in workers:

        missing_set = set(missing_days[w])
        absence_set = set(absences[w])
        empty_days_set = set(empty_days[w])
        fixed_days_set = set(fixed_days_off[w])
        fixed_LQs_set = set(fixed_LQs[w])

        blocked_days = absence_set | missing_set | empty_days_set | closed_set | fixed_days_set | fixed_LQs_set

        for d in range(first_day[w], last_day[w] + 1):
            if d not in blocked_days:
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")

        add_var(model, shift, w, missing_set - absence_set - closed_set - fixed_days_set - fixed_LQs_set, 'V')
        add_var(model, shift, w, absence_set - closed_set - fixed_days_set - fixed_LQs_set, 'A')
        add_var(model, shift, w, fixed_days_set - closed_set - fixed_LQs_set, 'L')
        add_var(model, shift, w, fixed_LQs_set - closed_set, 'LQ')
        add_var(model, shift, w, closed_set, 'F')

    #52332 vs 31555 vs 25489
    return shift
