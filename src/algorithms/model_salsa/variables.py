#----------------------------------------DECISION VARIABLES----------------------------------------

def decision_variables(model, days_of_year, workers, shifts, first_day, last_day, absences, missing_days, empty_days, closed_holidays, fixed_days_off):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}
    shifts2 = shifts.copy()
    shifts2.remove('A')
    shifts2.remove('V')
    shifts2.remove('F')
    for w in workers:
        blocked_days = set(absences[w]) | set(missing_days[w]) | set(empty_days[w]) | set(closed_holidays) | set(fixed_days_off[w])
        for d in days_of_year:
            if first_day[w] <= d <= last_day[w] and d not in blocked_days:
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")
        for d in absences[w]:
            shift[(w, d, 'A')] = model.NewBoolVar(f"{w}_Day{d}_A")
            model.Add(shift[(w, d, 'A')] == 1)
        for d in missing_days[w]:
            shift[(w, d, 'V')] = model.NewBoolVar(f"{w}_Day{d}_V")
            model.Add(shift[(w, d, 'V')] == 1)
        for d in closed_holidays:
            shift[(w, d, 'F')] = model.NewBoolVar(f"{w}_Day{d}_F")
            model.Add(shift[(w, d, "F")] == 1)
        #for d in fixed_days_off[w]:
        #    shift[(w, d, 'L')] = model.NewBoolVar(f"{w}_Day{d}_L")
        #    model.Add(shift[(w, d, "L")] == 1)

    #52332 vs 31555 vs 25489
    return shift
