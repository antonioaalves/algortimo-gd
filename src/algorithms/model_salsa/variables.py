#----------------------------------------DECISION VARIABLES----------------------------------------

def decision_variables(model, days_of_year, workers, shifts, first_day, last_day, absences, missing_days, empty_days):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}
    shifts2 = shifts.copy()
    shifts2.remove('A')
    shifts2.remove('V')
    for w in workers:
        for d in days_of_year:
            if d >= first_day[w] and d <= last_day[w] and d not in absences[w] and d not in missing_days[w] and d not in empty_days[w]:
                for s in shifts2:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")
        for d in absences[w]:
            shift[(w, d, 'A')] = model.NewBoolVar(f"{w}_Day{d}_A")
            model.Add(shift[(w, d, 'A')] == 1)
        for d in missing_days[w]:
            shift[(w, d, 'V')] = model.NewBoolVar(f"{w}_Day{d}_V")
            model.Add(shift[(w, d, 'V')] == 1)
    #45990 vs 40264 vs 400013 vs 34181 vs 32251
    return shift