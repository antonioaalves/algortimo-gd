#----------------------------------------DECISION VARIABLES----------------------------------------

def decision_variables(model, days_of_year, workers, shifts, first_day, last_day):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}
    for w in workers:
        print(w, first_day[w], last_day[w])
        for d in days_of_year:
            if d >= first_day[w] and d <= last_day[w]:
                for s in shifts:
                    shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")
    exit(0)
    
    return shift