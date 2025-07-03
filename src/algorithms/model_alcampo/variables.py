#----------------------------------------DECISION VARIABLES----------------------------------------

def decision_variables(model, days_of_year, workers, shifts):
    # Create decision variables (binary: 1 if person is assigned to shift, 0 otherwise)
    shift = {}
    for w in workers:
        for d in days_of_year:
            for s in shifts:
                shift[(w, d, s)] = model.NewBoolVar(f"{w}_Day{d}_{s}")
    
    return shift