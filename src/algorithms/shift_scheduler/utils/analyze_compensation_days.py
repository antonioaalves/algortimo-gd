def analyze_compensation_days(solver, all_comp_day_vars):
    """
    Analyze compensation day assignments to identify conflicts.
    This function should be called after the solver has found a solution.
    """
    print("Analyzing compensation day assignments...")
    conflicts = {}
    
    for w in all_comp_day_vars:
        comp_day_assignments = {}
        worker_conflicts = False
        
        for (special_day, comp_day), var in all_comp_day_vars[w].items():
            # Check if this compensation day variable is true (value = 1)
            if solver.Value(var) == 1:
                print(f"Worker {w}: Compensation day {comp_day} compensates for special day {special_day}")
                if comp_day not in comp_day_assignments:
                    comp_day_assignments[comp_day] = []
                comp_day_assignments[comp_day].append(special_day)
                
                # Detect if this compensation day is used more than once
                if len(comp_day_assignments[comp_day]) > 1:
                    if w not in conflicts:
                        conflicts[w] = {}
                    conflicts[w][comp_day] = comp_day_assignments[comp_day]
                    worker_conflicts = True
        
        if worker_conflicts:
            print(f"Worker {w} has compensation day conflicts:")
            for comp_day, special_days in conflicts[w].items():
                print(f"  Compensation day {comp_day} is used for multiple special days: {special_days}")
        else:
            print(f"Worker {w} has no compensation day conflicts")
    
    return conflicts