    sunday_balance_penalties = []
    worker_sundays = {}
    sunday_free_vars = {}
    sunday_free_vars_ratio = {}

    for w in workers + workers_past:
        worker_sundays[w] = [d for d in sundays if d in working_days[w]]
        sunday_free_vars_ratio[w] = []
        sunday_free_vars[w] = []

        total_sundays = len(worker_sundays[w])
        if total_sundays <= 1:
            continue
        
        for sunday in worker_sundays[w]:
            sunday_free = model.NewBoolVar(f"sunday_free_{w}_{sunday}")
            
            # Link to actual L shift assignment
            model.Add(shift.get((w, sunday, "L"), 0) >= 1).OnlyEnforceIf(sunday_free)
            model.Add(shift.get((w, sunday, "L"), 0) == 0).OnlyEnforceIf(sunday_free.Not())
            sunday_free_vars[w].append(sunday_free)
            sunday_free_vars_ratio[w].append((sunday, sunday_free))
        
        sundays_off = model.NewIntVar(0, total_sundays, f"sundays_off_{w}")
        model.Add(sundays_off == sum(sunday_free_vars[w]))

        sunday_blocks = np.array_split(worker_sundays[w], 12)

        for month_index, block in enumerate(sunday_blocks):
            # Extract free-vars for this block
            block_vars = [v for (d, v) in sunday_free_vars_ratio[w] if d in block]

            S_month_free = model.NewIntVar(0, len(block), f"S_month_free_{w}_{month_index}")
            model.Add(S_month_free == sum(block_vars))

            # expected = uniform proportion
            lhs = S_month_free * total_sundays
            rhs = sundays_off * len(block)

            over = model.NewIntVar(0, 1000, f"sunday_over_{w}_{month_index}")
            under = model.NewIntVar(0, 1000, f"sunday_under_{w}_{month_index}")

            model.Add(over >= lhs - rhs)
            model.Add(under >= rhs - lhs)

            sunday_balance_penalties.append(SUNDAY_YEAR_BALANCE_PENALTY * over)
            sunday_balance_penalties.append(SUNDAY_YEAR_BALANCE_PENALTY * under)

            # Store in optimization details
            optimization_details['point_5_1_sunday_balance']['variables'].append({
                                'worker': w,
                                'segment': month,
                                'over_penalty': over,
                                'under_penalty': under,
                                'ideal_count': sundays_off,
                                'segment_sundays': []
                                })
            




#1 maria ana
    parts = np.array_split(days_of_year, 12) 
    sunday_parts=[]
    for part in parts:
        sunday_part=[d for d in part if d in sundays]
        sunday_parts.append(sunday_part)

    for w in workers:
        list_of_free_sundays_per_semester=[]
        for part in sunday_parts:
            free_sundays_semester=sum(shift[(w, d, 'L')] for d in part if (w,d,'L') in shift)
            list_of_free_sundays_per_semester.append(free_sundays_semester)
        
        max_free_sundays = model.NewIntVar(0, len(sundays), f"max_free_semester_sundays_{w}")
        min_free_sundays = model.NewIntVar(0, len(sundays), f"min_free_semester_sundays_{w}") 
        
        model.AddMaxEquality(max_free_sundays, list_of_free_sundays_per_semester)
        model.AddMinEquality(min_free_sundays, list_of_free_sundays_per_semester)

        semester_diff = model.NewIntVar(0, len(sundays), f"semester_diff_{w}")
        model.Add(semester_diff == max_free_sundays - min_free_sundays)
        
        objective_terms.append(SUNDAY_YEAR_BALANCE_PENALTY * semester_diff)
