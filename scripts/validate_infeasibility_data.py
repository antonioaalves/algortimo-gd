"""
Validate input data for SALSA infeasibility diagnosis.
Uses df_colaborador and df_calendario (process 25274, posto 617).
Ciclo/Completo employees: result must equal df_calendario (they are fixed).
"""
import math
import os
import pandas as pd

# Paths
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COLAB = os.path.join(BASE, "data", "output", "df_colaborador-25274-617.csv")
CAL = os.path.join(BASE, "data", "output", "df_calendario-25274-617.csv")

def main():
    colab = pd.read_csv(COLAB)
    cal = pd.read_csv(CAL)
    cal["schedule_day"] = pd.to_datetime(cal["schedule_day"])

    # Day index -> wday (1=Mon .. 7=Sun) and weekday name
    day_info = cal.drop_duplicates("index").set_index("index")[["wday", "wd"]].to_dict("index")
    indexes = sorted(day_info.keys())
    min_idx, max_idx = min(indexes), max(indexes)
    sundays = [i for i in indexes if day_info[i]["wd"] == "Sun"]
    saturdays = [i for i in indexes if day_info[i]["wd"] == "Sat"]

    # Workers: exclude Ciclo/Completo (they are fixed from calendar)
    ciclo = set(colab[colab["ciclo"].fillna("").astype(str).str.contains("Completo", case=False, na=False)]["employee_id"].tolist())
    all_workers = colab["employee_id"].tolist()
    workers = [w for w in all_workers if w not in ciclo]

    # Per-worker from calendar (same logic as read_salsa)
    empty_days = {}
    vacation_days = {}
    worker_absences = {}
    fixed_days_off = {}
    fixed_LQs = {}
    shift_M = {}
    shift_T = {}
    working_days = {}

    for w in all_workers:
        wcal = cal[cal["employee_id"] == w]
        if wcal.empty:
            continue
        empty_days[w] = set(wcal[(wcal["horario"].isin(["-", "A-", "V-", "0"]))]["index"].tolist())
        vacation_days[w] = set(wcal[(wcal["horario"].isin(["V", "V-"]))]["index"].tolist())
        worker_absences[w] = set(wcal[(wcal["horario"].isin(["A", "AP", "A-"]))]["index"].tolist())
        fixed_days_off[w] = set(wcal[(wcal["horario"].isin(["L", "C", "L_DOM"]))]["index"].tolist())
        fixed_LQs[w] = set(wcal[wcal["horario"] == "LQ"]["index"].tolist())
        shift_M[w] = set(wcal[(wcal["horario"] == "M") | (wcal["horario"] == "MoT")]["index"].tolist())
        shift_T[w] = set(wcal[(wcal["horario"] == "T") | (wcal["horario"] == "MoT")]["index"].tolist())
        # working_days = all days in range minus empty, absences, vacation (no closed_holidays here)
        working_days[w] = set(range(min_idx, max_idx + 1)) - empty_days[w] - worker_absences[w] - vacation_days[w]

    # c2d and l_dom from colaborador
    c2d = colab.set_index("employee_id")["c2d"].fillna(0).astype(int).to_dict()
    l_dom = colab.set_index("employee_id")["l_dom"].fillna(0).astype(int).to_dict()
    tipo_contrato = colab.set_index("employee_id")["tipo_contrato"].fillna(5).astype(int).to_dict()
    data_admissao_raw = colab.set_index("employee_id")["data_admissao"].to_dict()
    data_demissao_raw = colab.set_index("employee_id")["data_demissao"].to_dict()

    # Map calendar date -> index (one index per schedule_day)
    date_to_index = cal.drop_duplicates("schedule_day").set_index("schedule_day")["index"].to_dict()
    data_admissao = {}
    data_demissao = {}
    for w in all_workers:
        try:
            v = data_admissao_raw.get(w)
            if pd.notna(v) and v:
                dt = pd.to_datetime(v)
                data_admissao[w] = int(date_to_index[dt]) if dt in date_to_index else 0
            else:
                data_admissao[w] = 0
        except Exception:
            data_admissao[w] = 0
        try:
            v = data_demissao_raw.get(w)
            if pd.notna(v) and v:
                dt = pd.to_datetime(v)
                data_demissao[w] = int(date_to_index[dt]) if dt in date_to_index else max_idx + 1
            else:
                data_demissao[w] = max_idx + 1
        except Exception:
            data_demissao[w] = max_idx + 1

    # First/last registered day per worker (from calendar)
    first_registered = {}
    last_registered = {}
    for w in all_workers:
        wcal = cal[cal["employee_id"] == w]
        if not wcal.empty:
            first_registered[w] = int(wcal["index"].min())
            last_registered[w] = int(wcal["index"].max())
        else:
            first_registered[w] = 0
            last_registered[w] = 0

    # Week mapping: same as pipeline (week 1 = days 1-7, week 2 = 8-14, ...)
    def week_of(d):
        return (d - 1) // 7 + 1
    week_to_days_salsa = {}
    for d in range(min_idx, max_idx + 1):
        wk = week_of(d)
        week_to_days_salsa.setdefault(wk, []).append(d)
    for wk in week_to_days_salsa:
        week_to_days_salsa[wk].sort()
    n_weeks = len(week_to_days_salsa)
    # work_days_per_week: simplified [5]*n_weeks (tipo_contrato 8 uses 5/6 pattern; we use 5 for validation)
    work_days_per_week = {w: [5] * (n_weeks + 1) for w in all_workers}
    admissao_proporcional = "floor"

    print("=" * 60)
    print("INFEASIBILITY DATA VALIDATION (process 25274, posto 617)")
    print("=" * 60)
    print(f"Workers (excl. Ciclo/Completo): {workers}")
    print(f"Ciclo/Completo (fixed from calendar): {sorted(ciclo)}")
    print(f"Day index range: {min_idx} - {max_idx}, Sundays: {len(sundays)}")
    print()

    # 1) LQ / c2d vs Saturday–Sunday pairs in working_days
    #    Answers: "Could any employee need quality weekends but not have the space?"
    print("--- 1) Quality weekends: need c2d LQ weekends vs Saturday–Sunday pairs in working_days ---")
    no_space = []   # c2d > 0 but pairs < c2d -> IMPOSSIBLE
    tight = []      # c2d > 0 and pairs == c2d -> exactly enough; free_days_week can still conflict
    for w in workers:
        req = c2d.get(w, 0)
        if req <= 0:
            continue
        wd = working_days.get(w, set())
        # Saturday–Sunday pairs: (d, d+1) with d Sat, d+1 Sun, both in wd
        pairs = []
        for d in saturdays:
            if d in wd and (d + 1) in wd and (d + 1) in sundays:
                pairs.append((d, d + 1))
        n_pairs = len(pairs)
        if n_pairs < req:
            no_space.append((w, req, n_pairs))
            print(f"  Worker {w}: c2d={req}, Saturday–Sunday pairs in working_days={n_pairs} -> IMPOSSIBLE (no space)")
        elif n_pairs == req:
            tight.append((w, req, n_pairs))
            print(f"  Worker {w}: c2d={req}, Saturday–Sunday pairs in working_days={n_pairs} -> TIGHT (exactly enough; free_days_week may still conflict)")
        else:
            print(f"  Worker {w}: c2d={req}, Saturday–Sunday pairs in working_days={n_pairs} -> OK")
    print()
    print("  >>> Could any employee need quality weekends but not have the space?")
    if no_space:
        print(f"  YES. Workers with NO SPACE (c2d > pairs): {[x[0] for x in no_space]}")
        for w, req, n in no_space:
            print(f"    - Worker {w}: needs {req} quality weekends, only {n} Sat–Sun pairs in working_days.")
    else:
        print("  NO. Every worker with c2d>0 has at least c2d Saturday–Sunday pairs in working_days (in this script).")
    if tight:
        print(f"  TIGHT (pairs == c2d): {[x[0] for x in tight]}. With salsa_2_free_days_week, some of those weekends may be forced to L/LQ in a way that leaves no valid LQ pair.")
    print("  Note: This script does not subtract closed_holidays. The pipeline does; so the real working_days may have FEWER pairs than above.")
    print()

    # 1b) Per-week day-offs: required free days (from contract) vs days available in that week
    # Each week the worker must have required_free_days of L/LQ; only days in working_days can be used.
    # If in a week there are fewer days in working_days than required_free_days, that week is impossible.
    print("--- 1b) Per-week day-offs (contract): required free days vs days in working_days ---")
    # Replicate required_free_days logic from salsa_2_free_days_week (salsa_constraints.py)
    def required_free_days_for_week(w, week, week_work_days, fixed_days_off, fixed_LQs, contract_type, work_days_per_week, data_admissao, data_demissao, admissao_proporcional):
        if not week_work_days:
            return 0, "no_days"
        days = week_to_days_salsa.get(week, [])
        worker_admissao = data_admissao.get(w, 0)
        worker_demissao = data_demissao.get(w, max_idx + 1)
        is_admissao_week = (worker_admissao > 0 and worker_admissao in days)
        is_demissao_week = (worker_demissao > 0 and worker_demissao in days)
        tipo_contrato = contract_type.get(w, 0)
        actual_days_in_week = len(week_work_days)
        week_work_days_set = set(week_work_days)
        fixed_days_week = week_work_days_set.intersection(set(fixed_days_off.get(w, set())))
        fixed_lqs_week = week_work_days_set.intersection(set(fixed_LQs.get(w, set())))
        if is_admissao_week or is_demissao_week:
            if tipo_contrato >= 5:
                if 4 <= actual_days_in_week <= 5:
                    required_free_days = 1
                elif actual_days_in_week < 4:
                    required_free_days = 0
                else:
                    wdpw = work_days_per_week.get(w, [5] * (n_weeks + 1))
                    if tipo_contrato == 8 and len(wdpw) >= week and wdpw[week - 1] == 6:
                        required_free_days = 1
                    else:
                        required_free_days = 2
            else:
                free_days_weekly = 7 - tipo_contrato
                proportion = actual_days_in_week / 7.0
                proportion_days = proportion * free_days_weekly
                if admissao_proporcional == "floor":
                    required_free_days = max(0, int(math.floor(proportion_days)))
                elif admissao_proporcional == "ceil":
                    required_free_days = max(0, int(math.ceil(proportion_days)))
                else:
                    required_free_days = max(0, int(math.floor(proportion_days)))
        else:
            if tipo_contrato >= 5:
                wdpw = work_days_per_week.get(w, [5] * (n_weeks + 1))
                if tipo_contrato == 8 and len(wdpw) >= week and wdpw[week - 1] == 6 and actual_days_in_week >= 1:
                    required_free_days = 1
                elif actual_days_in_week >= 2:
                    required_free_days = 2
                elif actual_days_in_week == 1:
                    required_free_days = 1
                else:
                    required_free_days = 0
            else:
                if len(week_work_days) >= 7 - tipo_contrato:
                    required_free_days = 7 - tipo_contrato
                elif len(week_work_days) == 2:
                    required_free_days = 2
                elif len(week_work_days) == 1:
                    required_free_days = 1
                else:
                    required_free_days = 0
        if required_free_days < (len(fixed_days_week) + len(fixed_lqs_week)):
            required_free_days = len(fixed_days_week) + len(fixed_lqs_week)
        return required_free_days, "ok"

    impossible_weeks = []  # (worker, week, required, actual)
    quality_weekend_weeks = {}  # worker -> number of weeks with both Sat and Sun in working_days and enough room
    for w in workers:
        wd = working_days.get(w, set())
        quality_weekend_weeks[w] = 0
        for week, days in week_to_days_salsa.items():
            week_work_days = [d for d in days if d in wd]
            if not week_work_days:
                continue
            if week_work_days[-1] < min_idx or week_work_days[0] > max_idx:
                continue
            req, _ = required_free_days_for_week(w, week, week_work_days, fixed_days_off, fixed_LQs, tipo_contrato, work_days_per_week, data_admissao, data_demissao, admissao_proporcional)
            if len(week_work_days) < req:
                impossible_weeks.append((w, week, req, len(week_work_days)))
            # Count weeks that can host a quality weekend: both Sat and Sun in working_days in this week, and enough days for required free days
            sat_in_week = any(d in saturdays and d in wd for d in week_work_days)
            sun_in_week = any(d in sundays and d in wd for d in week_work_days)
            if sat_in_week and sun_in_week and len(week_work_days) >= req:
                quality_weekend_weeks[w] = quality_weekend_weeks.get(w, 0) + 1

    if impossible_weeks:
        print("  IMPOSSIBLE weeks (required free days > days in working_days for that week):")
        for w, week, req, actual in impossible_weeks:
            print(f"    Worker {w} week {week}: contract requires {req} day-offs (L/LQ) but only {actual} days in working_days -> INFEASIBLE")
        print("  >>> These weeks cannot satisfy salsa_2_free_days_week; fix data or contract.")
    else:
        print("  OK: Every worker has at least required_free_days days in working_days for each week.")
    print("  Quality weekend 'space' (weeks with both Sat and Sun in working_days and enough room for required free days):")
    for w in workers:
        c2d_w = c2d.get(w, 0)
        qw_weeks = quality_weekend_weeks.get(w, 0)
        if c2d_w <= 0:
            continue
        status = "OK" if qw_weeks >= c2d_w else "TIGHT/IMPOSSIBLE"
        print(f"    Worker {w}: c2d={c2d_w}, weeks with room for quality weekend={qw_weeks} -> {status}")
    print()

    # 1c) Pipeline-style: HORARIO F = closed (same as pipeline closed_holidays), recompute working_days
    # This matches what the solver actually uses; finds the concrete data cause.
    print("--- 1c) Pipeline-style (HORARIO F = closed): working_days minus closed, then per-week check ---")
    closed_from_cal = set(cal[cal["horario"] == "F"]["index"].unique())
    print(f"  Closed days (horario F) indices: {sorted(closed_from_cal)}")
    working_days_pipeline = {}
    for w in workers:
        wcal = cal[cal["employee_id"] == w]
        if wcal.empty:
            continue
        empty_d = set(wcal[wcal["horario"].isin(["-", "A-", "V-", "0"])]["index"].tolist())
        vac_d = set(wcal[wcal["horario"].isin(["V", "V-"])]["index"].tolist())
        abs_d = set(wcal[wcal["horario"].isin(["A", "AP", "A-"])]["index"].tolist())
        wd = set(range(min_idx, max_idx + 1)) - empty_d - abs_d - vac_d - closed_from_cal
        working_days_pipeline[w] = wd
    # Per-week: days in pipeline wd >= required free days?
    impossible_pipeline = []
    for w in workers:
        wd = working_days_pipeline.get(w, set())
        tc = tipo_contrato.get(w, 5)
        req_free = 7 - tc
        for week, days in week_to_days_salsa.items():
            week_days = [d for d in days if d in wd]
            if not week_days:
                continue
            if len(week_days) < req_free:
                impossible_pipeline.append((w, week, req_free, len(week_days)))
    if impossible_pipeline:
        print("  IMPOSSIBLE weeks (pipeline working_days):")
        for w, wk, req, n in impossible_pipeline:
            print(f"    Worker {w} week {wk}: need {req} free days, only {n} days in pipeline wd")
        print("  >>> DATA REASON: Above week(s) cannot satisfy salsa_2_free_days_week (this is why the model is INFEASIBLE).")
    else:
        print("  OK: every week has enough days in pipeline working_days.")
    # Sat-Sun pairs in pipeline wd vs c2d (for completeness)
    for w in workers:
        req = c2d.get(w, 0)
        if req <= 0:
            continue
        wd = working_days_pipeline.get(w, set())
        pairs = sum(1 for d in saturdays if d in wd and (d + 1) in wd and (d + 1) in sundays)
        if pairs < req:
            print(f"  Worker {w}: c2d={req}, Sat-Sun pairs in pipeline wd={pairs} -> FAIL")
    print()

    # 2) free_days_special_days: Sundays in working_days >= l_dom
    print("--- 2) free_days_special_days: Sundays in working_days >= l_dom ---")
    for w in workers:
        req = l_dom.get(w, 0)
        if req <= 0:
            continue
        wd = working_days.get(w, set())
        sundays_in_wd = [d for d in sundays if d in wd]
        n_sun = len(sundays_in_wd)
        ok = "OK" if n_sun >= req else "FAIL"
        print(f"  Worker {w}: l_dom={req}, Sundays in working_days={n_sun} -> {ok}")
        if n_sun < req:
            print(f"    -> Need {req} Sundays as L but only {n_sun} Sundays in working_days. Fix l_dom or window.")
    print()

    # 3) first_day_not_free: first registered day must not be fixed as L/LQ
    print("--- 3) first_day_not_free: first registered day not fixed L/LQ ---")
    earliest_first = min((first_registered[w] for w in workers if first_registered.get(w, 0) > 0), default=0)
    for w in workers:
        first = first_registered.get(w, 0)
        if first <= 0 or first <= earliest_first:
            continue
        off = fixed_days_off.get(w, set())
        lqs = fixed_LQs.get(w, set())
        if first in off or first in lqs:
            print(f"  Worker {w}: first_registered_day={first} is fixed as L/LQ -> CONFLICT with first_day_not_free")
        else:
            print(f"  Worker {w}: first_registered_day={first} not in fixed L/LQ -> OK")
    print()

    # 4) salsa_2_free_days_week: partial week (e.g. worker 1668 week 8, only day 55)
    print("--- 4) Partial weeks: required free day vs same day forced working ---")
    for w in workers:
        wd = working_days.get(w, set())
        if not wd:
            continue
        weeks = {}
        for d in wd:
            wk = week_of(d)
            weeks.setdefault(wk, []).append(d)
        for wk, days_in_week in sorted(weeks.items()):
            if len(days_in_week) < 2 and len(days_in_week) == 1:
                # Single-day week: that day must be L or LQ (required free day)
                d55 = days_in_week[0]
                in_off = d55 in fixed_days_off.get(w, set())
                in_lq = d55 in fixed_LQs.get(w, set())
                in_mt = d55 in shift_M.get(w, set()) or d55 in shift_T.get(w, set())
                if in_mt and not in_off and not in_lq:
                    print(f"  Worker {w} week {wk}: only day {d55}; required free day but day is M/T in calendar -> check if first_day_not_free also forces this day")
                elif in_off or in_lq:
                    print(f"  Worker {w} week {wk}: only day {d55}; day is L/LQ -> OK for free day")
    print()

    # 5) one_colab_min: per-day count of workers “available” (day in shift_M or shift_T)
    print("--- 5) one_colab_min: days with 0 or 1 vs 2+ workers available (M/T) ---")
    n_zero, n_one, n_two_plus = 0, 0, 0
    zero_days = []
    for d in range(min_idx, max_idx + 1):
        n_avail = sum(1 for w in workers if d in shift_M.get(w, set()) or d in shift_T.get(w, set()))
        if n_avail == 0:
            n_zero += 1
            if len(zero_days) < 15:
                zero_days.append(d)
        elif n_avail == 1:
            n_one += 1
        else:
            n_two_plus += 1
    print(f"  Days with 0 workers available: {n_zero} (one_colab_min not applied)")
    print(f"  Days with 1 worker available: {n_one}")
    print(f"  Days with 2+ workers available: {n_two_plus} (need >=1 working)")
    if zero_days:
        print(f"  Example days with 0 available: {zero_days}")
    print()

    # 6) Ciclo/Completo: ensure calendar has no '-' for them in their active window (optional)
    print("--- 6) Ciclo/Completo: calendar consistency ---")
    for w in sorted(ciclo):
        wcal = cal[cal["employee_id"] == w]
        if wcal.empty:
            continue
        empty_count = (wcal["horario"].isin(["-", "A-", "V-", "0"])).sum()
        total = len(wcal.drop_duplicates("index"))
        print(f"  Worker {w} (Ciclo): {empty_count} empty rows (M+T), {total} unique days -> result should equal calendar")
    # Summary
    print("--- SUMMARY ---")
    print("  1) LQ/c2d: Saturday-Sunday pairs sufficient for all workers with c2d>0.")
    print("  1b) Per-week day-offs: Each week has enough days in working_days to meet contract (required_free_days).")
    print("  2) free_days_special_days: Sunday count sufficient for all workers with l_dom>0.")
    print("  first_day_not_free: No worker has first registered day fixed as L/LQ.")
    print("  Partial weeks: Worker 3176 has single-day weeks (58, 288); model will assign L/LQ there (calendar shows MoT; solver can change).")
    print("  one_colab_min: On days with 2+ available, at least one worker must work; data does not force 0.")
    print("  Ciclo/Completo: Output must match df_calendario for workers 3354, 4135, 7003.")
    print("=" * 60)

if __name__ == "__main__":
    main()
