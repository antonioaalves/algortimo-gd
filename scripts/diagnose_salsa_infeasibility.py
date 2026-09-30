"""
Rebuild the SALSA model for one process and find which restriction makes it infeasible.

The method is leave-one-out, not a guess:
a restriction is necessary when the full model is INFEASIBLE and the same model
with that restriction removed becomes FEASIBLE or OPTIMAL.

Inputs are the four frames saved just before the algorithm, plus the holiday
calendar, compensation rules, and pending days off printed in the run log.
The rebuilt model is accepted only when it matches the logged fingerprint
(15 workers, period 260-375, 378 days, 12 special days, 9038 shift variables).
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import pandas as pd
from ortools.sat.python import cp_model

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.algorithms.model_salsa.optimization_salsa import salsa_optimization
from src.algorithms.model_salsa.salsa_constraints import (
    LQ_attribution,
    dynamic_empty_day,
    first_day_not_free,
    free_days_saturdays,
    free_days_special_days,
    free_days_sundays,
    global_compensation_days,
    maximum_continuous_working_days,
    one_colab_min_constraint,
    salsa_2_consecutive_free_days,
    salsa_2_day_quality_weekend,
    salsa_2_free_days_week,
    salsa_saturday_L_constraint,
    shift_day_constraint,
    week_working_days_constraint,
    working_day_shifts,
)
from src.algorithms.model_salsa.variables import decision_variables
from src.algorithms.salsaAlgorithm import SalsaAlgorithm

OUT_DIR = ROOT / "data" / "output"
PROCESS = "44650"
EXPECTED_WORKERS = 15
EXPECTED_DAYS = 378
EXPECTED_SPECIAL = 12
EXPECTED_PERIOD = [260, 375]
EXPECTED_SHIFT_VARS = 9038

# Same families the 2026-09-29 run applied, in the same order.
FAMILIES = [
    "shift_day_constraint",
    "working_day_shifts",
    "compensation_days",
    "week_working_days_constraint",
    "maximum_continuous_working_days",
    "LQ_attribution",
    "salsa_2_consecutive_free_days",
    "salsa_2_day_quality_weekend",
    "salsa_saturday_L_constraint",
    "salsa_2_free_days_week",
    "first_day_not_free",
    "free_days_special_days",
    "free_days_sundays",
    "free_days_saturdays",
    "one_colab_min_constraint",
    "dynamic_empty_day",
    "objective",
]

# Structural assignment. Dropped only in the domain check, never in family leave-one-out.
STRUCTURAL = {"shift_day_constraint"}


def _feriados() -> pd.DataFrame:
    rows = [
        ("2025-12-25", "F", 4),
        ("2026-01-01", "F", 11),
        ("2026-01-06", "F", 16),
        ("2026-04-02", "A", 102),
        ("2026-04-03", "A", 103),
        ("2026-05-01", "A", 131),
        ("2026-05-02", "A", 132),
        ("2026-05-15", "A", 145),
        ("2026-07-25", "A", 216),
        ("2026-08-15", "A", 237),
        ("2026-10-12", "A", 295),
        ("2026-11-02", "A", 316),
        ("2026-11-09", "A", 323),
        ("2026-12-07", "A", 351),
        ("2026-12-08", "A", 352),
        ("2026-12-25", "F", 369),
        ("2027-01-01", "F", 376),
    ]
    frame = pd.DataFrame(rows, columns=["schedule_day", "tipo_feriado", "index"])
    frame["fk_unidade"] = 2339
    return frame


def _process_rules(employee_ids: list[int]) -> pd.DataFrame:
    """One ld_holiday row per employee and day index 260..375, as printed for this run.

    The log shows amount 1, deadline 28, overlap Y, and no day-off or empty-day rows.
    116 indexes * 10 employees = 1160 rows, matching the logged shape.
    """
    rows = []
    for employee_id in employee_ids:
        for index in range(260, 376):
            rows.append(
                {
                    "employee_id": employee_id,
                    "rule_code": "ld_holiday",
                    "index": index,
                    "time_off_additional": 1,
                    "time_off_deadline": 28,
                    "overlap_sunday_holiday": "Y",
                }
            )
    return pd.DataFrame(rows)


def _past_lds() -> pd.DataFrame:
    rows = [
        (118, "2026-08-15", 28, 1),
        (113, "2026-08-15", 28, 1),
        (116, "2026-08-15", 28, 1),
    ]
    frame = pd.DataFrame(
        rows,
        columns=["employee_id", "schedule_day", "time_off_deadline", "n_lds_pending"],
    )
    frame["rule_code"] = "ld_holiday"
    frame["schedule_day"] = pd.to_datetime(frame["schedule_day"])
    return frame


def load_adapted() -> dict:
    annual = pd.read_csv(OUT_DIR / f"df_annual_variables-{PROCESS}-27.csv")
    annual["begin_date"] = pd.to_datetime(annual["begin_date"])
    annual["end_date"] = pd.to_datetime(annual["end_date"])
    colaborador = pd.read_csv(OUT_DIR / f"df_colaborador-{PROCESS}-27.csv")
    calendario = pd.read_csv(OUT_DIR / f"df_calendario-{PROCESS}-27.csv")
    estimativas = pd.read_csv(OUT_DIR / f"df_estimativas-{PROCESS}-27.csv")
    employee_ids = sorted(colaborador["employee_id"].dropna().astype(int).unique().tolist())

    treatment = {
        "admissao_proporcional": "floor",
        "wfm_proc_colab": "",
        "df_feriados": _feriados(),
        "nome_pais": "Espanha",
        "eci_flag": False,
        "eci_sibling_results_flag": False,
        "NUM_DIAS_CONS": 8,
        "ld_sunday_param": 0.0,
        "ld_holiday_param": 1.0,
        "start_date": "2026-09-07",
        "end_date": "2026-12-31",
        "employees_id_list_for_posto": [str(employee_id) for employee_id in employee_ids],
        "employees_id_90_list": [],
        "df_process_rules": _process_rules(employee_ids),
        "df_pro_emp_mov": _past_lds(),
        "df_annual_variables": annual,
        "real_shifts": ["M", "T"],
    }
    algorithm = SalsaAlgorithm(
        process_id=int(PROCESS),
        start_date="2026-09-07",
        end_date="2026-12-31",
    )
    return algorithm.adapt_data(
        {
            "df_calendario": calendario,
            "df_colaborador": colaborador,
            "df_estimativas": estimativas,
            "df_annual_variables": annual,
        },
        treatment,
    )


def _keep_set(adapted: dict, worker: int) -> set[int]:
    kept = {worker}
    dummy_workers = adapted["dummy_workers"]
    if worker in dummy_workers:
        kept.add(dummy_workers[worker]["parent"])
    for dummy_id, info in dummy_workers.items():
        if info.get("parent") in kept:
            kept.add(dummy_id)
    return kept


def _filter_ids(values, keep: set[int] | None):
    if keep is None:
        return list(values)
    return [worker for worker in values if worker in keep]


def _filter_dict(mapping: dict, keep: set[int] | None) -> dict:
    if keep is None:
        return mapping
    return {key: value for key, value in mapping.items() if key in keep}


def build_model(adapted: dict, skip: set[str], keep: set[int] | None = None):
    workers = _filter_ids(adapted["workers"], keep)
    workers_complete = _filter_ids(adapted["workers_complete"], keep)
    workers_no_contract_changes = _filter_ids(adapted["workers_no_contract_changes"], keep)
    workers_complete_cycle = _filter_ids(adapted["workers_complete_cycle"], keep)
    workers_past = _filter_ids(adapted["workers_past"], keep)
    dummy_workers = _filter_dict(adapted["dummy_workers"], keep)
    workers_with_dummy = _filter_dict(adapted["workers_with_dummy"], keep)

    days_of_year = adapted["days_of_year"]
    real_working_shift = ["M", "T"]
    shifts = real_working_shift + ["L", "LQ", "LD", "F", "A", "V", "-"]
    check_shift = real_working_shift + ["L", "LQ", "LD"]
    working_shift = real_working_shift + ["LD"]

    model = cp_model.CpModel()
    shift = decision_variables(
        model,
        workers_complete,
        shifts,
        adapted["first_registered_day"],
        adapted["last_registered_day"],
        adapted["worker_absences"],
        adapted["vacation_days"],
        adapted["empty_days"],
        adapted["closed_holidays"],
        adapted["fixed_days_off"],
        adapted["fixed_LQs"],
        adapted["shift_data"],
        workers_past,
        adapted["fixed_compensation_days"],
        adapted["locked_days"],
        adapted["forced_work_days"],
        adapted["contract_type"],
        adapted["dynamic_empty"],
        adapted["complete_cycle_days"],
        real_working_shift,
    )

    if "shift_day_constraint" not in skip:
        shift_day_constraint(model, shift, days_of_year, workers_complete, shifts)
    if "working_day_shifts" not in skip:
        working_day_shifts(
            model, shift, workers, adapted["working_days"], check_shift, working_shift,
            adapted["period"], adapted["contract_type"], adapted["complete_cycle_days"],
        )
    if "compensation_days" not in skip and adapted["country"] == "Espanha":
        global_compensation_days(
            model, shift, workers_complete, adapted["working_days"], adapted["holidays"],
            adapted["sundays"], adapted["week_to_days"], real_working_shift,
            adapted["holiday_rules"], adapted["sunday_rules"], adapted["fixed_days_off"],
            adapted["fixed_LQs"], adapted["worker_absences"], adapted["vacation_days"],
            adapted["period"], adapted["override_holiday_sunday"],
            adapted["fixed_compensation_days"], adapted["holiday_past_lds"],
            adapted["sunday_past_lds"], adapted["closed_holidays"], dummy_workers,
            workers_with_dummy,
        )
    if workers:
        if "week_working_days_constraint" not in skip:
            week_working_days_constraint(
                model, shift, adapted["week_to_days_salsa"], workers, working_shift,
                adapted["work_days_per_week"], adapted["period"], adapted["complete_cycle_days"],
            )
        if "maximum_continuous_working_days" not in skip:
            maximum_continuous_working_days(
                model, shift, days_of_year, workers, working_shift, adapted["num_dias_cons"],
                adapted["period"], dummy_workers, workers_with_dummy, adapted["complete_cycle_days"],
            )
        if "LQ_attribution" not in skip:
            LQ_attribution(
                model, shift, workers_no_contract_changes, adapted["working_days"], adapted["c2d"],
                adapted["year_range"], adapted["annual_variables"], workers_with_dummy, adapted["sundays"],
            )
        if "salsa_2_consecutive_free_days" not in skip:
            salsa_2_consecutive_free_days(
                model, shift, workers, adapted["working_days"], adapted["contract_type"],
                adapted["fixed_days_off"], adapted["fixed_LQs"], adapted["period"],
                adapted["complete_cycle_days"],
            )
        if "salsa_2_day_quality_weekend" not in skip:
            salsa_2_day_quality_weekend(
                model, shift, workers, adapted["contract_type"], adapted["working_days"],
                adapted["sundays"], False, days_of_year, adapted["year_range"],
            )
        if "salsa_saturday_L_constraint" not in skip:
            salsa_saturday_L_constraint(
                model, shift, workers, adapted["working_days"], adapted["period"],
            )
        if "salsa_2_free_days_week" not in skip:
            salsa_2_free_days_week(
                model, shift, workers, adapted["week_to_days_salsa"], adapted["working_days"],
                adapted["admissao_proporcional"], adapted["data_admissao"], adapted["data_demissao"],
                adapted["fixed_days_off"], adapted["fixed_LQs"], adapted["contract_type"],
                adapted["work_days_per_week"], adapted["period"], adapted["complete_cycle_days"],
                adapted["dynamic_empty"], dummy_workers, workers_with_dummy,
            )
        if "first_day_not_free" not in skip:
            first_day_not_free(
                model, shift, workers, adapted["working_days"], adapted["first_registered_day"],
                working_shift, adapted["fixed_days_off"], adapted["period"],
            )
        if "free_days_special_days" not in skip:
            free_days_special_days(
                model, shift, adapted["sundays"], workers_no_contract_changes, adapted["working_days"],
                adapted["total_l_dom_or_sab"], adapted["year_range"], adapted["annual_variables"],
                workers_with_dummy,
            )
        if "free_days_sundays" not in skip:
            free_days_sundays(
                model, shift, adapted["sundays"], workers_no_contract_changes, adapted["working_days"],
                adapted["total_l_dom"], adapted["year_range"], adapted["annual_variables"],
                workers_with_dummy,
            )
        if "free_days_saturdays" not in skip:
            free_days_saturdays(
                model, shift, adapted["sundays"], workers_no_contract_changes, adapted["working_days"],
                adapted["total_l_sab"], adapted["year_range"], adapted["annual_variables"],
                workers_with_dummy,
            )
        if "one_colab_min_constraint" not in skip:
            one_colab_min_constraint(
                model, shift, workers_past, workers, real_working_shift, days_of_year,
                adapted["shift_data"], adapted["period"], adapted["closed_holidays"],
                adapted["contract_type"],
            )
        if "dynamic_empty_day" not in skip:
            dynamic_empty_day(
                model, shift, workers, adapted["contract_type"], adapted["week_to_days"],
                adapted["empty_days"], adapted["dynamic_empty"], adapted["fixed_days_off"],
                adapted["fixed_LQs"], adapted["data_admissao"], adapted["data_demissao"],
                adapted["period"], adapted["admissao_proporcional"], adapted["closed_holidays"],
                adapted["complete_cycle_days"], adapted["work_days_per_week"],
            )
    if "objective" not in skip:
        salsa_optimization(
            model, days_of_year, workers_complete, workers_complete_cycle, real_working_shift,
            shift, adapted["pess_obj"], adapted["working_days"], adapted["closed_holidays"],
            adapted["min_workers"], adapted["max_workers"], adapted["week_to_days"],
            adapted["sundays"], adapted["c2d"], adapted["total_l_dom"], adapted["total_l_sab"],
            adapted["total_l_dom_or_sab"], adapted["work_day_hours"], workers_past,
            adapted["year_range"], adapted["managers"], adapted["keyholders"], adapted["h_plus"],
            adapted["eci_sibling_results_flag"],
        )
    return model, shift


def solve_status(model: cp_model.CpModel, time_limit: float = 20.0) -> str:
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = time_limit
    solver.parameters.num_search_workers = 4
    solver.parameters.log_search_progress = False
    solver.parameters.stop_after_first_solution = True
    status = solver.Solve(model)
    return solver.StatusName(status)


def _date(adapted: dict, day: int) -> str:
    raw = adapted["index_to_date"].get(day)
    if raw is None:
        return str(day)
    return str(raw)[:10]


def pattern_clashes(adapted: dict) -> list[str]:
    """Same comparison the pre-solver check logs, translated to dates.

    This is the warning already printed in the run. It is not, by itself,
    proof that the CP model is infeasible.
    """
    lines = []
    period = adapted["period"]
    for worker, pattern in adapted["work_days_per_week"].items():
        for week, days in adapted["week_to_days_salsa"].items():
            if not days or days[-1] < period[0] or days[0] > period[1]:
                continue
            days_off = set(days).intersection(
                set(adapted["fixed_days_off"].get(worker, ())) | set(adapted["fixed_LQs"].get(worker, ()))
            )
            expected = 7 - int(pattern[week - 1])
            if len(days_off) > expected:
                dated = ", ".join(f"{_date(adapted, day)} (idx {day})" for day in sorted(days_off))
                lines.append(
                    f"worker {worker} week {week}: pattern asks for {int(pattern[week - 1])} working days "
                    f"({expected} day off) and {len(days_off)} fixed days off are already set: {dated}"
                )
    return lines


def without_days(adapted: dict, worker: int, days: set[int]) -> dict:
    cloned = dict(adapted)
    for field in ("fixed_days_off", "fixed_LQs", "vacation_days", "empty_days", "locked_days"):
        cloned[field] = dict(adapted[field])
        current = adapted[field].get(worker, set())
        if isinstance(current, set):
            cloned[field][worker] = current - days
        else:
            cloned[field][worker] = type(current)(day for day in current if day not in days)
    return cloned


def main() -> None:
    logging.disable(logging.WARNING)
    print("Loading frames and rebuilding adapted data...", flush=True)
    adapted = load_adapted()
    names = {}
    colaborador = pd.read_csv(OUT_DIR / f"df_colaborador-{PROCESS}-27.csv")
    for row in colaborador.itertuples(index=False):
        names[int(row.employee_id)] = str(row.nome)

    print(
        f"workers={len(adapted['workers'])} days={len(adapted['days_of_year'])} "
        f"special={len(adapted['special_days'])} period={adapted['period']} country={adapted['country']}",
        flush=True,
    )
    mismatches = []
    if len(adapted["workers"]) != EXPECTED_WORKERS:
        mismatches.append(f"workers {len(adapted['workers'])} != {EXPECTED_WORKERS}")
    if len(adapted["days_of_year"]) != EXPECTED_DAYS:
        mismatches.append(f"days {len(adapted['days_of_year'])} != {EXPECTED_DAYS}")
    if len(adapted["special_days"]) != EXPECTED_SPECIAL:
        mismatches.append(f"special {len(adapted['special_days'])} != {EXPECTED_SPECIAL}")
    if list(adapted["period"]) != EXPECTED_PERIOD:
        mismatches.append(f"period {list(adapted['period'])} != {EXPECTED_PERIOD}")
    if mismatches:
        raise SystemExit("Rebuilt data does not match the logged run: " + "; ".join(mismatches))

    lines = [
        f"SALSA infeasibility diagnosis for process {PROCESS}",
        f"workers={adapted['workers']}",
        f"period={adapted['period']} ({_date(adapted, adapted['period'][0])} to {_date(adapted, adapted['period'][1])})",
        "",
        "Pre-solver 5/6 pattern warnings inside the scheduling period:",
    ]
    clashes = pattern_clashes(adapted)
    lines.extend(clashes or ["none"])
    lines.append("")

    print("Solving the full model...", flush=True)
    started = time.perf_counter()
    full_model, full_shift = build_model(adapted, skip=set())
    if len(full_shift) != EXPECTED_SHIFT_VARS:
        raise SystemExit(
            f"Shift variable count {len(full_shift)} != logged {EXPECTED_SHIFT_VARS}. "
            "The rebuilt model is not the one that was solved."
        )
    full_status = solve_status(full_model)
    print(f"full model: {full_status} in {time.perf_counter() - started:.1f}s vars={len(full_shift)}", flush=True)
    lines.append(f"Full model status: {full_status} (shift variables {len(full_shift)})")
    if full_status != "INFEASIBLE":
        lines.append("The rebuilt model is not infeasible, so family tests were not run.")
        _write(lines)
        return

    print("Domain check: variables + exactly one shift, no business restrictions...", flush=True)
    domain_status = solve_status(build_model(adapted, skip=set(FAMILIES) - STRUCTURAL)[0])
    print(f"domains + exactly one: {domain_status}", flush=True)
    lines.append(f"Variables + exactly one shift, no other restriction: {domain_status}")
    lines.append("")
    lines.append("Leave-one-out. FEASIBLE/OPTIMAL means that restriction is necessary.")

    necessary = []
    for family in FAMILIES:
        if family in STRUCTURAL:
            continue
        started = time.perf_counter()
        status = solve_status(build_model(adapted, skip={family})[0])
        print(f"without {family}: {status} ({time.perf_counter() - started:.1f}s)", flush=True)
        lines.append(f"without {family}: {status}")
        if status in {"FEASIBLE", "OPTIMAL"}:
            necessary.append(family)

    lines.append("")
    lines.append(
        "Necessary restrictions: " + (", ".join(necessary) if necessary else "none by itself")
    )

    print("Single-employee models (business restrictions, no objective)...", flush=True)
    lines.append("")
    lines.append("Single-employee models, objective left out:")
    parents = [worker for worker in adapted["workers"] if worker not in adapted["dummy_workers"]]
    solo_infeasible = []
    for worker in parents:
        kept = _keep_set(adapted, worker)
        label = names.get(worker, str(worker))
        started = time.perf_counter()
        status = solve_status(build_model(adapted, skip={"objective"}, keep=kept)[0])
        extra = ""
        if len(kept) > 1:
            extra = f" with contract slices {sorted(kept - {worker})}"
        print(f"solo {worker} {label}: {status} ({time.perf_counter() - started:.1f}s)", flush=True)
        lines.append(f"employee {worker} ({label}){extra}: {status}")
        if status == "INFEASIBLE":
            solo_infeasible.append(worker)

    period_start, period_end = adapted["period"]
    for worker in solo_infeasible:
        kept = _keep_set(adapted, worker)
        label = names.get(worker, str(worker))
        lines.append("")
        lines.append(f"Leave-one-out for employee {worker} ({label}) alone:")
        print(f"Family leave-one-out for {worker}...", flush=True)
        worker_necessary = []
        for family in FAMILIES:
            if family in STRUCTURAL or family == "objective":
                continue
            status = solve_status(build_model(adapted, skip={"objective", family}, keep=kept)[0])
            print(f"  {worker} without {family}: {status}", flush=True)
            lines.append(f"  without {family}: {status}")
            if status in {"FEASIBLE", "OPTIMAL"}:
                worker_necessary.append(family)
        if not worker_necessary:
            print(f"  no single family frees {worker}; testing pairs...", flush=True)
            lines.append("  no single family; pairs that restore feasibility:")
            business = [family for family in FAMILIES if family not in STRUCTURAL and family != "objective"]
            for index, first in enumerate(business):
                for second in business[index + 1:]:
                    status = solve_status(
                        build_model(adapted, skip={"objective", first, second}, keep=kept)[0]
                    )
                    if status in {"FEASIBLE", "OPTIMAL"}:
                        lines.append(f"  without {first} AND {second}: {status}")
                        print(f"  {worker} without {first} + {second}: {status}", flush=True)
        else:
            lines.append("  necessary for this employee: " + ", ".join(worker_necessary))

        lines.append(f"Days whose removal makes employee {worker} feasible:")
        print(f"Day probe for {worker}...", flush=True)
        found_day = False
        for week, days in adapted["week_to_days_salsa"].items():
            if not days or max(days) < period_start or min(days) > period_end:
                continue
            blocked = set()
            for person in kept:
                for field in ("fixed_days_off", "fixed_LQs", "vacation_days", "empty_days", "locked_days"):
                    blocked.update(set(adapted[field].get(person, ())) & set(days))
            blocked = {day for day in blocked if period_start <= day <= period_end}
            if not blocked:
                continue
            probe = adapted
            for person in kept:
                probe = without_days(probe, person, blocked)
            status = solve_status(build_model(probe, skip={"objective"}, keep=kept)[0])
            if status not in {"FEASIBLE", "OPTIMAL"}:
                continue
            dated_week = f"{_date(adapted, min(days))}..{_date(adapted, max(days))}"
            lines.append(f"  week {week} ({dated_week}) is involved")
            print(f"  week {week} ({dated_week}) unblocks {worker}", flush=True)
            for day in sorted(blocked):
                one = adapted
                for person in kept:
                    one = without_days(one, person, {day})
                one_status = solve_status(build_model(one, skip={"objective"}, keep=kept)[0])
                if one_status in {"FEASIBLE", "OPTIMAL"}:
                    found_day = True
                    lines.append(f"    removing {_date(adapted, day)} (idx {day}) -> {one_status}")
                    print(f"    day {_date(adapted, day)} unblocks {worker}", flush=True)
        if not found_day:
            lines.append("  no single fixed, vacation, empty, or locked day inside the period")

    lines.append("")
    lines.append(
        "A restriction is reported only when removing it changes the status away from INFEASIBLE. "
        "UNKNOWN means the time limit was hit before a proof."
    )
    _write(lines)
    print("\n".join(lines), flush=True)


def _write(lines: list[str]) -> None:
    path = OUT_DIR / f"infeasibility_{PROCESS}_report.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {path}", flush=True)


if __name__ == "__main__":
    main()
