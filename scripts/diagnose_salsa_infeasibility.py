"""
Rebuild the SALSA model for one process/posto and find which restriction makes it infeasible.

Leave-one-out: a restriction is necessary when the full model is INFEASIBLE and removing
that restriction alone yields FEASIBLE or OPTIMAL.

After a normal batch run, func_inicializa writes a diagnostic bundle under data/output:
  diagnose_salsa_manifest-{process}-{posto}.json
  df_colaborador / df_calendario / df_estimativas / df_annual_variables (existing)
  df_feriados / df_process_rules / df_pro_emp_mov (when non-empty)

Usage:
  .\\.venv\\Scripts\\python.exe scripts/diagnose_salsa_infeasibility.py --process 74961 --posto 27
"""

from __future__ import annotations

import argparse
import json
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

DEFAULT_OUT_DIR = ROOT / "data" / "output"

# Constraint families applied in SALSA (same order as salsaAlgorithm.execute_algorithm).
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


def _bundle_prefix(process: str | int, posto: str | int) -> str:
    return f"{process}-{posto}"


def _require_path(out_dir: Path, name: str) -> Path:
    path = out_dir / name
    if not path.is_file():
        raise SystemExit(
            f"Missing {path.name}. Run the batch for this process/posto first "
            f"(func_inicializa must finish so debug CSVs and manifest are written)."
        )
    return path


def load_manifest(out_dir: Path, process: str, posto: str) -> dict:
    path = _require_path(out_dir, f"diagnose_salsa_manifest-{_bundle_prefix(process, posto)}.json")
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _read_optional_csv(out_dir: Path, filename: str) -> pd.DataFrame:
    path = out_dir / filename
    if not path.is_file():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    for column in ("schedule_day", "begin_date", "end_date", "deadline_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column])
    return frame


def load_adapted(out_dir: Path, process: str, posto: str) -> dict:
    prefix = _bundle_prefix(process, posto)
    manifest = load_manifest(out_dir, process, posto)

    colaborador = pd.read_csv(_require_path(out_dir, f"df_colaborador-{prefix}.csv"))
    calendario = pd.read_csv(_require_path(out_dir, f"df_calendario-{prefix}.csv"))
    estimativas = pd.read_csv(_require_path(out_dir, f"df_estimativas-{prefix}.csv"))
    annual_path = out_dir / f"df_annual_variables-{prefix}.csv"
    if annual_path.is_file():
        annual = pd.read_csv(annual_path)
        annual["begin_date"] = pd.to_datetime(annual["begin_date"])
        annual["end_date"] = pd.to_datetime(annual["end_date"])
    else:
        annual = pd.DataFrame()

    start_date = manifest["start_date"]
    end_date = manifest["end_date"]
    employees_for_posto = manifest.get("employees_id_list_for_posto")
    if not employees_for_posto:
        employees_for_posto = [
            str(employee_id)
            for employee_id in sorted(colaborador["employee_id"].dropna().astype(int).unique().tolist())
        ]

    treatment = {
        "admissao_proporcional": manifest.get("admissao_proporcional", "floor"),
        "wfm_proc_colab": manifest.get("wfm_proc_colab", ""),
        "df_feriados": _read_optional_csv(out_dir, f"df_feriados-{prefix}.csv"),
        "nome_pais": manifest.get("nome_pais", "Espanha"),
        "eci_flag": manifest.get("eci_flag", False),
        "eci_sibling_results_flag": manifest.get("eci_sibling_results_flag", False),
        "NUM_DIAS_CONS": int(manifest.get("NUM_DIAS_CONS", 6)),
        "ld_sunday_param": float(manifest.get("ld_sunday_param", 0.0)),
        "ld_holiday_param": float(manifest.get("ld_holiday_param", 1.0)),
        "start_date": start_date,
        "end_date": end_date,
        "employees_id_list_for_posto": employees_for_posto,
        "employees_id_90_list": manifest.get("employees_id_90_list", []),
        "df_process_rules": _read_optional_csv(out_dir, f"df_process_rules-{prefix}.csv"),
        "df_pro_emp_mov": _read_optional_csv(out_dir, f"df_pro_emp_mov-{prefix}.csv"),
        "df_annual_variables": annual,
        "real_shifts": manifest.get("real_shifts", ["M", "T"]),
    }
    algorithm = SalsaAlgorithm(
        process_id=int(process),
        start_date=start_date,
        end_date=end_date,
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
            adapted["empty_rules"],
            adapted["day_off_rules"],
            adapted["dynamic_empty"],
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SALSA CP-SAT leave-one-out infeasibility diagnosis from func_inicializa debug bundle.",
    )
    parser.add_argument("--process", required=True, help="Process id (same as current_process_id in the batch)")
    parser.add_argument("--posto", required=True, help="Posto id for the infeasible run (see Current posto_id in log)")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Folder with debug CSVs and manifest (default: {DEFAULT_OUT_DIR})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = args.output_dir.resolve()
    process = str(args.process)
    posto = str(args.posto)
    prefix = _bundle_prefix(process, posto)

    logging.disable(logging.WARNING)
    print(f"Loading bundle {prefix} from {out_dir} ...", flush=True)
    if not (out_dir / f"df_feriados-{prefix}.csv").is_file():
        print(
            f"Note: df_feriados-{prefix}.csv not found (empty in run, or re-run batch with updated code).",
            flush=True,
        )
    adapted = load_adapted(out_dir, process, posto)
    names = {}
    colaborador = pd.read_csv(out_dir / f"df_colaborador-{prefix}.csv")
    for row in colaborador.itertuples(index=False):
        names[int(row.employee_id)] = str(row.nome)

    print(
        f"workers={len(adapted['workers'])} days={len(adapted['days_of_year'])} "
        f"special={len(adapted['special_days'])} period={adapted['period']} country={adapted['country']}",
        flush=True,
    )

    lines = [
        f"SALSA infeasibility diagnosis for process {process} posto {posto}",
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
    full_status = solve_status(full_model)
    print(f"full model: {full_status} in {time.perf_counter() - started:.1f}s vars={len(full_shift)}", flush=True)
    lines.append(f"Full model status: {full_status} (shift variables {len(full_shift)})")
    if full_status != "INFEASIBLE":
        lines.append("The rebuilt model is not infeasible, so family tests were not run.")
        _write(out_dir, process, posto, lines)
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
    _write(out_dir, process, posto, lines)
    print("\n".join(lines), flush=True)


def _write(out_dir: Path, process: str, posto: str, lines: list[str]) -> None:
    path = out_dir / f"infeasibility_{process}_{posto}_report.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {path}", flush=True)


if __name__ == "__main__":
    main()
