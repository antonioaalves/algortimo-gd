"""
read_salsa worker calendar processing — data-model mirror.

Copies read_salsa.py steps 6 + workers_complete loop (extract_worker_horario_sets,
finalize_worker_calendar_sets) so func_inicializa pre-checks use the same day sets
as the solver without modifying src/algorithms/.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from src.algorithms.model_salsa.auxiliar_functions_salsa import (
    absences_to_empty,
    check_5_6_pattern_consistency,
    days_off_atributtion,
    first_not_A_value,
    joining_template_with_contract_per_week,
    populate_week_fixed_days_off,
    populate_week_template,
)


@dataclass
class FinalizedWorkerCalendarSets:
    empty_days: Set[int]
    vacation_days: Set[int]
    worker_absences: Set[int]
    fixed_days_off: Set[int]
    fixed_LQs: Set[int]
    fixed_compensation_days: Set[int]
    shift_M: Set[int]
    shift_T: Set[int]
    forced_work_days: Set[int]
    locked_days: Set[int]
    complete_cycle_days: Set[int]
    free_day_complete_cycle: List[int]
    work_days_per_week: np.ndarray
    working_days: Set[int]
    dynamic_empty: Set[int]


def build_days_of_year_and_week_maps(
    df_calendario: pd.DataFrame,
    closed_holidays: Set[int],
) -> Tuple[List[int], int, Dict[int, List[int]], Dict[int, List[int]], int]:
    """Mirror read_salsa build_days_of_year_and_week_maps / week_to_days_salsa."""
    days_of_year = sorted(df_calendario['index'].drop_duplicates().astype(int).tolist())
    max_day = max(days_of_year) if days_of_year else 0
    non_holidays = set(days_of_year) - set(closed_holidays)

    week_to_days_salsa: Dict[int, List[int]] = {}
    week_to_days: Dict[int, List[int]] = {}
    week_number = 1

    df = (
        df_calendario[['index']]
        .drop_duplicates()
        .sort_values('index')
    )
    for _, row in df.iterrows():
        idx = int(row['index'])
        if week_number not in week_to_days_salsa:
            week_to_days_salsa[week_number] = []
        if idx not in week_to_days_salsa[week_number]:
            week_to_days_salsa[week_number].append(idx)
        if idx in non_holidays:
            if week_number not in week_to_days:
                week_to_days[week_number] = []
            if idx not in week_to_days[week_number]:
                week_to_days[week_number].append(idx)
        if idx % 7 == 0:
            week_number += 1

    for week in week_to_days_salsa:
        week_to_days_salsa[week].sort()
    for week in week_to_days:
        week_to_days[week].sort()

    nbr_weeks = max(week_to_days_salsa) if week_to_days_salsa else 0
    return days_of_year, max_day, week_to_days, week_to_days_salsa, nbr_weeks


def build_salsa_day_week_date_maps(
    df_calendario: pd.DataFrame,
) -> Tuple[Dict[int, frozenset], Dict[pd.Timestamp, int]]:
    """schedule_day keyed salsa week maps (for weekly L/LQ quota checks)."""
    week_to_days: Dict[int, list] = {}
    day_to_week: Dict[pd.Timestamp, int] = {}
    week_number = 1

    df = (
        df_calendario[['schedule_day', 'index']]
        .drop_duplicates('schedule_day')
        .sort_values('index')
    )
    for _, row in df.iterrows():
        day = pd.Timestamp(row['schedule_day']).normalize()
        idx = int(row['index'])
        if week_number not in week_to_days:
            week_to_days[week_number] = []
        if day not in week_to_days[week_number]:
            week_to_days[week_number].append(day)
        day_to_week[day] = week_number
        if idx % 7 == 0:
            week_number += 1

    return (
        {w: frozenset(days) for w, days in week_to_days.items()},
        day_to_week,
    )


def build_week_template_for_worker(
    week_template_temp: Dict[int, str],
    week_to_days_salsa: Dict[int, List[int]],
) -> Dict[int, str]:
    """Mirror read_salsa build_week_template_for_worker."""
    week_template: Dict[int, str] = {}
    for week, days in week_to_days_salsa.items():
        anchor_idx = days[1] if len(days) >= 2 else days[0]
        week_template[week] = week_template_temp.get(anchor_idx, 'A')
    return week_template


def extract_worker_horario_sets(worker_calendar: pd.DataFrame) -> Dict[str, Any]:
    """Mirror read_salsa extract_worker_horario_sets (workers_complete raw extraction)."""
    horario = worker_calendar['horario'].astype(str).str.strip()
    idx = worker_calendar['index'].astype(int)

    return {
        'empty_days': worker_calendar.loc[
            horario.isin(['-', 'A-', 'V-', '0']), 'index'
        ].astype(int).tolist(),
        'vacation_days': worker_calendar.loc[
            horario.isin(['V', 'V-']), 'index'
        ].astype(int).tolist(),
        'worker_absences': worker_calendar.loc[
            horario.isin(['A', 'AP', 'A-']), 'index'
        ].astype(int).tolist(),
        'fixed_days_off': worker_calendar.loc[
            horario.isin(['L', 'C', 'L_DOM']), 'index'
        ].astype(int).tolist(),
        'free_day_complete_cycle': worker_calendar.loc[
            horario.isin(['L', 'L_DOM']), 'index'
        ].astype(int).tolist(),
        'fixed_LQs': set(worker_calendar.loc[horario == 'LQ', 'index'].astype(int)),
        'fixed_compensation_days': set(
            worker_calendar.loc[horario == 'LD', 'index'].astype(int)
        ),
        'shift_M': worker_calendar.loc[
            horario.isin(['M', 'MoT', 'NL', 'NLM']), 'index'
        ].astype(int).tolist(),
        'shift_T': worker_calendar.loc[
            horario.isin(['T', 'MoT', 'NLT']), 'index'
        ].astype(int).tolist(),
        'forced_work_days': worker_calendar.loc[
            horario.isin(['NL', 'NLT', 'NLM']), 'index'
        ].astype(int).tolist(),
        'locked_days': set(
            worker_calendar.loc[worker_calendar['fixed'] == True, 'index'].astype(int)  # noqa: E712
        ) if 'fixed' in worker_calendar.columns else set(),
        'complete_cycle_days': set(
            worker_calendar.loc[worker_calendar['tipo_ciclo'] == True, 'index'].astype(int)  # noqa: E712
        ) if 'tipo_ciclo' in worker_calendar.columns else set(),
        'week_template_temp': (
            worker_calendar.drop_duplicates(subset='index')
            .set_index('index')['workload_template']
            .fillna('A')
            .astype(str)
            .to_dict()
            if 'workload_template' in worker_calendar.columns
            else {}
        ),
    }


def finalize_worker_calendar_sets(
    worker_id: str,
    raw: Dict[str, Any],
    *,
    first_registered_day: int,
    last_registered_day: int,
    days_of_year: List[int],
    max_day: int,
    closed_holidays: Set[int],
    week_to_days_salsa: Dict[int, List[int]],
    nbr_weeks: int,
    week_template: Dict[int, str],
    contract_type: int,
    min_work_days: int,
    max_work_days: int,
    year_range: List[int],
    period: Tuple[int, int],
) -> FinalizedWorkerCalendarSets:
    """Mirror read_salsa finalize_worker_calendar_sets (workers_complete post-process)."""
    empty_days: List[int] = list(raw['empty_days'])
    vacation_days: Set[int] = set(raw['vacation_days'])
    worker_absences: Set[int] = set(raw['worker_absences'])
    fixed_days_off: Set[int] = set(raw['fixed_days_off'])
    fixed_LQs: Set[int] = set(raw['fixed_LQs'])
    fixed_compensation_days: Set[int] = set(raw['fixed_compensation_days'])
    shift_M: Set[int] = set(raw['shift_M'])
    shift_T: Set[int] = set(raw['shift_T'])
    forced_work_days: Set[int] = set(raw['forced_work_days'])
    locked_days: Set[int] = set(raw['locked_days'])
    complete_cycle_days: Set[int] = set(raw['complete_cycle_days'])
    free_day_complete_cycle: List[int] = list(raw['free_day_complete_cycle'])

    if first_registered_day > 0 or last_registered_day > 0:
        empty_days.extend(
            d for d in range(1, first_registered_day) if d not in empty_days
        )
        empty_days.extend(
            d for d in range(last_registered_day + 1, max_day + 1) if d not in empty_days
        )

    empty_days_set = set(empty_days) - closed_holidays - shift_M - shift_T
    vacation_days = vacation_days - closed_holidays
    worker_absences = worker_absences - closed_holidays
    fixed_days_off = fixed_days_off - closed_holidays
    fixed_LQs = fixed_LQs - closed_holidays
    free_day_complete_cycle = sorted(set(free_day_complete_cycle) - closed_holidays)

    if contract_type == 8:
        first_week = first_not_A_value(week_template)
        if first_week > -1:
            work_days_per_week = populate_week_template(
                int(week_template[first_week]), first_week - 1, nbr_weeks
            )
        else:
            work_days_per_week = populate_week_fixed_days_off(
                fixed_days_off, fixed_LQs, week_to_days_salsa, period, nbr_weeks
            )
        check_5_6_pattern_consistency(
            worker_id, fixed_days_off, fixed_LQs, week_to_days_salsa, work_days_per_week
        )
    else:
        work_days_per_week = np.full(nbr_weeks, contract_type)

    work_days_per_week = joining_template_with_contract_per_week(
        work_days_per_week,
        week_template,
        min_work_days,
        max_work_days,
        worker_id,
        contract_type,
    )

    worker_absences, vacation_days, fixed_days_off, fixed_LQs = days_off_atributtion(
        worker_id,
        worker_absences,
        vacation_days,
        fixed_days_off,
        fixed_LQs,
        week_to_days_salsa,
        closed_holidays,
        work_days_per_week,
        year_range,
    )

    working_days = (
        set(days_of_year) - empty_days_set - worker_absences - vacation_days - closed_holidays
    )
    dynamic_empty: Set[int] = set()

    if contract_type <= 4:
        worker_absences, vacation_days, dynamic_empty = absences_to_empty(
            worker_absences,
            vacation_days,
            contract_type,
            week_to_days_salsa,
            empty_days_set,
        )

    return FinalizedWorkerCalendarSets(
        empty_days=empty_days_set,
        vacation_days=vacation_days,
        worker_absences=worker_absences,
        fixed_days_off=fixed_days_off,
        fixed_LQs=fixed_LQs,
        fixed_compensation_days=fixed_compensation_days,
        shift_M=shift_M,
        shift_T=shift_T,
        forced_work_days=forced_work_days,
        locked_days=locked_days,
        complete_cycle_days=complete_cycle_days,
        free_day_complete_cycle=free_day_complete_cycle,
        work_days_per_week=work_days_per_week,
        working_days=working_days,
        dynamic_empty=dynamic_empty,
    )


def build_read_salsa_worker_calendar(
    df_calendario: pd.DataFrame,
    df_colaborador: pd.DataFrame,
    employee_id: str,
    closed_holidays: Set[int],
    year_range: List[int],
    period: Tuple[int, int],
    extra_vacation_indices: Optional[Set[int]] = None,
    extra_absence_indices: Optional[Set[int]] = None,
) -> Optional[FinalizedWorkerCalendarSets]:
    """
    Run read_salsa worker calendar pipeline for one employee on df_calendario.
    """
    emp_cal = df_calendario[
        df_calendario['employee_id'].astype(str) == str(employee_id)
    ]
    if emp_cal.empty:
        return None

    emp_colab = df_colaborador[
        df_colaborador['employee_id'].astype(str) == str(employee_id)
    ]
    if emp_colab.empty:
        return None

    days_of_year, max_day, _, week_to_days_salsa, nbr_weeks = (
        build_days_of_year_and_week_maps(df_calendario, closed_holidays)
    )
    if not days_of_year or nbr_weeks <= 0:
        return None

    raw = extract_worker_horario_sets(emp_cal)
    if extra_vacation_indices:
        raw['vacation_days'] = list(set(raw['vacation_days']) | extra_vacation_indices)
    if extra_absence_indices:
        raw['worker_absences'] = list(set(raw['worker_absences']) | extra_absence_indices)
    week_template = build_week_template_for_worker(
        raw['week_template_temp'], week_to_days_salsa
    )

    row = emp_colab.iloc[0]
    try:
        contract_type = int(row.get('tipo_contrato', 0))
    except (TypeError, ValueError):
        contract_type = 0
    try:
        min_work_days = int(row.get('min_dia_trab', 0))
    except (TypeError, ValueError):
        min_work_days = 0
    try:
        max_work_days = int(row.get('max_dia_trab', 0))
    except (TypeError, ValueError):
        max_work_days = 0

    first_registered_day = int(emp_cal['index'].min())
    last_registered_day = int(emp_cal['index'].max())

    return finalize_worker_calendar_sets(
        str(employee_id),
        raw,
        first_registered_day=first_registered_day,
        last_registered_day=last_registered_day,
        days_of_year=days_of_year,
        max_day=max_day,
        closed_holidays=closed_holidays,
        week_to_days_salsa=week_to_days_salsa,
        nbr_weeks=nbr_weeks,
        week_template=week_template,
        contract_type=contract_type,
        min_work_days=min_work_days,
        max_work_days=max_work_days,
        year_range=year_range,
        period=period,
    )
