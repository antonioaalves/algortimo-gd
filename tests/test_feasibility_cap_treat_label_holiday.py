"""Feasibility cap parity with STRSOL-1820 treat_label_holiday."""

from __future__ import annotations

import pandas as pd

from src.data_models.functions.data_treatment_functions import (
    _closed_holiday_dates_for_cap,
    _count_eligible_weekends_c2d,
    _count_feasibility_cap_for_field,
)


def _idx_maps_for_days(days: list[pd.Timestamp]) -> tuple[dict, dict]:
    idx_by_date = {d.normalize(): i + 1 for i, d in enumerate(days)}
    date_by_idx = {i: d for d, i in idx_by_date.items()}
    return idx_by_date, date_by_idx


class TestClosedHolidayDatesForCap:
    def test_treat_zero_excludes_label_replaced_feriado(self):
        christmas = pd.Timestamp('2025-12-25')
        days = pd.date_range('2025-12-22', '2025-12-28', freq='D')
        idx_by_date, date_by_idx = _idx_maps_for_days(list(days))

        emp_cal = pd.DataFrame(
            {
                'index': [idx_by_date[d] for d in days],
                'horario': ['M', 'M', 'M', 'L', 'L', 'M', 'M'],
            }
        )
        holiday_set = frozenset({christmas})

        blocking = _closed_holiday_dates_for_cap(
            emp_cal, holiday_set, idx_by_date, date_by_idx, treat_label_holiday=0
        )
        assert christmas not in blocking

        blocking_legacy = _closed_holiday_dates_for_cap(
            emp_cal, holiday_set, idx_by_date, date_by_idx, treat_label_holiday=1
        )
        assert christmas in blocking_legacy


class TestC2dEligibleWeekendsHolidayRule:
    def test_weekend_not_blocked_when_holiday_is_l_on_calendar(self):
        """c2d Rule 3 must use STRSOL-1820 blocking set, not raw df_feriados."""
        sat = pd.Timestamp('2025-12-27')
        sun = pd.Timestamp('2025-12-28')
        christmas = pd.Timestamp('2025-12-25')
        days = pd.date_range('2025-12-22', '2025-12-28', freq='D')
        idx_by_date, date_by_idx = _idx_maps_for_days(list(days))

        emp_cal = pd.DataFrame(
            {
                'index': [idx_by_date[d] for d in days],
                'horario': ['M', 'M', 'M', 'L', 'L', 'M', 'M'],
            }
        )
        blocking = _closed_holiday_dates_for_cap(
            emp_cal, frozenset({christmas}), idx_by_date, date_by_idx, treat_label_holiday=0
        )
        assert christmas not in blocking

        count = _count_eligible_weekends_c2d(
            begin=sat,
            effective_end=sun,
            weekly_rest=frozenset(),
            adjacent_free=frozenset(),
            non_working=frozenset(),
            holiday_set=blocking,
            tipo_contrato=5,
        )
        assert count == 1

        count_raw = _count_eligible_weekends_c2d(
            begin=sat,
            effective_end=sun,
            weekly_rest=frozenset(),
            adjacent_free=frozenset(),
            non_working=frozenset(),
            holiday_set=frozenset({christmas}),
            tipo_contrato=5,
        )
        assert count_raw == 0


class TestCountFeasibilityCapC2dUsesBlockingHolidays:
    def test_cap_field_receives_blocking_set_not_all_feriados(self):
        exec_begin = pd.Timestamp('2025-12-07')
        exec_end = pd.Timestamp('2025-12-31')
        christmas = pd.Timestamp('2025-12-25')
        blocking = frozenset()  # treat_label_holiday=0 style: no closed F left
        all_feriados = frozenset({christmas})

        kwargs = dict(
            annual_begin=exec_begin,
            annual_end=exec_end,
            effective_annual_end=exec_end,
            exec_begin=exec_begin,
            exec_end=exec_end,
            weekly_rest=frozenset(),
            adjacent_free=frozenset(),
            non_working=frozenset(),
            tipo_ciclo_weeks=frozenset(),
            tipo_contrato_resolver=lambda _d: 5,
            default_tipo=5,
        )

        sat = pd.Timestamp('2025-12-27')
        sun = pd.Timestamp('2025-12-28')
        achievable_blocking, _ = _count_feasibility_cap_for_field(
            field='c2d', holiday_set=blocking, **kwargs
        )
        achievable_raw, _ = _count_feasibility_cap_for_field(
            field='c2d', holiday_set=all_feriados, **kwargs
        )
        # Only difference when a weekend is adjacent to a feriado in Rule 3 / satisfied_outside;
        # with a single weekend in window, raw feriados block Sat-Sun if 25th incorrectly counts.
        assert achievable_blocking >= achievable_raw
