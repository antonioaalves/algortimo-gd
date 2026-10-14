"""
Compensatory schedule type overrides (apply_compensatory_sched_types).
"""

import os
import sys

import pandas as pd
import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_models.functions.data_treatment_functions import (
    _resolve_compensatory_rest_types,
    apply_compensatory_sched_types,
)


def _final_row(employee_id, day, sched_type='F', sched_subtype='D'):
    return {
        'colaborador': employee_id,
        'data': pd.Timestamp(day),
        'sched_type': sched_type,
        'sched_subtype': sched_subtype,
    }


class TestResolveCompensatoryRestTypes:
    def test_destination_day_wins_over_origin(self):
        by_day = {
            (6101, 'ld_holiday_dayoff', pd.Timestamp('2026-10-19')): ('F', 'A'),
            (6101, 'ld_holiday_dayoff', pd.Timestamp('2026-10-12')): ('F', 'B'),
        }
        resolved = _resolve_compensatory_rest_types(
            6101,
            'ld_holiday_dayoff',
            '2026-10-12',
            '2026-10-19',
            by_day,
            {},
        )
        assert resolved == ('F', 'A')

    def test_origin_day_used_when_destination_missing(self):
        by_day = {
            (7962, 'ld_holiday', pd.Timestamp('2026-08-15')): ('F', 'CH'),
        }
        resolved = _resolve_compensatory_rest_types(
            7962,
            'ld_holiday',
            '2026-08-15',
            '2026-08-31',
            by_day,
            {},
        )
        assert resolved == ('F', 'CH')


class TestApplyCompensatorySchedTypes:
    def test_pending_mov_origin_types_applied_on_destination_row(self):
        """Process 55289 pattern: head expired before process start; mov on worked day."""
        final_df = pd.DataFrame([
            _final_row(7962, '2026-08-31'),
            _final_row(7266, '2026-10-19', sched_type='F', sched_subtype='D'),
        ])
        compensatory_dict = {
            7962: {
                'feriados': {'ld_given': [('2026-08-15', '2026-08-31')]},
                'domingos': {},
                'vazios': {},
                'folgas': {},
            },
            7266: {
                'feriados': {},
                'domingos': {},
                'vazios': {},
                'folgas': {'ld_given': [('2026-10-12', '2026-10-19')]},
            },
        }
        df_process_rules = pd.DataFrame([
            {
                'employee_id': 7266,
                'schedule_day': pd.Timestamp('2026-10-19'),
                'rule_code': 'ld_holiday_dayoff',
                'rest_day_type': 'F',
                'rest_day_subtype': 'XY',
            },
        ])
        df_pro_emp_mov = pd.DataFrame([
            {
                'employee_id': 7962,
                'schedule_day': pd.Timestamp('2026-08-15'),
                'rule_code': 'ld_holiday',
                'rest_day_type': 'F',
                'rest_day_subtype': 'CH',
            },
        ])

        result = apply_compensatory_sched_types(
            final_df,
            compensatory_dict,
            df_process_rules,
            df_pro_emp_mov=df_pro_emp_mov,
        )

        row_7962 = result[result['colaborador'] == 7962].iloc[0]
        assert row_7962['sched_type'] == 'F'
        assert row_7962['sched_subtype'] == 'CH'

        row_7266 = result[result['colaborador'] == 7266].iloc[0]
        assert row_7266['sched_subtype'] == 'XY'

    def test_in_window_rules_not_overwritten_by_mov(self):
        by_day_process = {
            (8968, 'ld_holiday', pd.Timestamp('2026-09-06')): ('F', 'IN'),
        }
        by_day_mov = {
            (8968, 'ld_holiday', pd.Timestamp('2026-08-15')): ('F', 'MOV'),
        }
        type_lookup_by_day = {**by_day_mov, **by_day_process}
        resolved = _resolve_compensatory_rest_types(
            8968,
            'ld_holiday',
            '2026-08-15',
            '2026-09-06',
            type_lookup_by_day,
            {},
        )
        assert resolved == ('F', 'IN')
