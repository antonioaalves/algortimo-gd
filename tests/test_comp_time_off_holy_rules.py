"""
STRSOL-1776: COMP_TIME_OFF_EMPTY_ON_HOLY / COMP_TIME_OFF_REST_ON_HOLY pipeline tests.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_models.functions.data_treatment_functions import (
    add_process_rules_to_df_contratos,
    build_compensatory_output,
    filter_compensatory_labor_rules,
    treat_df_process_rules,
    treat_df_pro_emp_mov,
)


def _tall_rule_rows(
    *,
    labor_union_id=None,
    contract_id=None,
    employee_id=None,
    begin_date='2026-01-01',
    end_date='2026-01-01',
    rule_code='COMP_TIME_OFF_EMPTY_ON_HOLY',
    rule_id=543,
    rule_head_id=345,
    time_off_additional=1,
    time_off_deadline=90,
    rest_day_type='F',
    rest_day_subtype='CH',
):
    base = {
        'process_id': 54541,
        'labor_union_id': labor_union_id,
        'contract_id': contract_id,
        'employee_id': employee_id,
        'begin_date': begin_date,
        'end_date': end_date,
        'rule_code': rule_code,
        'rule_id': rule_id,
        'rule_head_id': rule_head_id,
        'priority': 1,
        'order_seq': 1,
        'rule_field_id': 41,
        'field_type': 'N',
    }
    return [
        {**base, 'field_code': 'TIME_OFF_ADDITIONAL', 'value': time_off_additional, 'order_seq': 1},
        {**base, 'field_code': 'TIME_OFF_DEADLINE', 'value': time_off_deadline, 'order_seq': 2, 'rule_field_id': 42},
        {
            **base, 'field_code': 'REST_DAY_TYPE', 'value': rest_day_type, 'order_seq': 3,
            'rule_field_id': 43, 'field_type': 'C',
        },
        {
            **base, 'field_code': 'REST_DAY_SUBTYPE', 'value': rest_day_subtype, 'order_seq': 4,
            'rule_field_id': 44, 'field_type': 'C',
        },
    ]


def _treat_rules(raw_rows, first_date='2026-01-01', last_date='2026-12-31'):
    success, df_treated, error = treat_df_process_rules(
        pd.DataFrame(raw_rows),
        first_date=first_date,
        last_date=last_date,
    )
    assert success, error
    return df_treated


class TestCompTimeOffHolyRuleCodes:
    def test_filter_and_map_wfm_codes(self):
        raw = pd.DataFrame(_tall_rule_rows() + _tall_rule_rows(
            rule_code='COMP_TIME_OFF_REST_ON_HOLY',
            rule_id=544,
            rule_head_id=346,
            time_off_deadline=28,
        ))
        filtered = filter_compensatory_labor_rules(raw)
        assert len(filtered) == 8

        df_treated = _treat_rules(raw.to_dict('records'))
        codes = set(df_treated['rule_code'].unique())
        assert codes == {'ld_empty_day', 'ld_holiday_dayoff'}

    def test_labor_union_hierarchy_deadlines(self):
        day = pd.Timestamp('2026-09-01')
        raw = (
            _tall_rule_rows(
                labor_union_id=16, rule_head_id=347, time_off_deadline=90,
                begin_date='2026-09-01', end_date='2026-09-01',
            )
            + _tall_rule_rows(
                labor_union_id=2, rule_head_id=348, time_off_deadline=28,
                begin_date='2026-09-01', end_date='2026-09-01',
            )
        )
        df_rules = _treat_rules(raw, first_date='2026-09-01', last_date='2026-09-01')
        df_contratos = pd.DataFrame([
            {'employee_id': 1, 'schedule_day': day, 'contract_id': 9, 'labor_union_id': 16},
            {'employee_id': 2, 'schedule_day': day, 'contract_id': 9, 'labor_union_id': 2},
        ])
        success, df_merged, error = add_process_rules_to_df_contratos(df_rules, df_contratos)
        assert success, error

        e1 = df_merged[(df_merged['employee_id'] == 1)].iloc[0]
        e2 = df_merged[(df_merged['employee_id'] == 2)].iloc[0]
        assert e1['_rule_source'] == 'labor_union'
        assert e1['TIME_OFF_DEADLINE'] == 90
        assert e2['TIME_OFF_DEADLINE'] == 28


class TestTreatDfProEmpMovHolyRules:
    def test_pending_mov_maps_wfm_rule_code(self):
        mov = pd.DataFrame([{
            'process_id': 1,
            'employee_id': 100,
            'schedule_day': '2026-05-01',
            'rule_head_id': 345,
            'rule_code': 'COMP_TIME_OFF_EMPTY_ON_HOLY',
            'rule_field_code': 'TIME_OFF_ADDITIONAL',
            'value': 1,
            'value_opt1': 'O',
            'value_opt2': 1,
        }])
        raw_rules = pd.DataFrame(_tall_rule_rows())
        success, df_out, error = treat_df_pro_emp_mov(mov, pd.DataFrame(), df_process_rules_raw=raw_rules)
        assert success, error
        assert df_out.iloc[0]['rule_code'] == 'ld_empty_day'
        assert df_out.iloc[0]['time_off_deadline'] == 90


class TestBuildCompensatoryOutputHolyGroups:
    def test_ld_empty_day_group_writes_wfm_rule_code(self):
        compensatory_dict = {
            100: {
                'feriados': {'ld_given': [], 'no_compensation': []},
                'domingos': {'ld_given': [], 'no_compensation': []},
                'ld_empty_day': {
                    'ld_given': [('2026-05-01', '2026-05-10')],
                    'no_compensation': [],
                },
                'ld_holiday_dayoff': {'ld_given': [], 'no_compensation': []},
            },
        }
        raw_rules = pd.DataFrame(_tall_rule_rows(
            begin_date='2026-05-01', end_date='2026-05-01', labor_union_id=16,
        ))
        merged = _treat_rules(raw_rules.to_dict('records'), first_date='2026-05-01', last_date='2026-05-01')
        merged['employee_id'] = 100

        success, df_out, error = build_compensatory_output(
            compensatory_dict=compensatory_dict,
            process_id=54541,
            df_process_rules_raw=raw_rules,
            df_process_rules_merged=merged,
        )
        assert success, error
        assert set(df_out['RULE_CODE'].unique()) == {'COMP_TIME_OFF_EMPTY_ON_HOLY'}
        assert len(df_out) == 2
        assert set(df_out['VALUE_OPT1']) == {'O', 'D'}
