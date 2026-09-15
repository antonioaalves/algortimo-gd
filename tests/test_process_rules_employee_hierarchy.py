"""
STRSOL-1775: employee-level compensatory process rules hierarchy tests.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_models.functions.data_treatment_functions import (
    add_process_rules_to_df_contratos,
    treat_df_process_rules,
)


def _tall_rule_rows(
    *,
    process_id=1,
    labor_union_id=None,
    contract_id=None,
    employee_id=None,
    begin_date='2026-01-01',
    end_date='2026-01-03',
    rule_code='COMPENSATORY_TIME_OFF_SUNDAYS',
    rule_id=10,
    rule_head_id=100,
    time_off_additional=1,
    time_off_deadline=15,
):
    base = {
        'process_id': process_id,
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
        'rule_field_id': 1,
        'field_type': 'N',
    }
    return [
        {**base, 'field_code': 'TIME_OFF_ADDITIONAL', 'value': time_off_additional},
        {**base, 'field_code': 'TIME_OFF_DEADLINE', 'value': time_off_deadline, 'order_seq': 2, 'rule_field_id': 2},
    ]


def _treat_rules(raw_rows, first_date='2026-01-01', last_date='2026-01-03'):
    success, df_treated, error = treat_df_process_rules(
        pd.DataFrame(raw_rows),
        first_date=first_date,
        last_date=last_date,
    )
    assert success, error
    return df_treated


def _day_level_contracts(rows):
    return pd.DataFrame(rows)


class TestTreatDfProcessRulesEmployeeLevel:
    def test_pivot_preserves_employee_level_rules(self):
        raw = _tall_rule_rows(
            employee_id=4710,
            rule_head_id=178,
            time_off_additional=2,
            time_off_deadline=20,
        )
        df_treated = _treat_rules(raw)

        assert not df_treated.empty
        assert df_treated['employee_id'].notna().all()
        assert df_treated['contract_id'].isna().all()
        assert df_treated['labor_union_id'].isna().all()
        assert (df_treated['rule_head_id'] == 178).all()
        assert (df_treated['TIME_OFF_ADDITIONAL'] == 2).all()
        assert (df_treated['TIME_OFF_DEADLINE'] == 20).all()


class TestAddProcessRulesEmployeeHierarchy:
    def test_employee_overrides_contract_and_union(self):
        day = pd.Timestamp('2026-01-02')
        df_rules = _treat_rules(
            _tall_rule_rows(labor_union_id=1, rule_head_id=10, time_off_deadline=10)
            + _tall_rule_rows(contract_id=9, rule_head_id=20, time_off_deadline=12)
            + _tall_rule_rows(
                employee_id=4710,
                rule_head_id=178,
                time_off_deadline=99,
            )
        )
        df_contratos = _day_level_contracts([
            {'employee_id': 4710, 'schedule_day': day, 'contract_id': 9, 'labor_union_id': 1},
            {'employee_id': 9999, 'schedule_day': day, 'contract_id': 9, 'labor_union_id': 1},
        ])

        success, df_merged, error = add_process_rules_to_df_contratos(df_rules, df_contratos)
        assert success, error

        excepted = df_merged[df_merged['employee_id'] == 4710].iloc[0]
        sibling = df_merged[df_merged['employee_id'] == 9999].iloc[0]

        assert excepted['_rule_source'] == 'employee'
        assert excepted['rule_head_id'] == 178
        assert excepted['TIME_OFF_DEADLINE'] == 99

        assert sibling['_rule_source'] == 'contract'
        assert sibling['rule_head_id'] == 20
        assert sibling['TIME_OFF_DEADLINE'] == 12

    def test_per_rule_code_independence(self):
        day = pd.Timestamp('2026-01-02')
        df_rules = _treat_rules(
            _tall_rule_rows(
                employee_id=4710,
                rule_code='COMPENSATORY_TIME_OFF_SUNDAYS',
                rule_head_id=178,
                time_off_deadline=99,
            )
            + _tall_rule_rows(
                contract_id=9,
                rule_code='COMPENSATORY_TIME_OFF_HOLIDAYS',
                rule_head_id=200,
                time_off_deadline=11,
            )
            + _tall_rule_rows(
                labor_union_id=1,
                rule_code='COMPENSATORY_TIME_OFF_HOLIDAYS',
                rule_head_id=300,
                time_off_deadline=5,
            )
        )
        df_contratos = _day_level_contracts([
            {'employee_id': 4710, 'schedule_day': day, 'contract_id': 9, 'labor_union_id': 1},
        ])

        success, df_merged, error = add_process_rules_to_df_contratos(df_rules, df_contratos)
        assert success, error

        sunday = df_merged[df_merged['rule_code'] == 'ld_sunday'].iloc[0]
        holiday = df_merged[df_merged['rule_code'] == 'ld_holiday'].iloc[0]

        assert sunday['_rule_source'] == 'employee'
        assert sunday['rule_head_id'] == 178
        assert holiday['_rule_source'] == 'contract'
        assert holiday['rule_head_id'] == 200

    def test_labor_union_fallback_when_no_employee_or_contract(self):
        day = pd.Timestamp('2026-01-02')
        df_rules = _treat_rules(
            _tall_rule_rows(labor_union_id=1, rule_head_id=300, time_off_deadline=7)
        )
        df_contratos = _day_level_contracts([
            {'employee_id': 100, 'schedule_day': day, 'contract_id': 9, 'labor_union_id': 1},
        ])

        success, df_merged, error = add_process_rules_to_df_contratos(df_rules, df_contratos)
        assert success, error
        assert len(df_merged) == 1
        assert df_merged.iloc[0]['_rule_source'] == 'labor_union'
        assert df_merged.iloc[0]['rule_head_id'] == 300
