"""STRSOL-1820: TREATLABELHOLIDAY validation and calendar F override behaviour."""

import pandas as pd
import pytest

from src.data_models.validations.load_process_data_validations import (
    validate_treat_label_holiday,
)
from src.data_models.functions.data_treatment_functions import (
    add_ciclos_completos,
    add_calendario_passado,
    add_folgas_ciclos,
    closed_holiday_preserve_mask,
    F_OVERRIDABLE_DAYOFF_HORARIOS,
)


class TestValidateTreatLabelHoliday:
    def test_success_zero(self):
        df = pd.DataFrame({'TREATLABELHOLIDAY': [0]})
        ok, code, val = validate_treat_label_holiday(df)
        assert ok is True
        assert code == ''
        assert val == 0

    def test_success_one(self):
        df = pd.DataFrame({'TREATLABELHOLIDAY': [1]})
        ok, code, val = validate_treat_label_holiday(df)
        assert ok and val == 1

    def test_missing_row(self):
        ok, code, val = validate_treat_label_holiday(pd.DataFrame())
        assert not ok
        assert code == 'ERR_TREAT_LABEL_HOLIDAY_MISSING'
        assert val is None

    def test_multiple_rows(self):
        df = pd.DataFrame({'TREATLABELHOLIDAY': [0, 1]})
        ok, code, val = validate_treat_label_holiday(df)
        assert not ok
        assert code == 'ERR_TREAT_LABEL_HOLIDAY_MULTIPLE'

    @pytest.mark.parametrize('value', [None, '', 2, 'x'])
    def test_invalid_value(self, value):
        df = pd.DataFrame({'TREATLABELHOLIDAY': [value]})
        ok, code, val = validate_treat_label_holiday(df)
        assert not ok
        assert code == 'ERR_TREAT_LABEL_HOLIDAY_INVALID'


class TestClosedHolidayPreserveMask:
    def test_flag_one_always_preserves_f(self):
        current = pd.Series(['F', 'F', 'M'])
        incoming = pd.Series(['L', 'M', 'L'])
        mask = closed_holiday_preserve_mask(current, incoming, treat_label_holiday=1)
        assert mask.tolist() == [True, True, False]

    def test_flag_zero_allows_dayoff_on_f(self):
        current = pd.Series(['F', 'F'])
        incoming = pd.Series(['L', 'M'])
        mask = closed_holiday_preserve_mask(current, incoming, treat_label_holiday=0)
        assert mask.tolist() == [False, True]

    def test_dayoff_codes_set(self):
        assert F_OVERRIDABLE_DAYOFF_HORARIOS == frozenset({'L', 'L_DOM', 'C', 'LQ'})


class TestTreatLabelHolidayCalendarioLayers:
    @pytest.fixture
    def df_with_closed_holiday(self):
        return pd.DataFrame({
            'employee_id': ['101', '101'],
            'schedule_day': ['2025-01-01', '2025-01-01'],
            'tipo_turno': ['M', 'T'],
            'horario': ['F', 'F'],
            'matricula': ['80001001', '80001001'],
            'wd': ['Wed', 'Wed'],
            'ww': [1, 1],
            'dia_tipo': ['', ''],
            'fixed': [False, False],
            'tipo_ciclo': [False, False],
        })

    def test_ciclos_l_overrides_f_when_flag_zero(self, df_with_closed_holiday):
        df_ciclos = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],
            'horario': ['L'],
        })
        _, df, _ = add_ciclos_completos(
            df_with_closed_holiday, df_ciclos, treat_label_holiday=0
        )
        assert set(df['horario']) == {'L'}

    def test_ciclos_l_preserves_f_when_flag_one(self, df_with_closed_holiday):
        df_ciclos = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],
            'horario': ['L'],
        })
        _, df, _ = add_ciclos_completos(
            df_with_closed_holiday, df_ciclos, treat_label_holiday=1
        )
        assert set(df['horario']) == {'F'}

    def test_passado_l_overrides_f_when_flag_zero(self, df_with_closed_holiday):
        df_passado = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],
            'horario': ['L'],
        })
        _, df, _ = add_calendario_passado(
            df_with_closed_holiday, df_passado, treat_label_holiday=0
        )
        assert set(df['horario']) == {'L'}

    def test_folgas_l_overrides_f_when_flag_zero(self, df_with_closed_holiday):
        df_folgas = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],
            'tipo_dia': ['L'],
        })
        _, df, _ = add_folgas_ciclos(
            df_with_closed_holiday, df_folgas, treat_label_holiday=0
        )
        assert set(df['horario']) == {'L'}
