import os
import sys

import pandas as pd


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.data_models.functions.data_treatment_functions import treat_df_ausencias_ferias


def _build_df(is_holiday_value, fk_motivo_ausencia=9999, tipo_ausencia='A'):
    return pd.DataFrame(
        {
            'codigo': [1],
            'employee_id': ['101'],
            'matricula': ['80001001'],
            'data_ini': ['2025-01-02'],
            'data_fim': ['2025-01-02'],
            'tipo_ausencia': [tipo_ausencia],
            'fk_motivo_ausencia': [fk_motivo_ausencia],
            'is_holiday': [is_holiday_value],
        }
    )


def test_db_is_holiday_converts_to_v():
    success, df, error_msg = treat_df_ausencias_ferias(
        df_ausencias_ferias=_build_df(1),
        start_date='2025-01-01',
        end_date='2025-01-31',
        classification_mode='db_is_holiday',
    )

    assert success, error_msg
    assert df.iloc[0]['tipo_ausencia'] == 'V'


def test_db_is_holiday_zero_keeps_original_tipo():
    success, df, error_msg = treat_df_ausencias_ferias(
        df_ausencias_ferias=_build_df(0),
        start_date='2025-01-01',
        end_date='2025-01-31',
        classification_mode='db_is_holiday',
    )

    assert success, error_msg
    assert df.iloc[0]['tipo_ausencia'] == 'A'


def test_db_is_holiday_null_keeps_original_tipo_and_logs_warning(caplog):
    with caplog.at_level('WARNING'):
        success, df, error_msg = treat_df_ausencias_ferias(
            df_ausencias_ferias=_build_df(None),
            start_date='2025-01-01',
            end_date='2025-01-31',
            classification_mode='db_is_holiday',
        )

    assert success, error_msg
    assert df.iloc[0]['tipo_ausencia'] == 'A'
    assert 'null is_holiday' in caplog.text


def test_legacy_motivo_list_still_converts(monkeypatch):
    monkeypatch.setattr(
        'src.data_models.functions.data_treatment_functions._config.parameters.get_parameter_default',
        lambda parameter_name: [2101] if parameter_name == 'codigos_motivo_ausencia' else [],
    )

    success, df, error_msg = treat_df_ausencias_ferias(
        df_ausencias_ferias=_build_df(0, fk_motivo_ausencia=2101),
        start_date='2025-01-01',
        end_date='2025-01-31',
        classification_mode='legacy_motivo_list',
    )

    assert success, error_msg
    assert df.iloc[0]['tipo_ausencia'] == 'V'


def test_db_is_holiday_does_not_fallback_to_legacy_list(monkeypatch):
    monkeypatch.setattr(
        'src.data_models.functions.data_treatment_functions._config.parameters.get_parameter_default',
        lambda parameter_name: [2101] if parameter_name == 'codigos_motivo_ausencia' else [],
    )

    success, df, error_msg = treat_df_ausencias_ferias(
        df_ausencias_ferias=_build_df(0, fk_motivo_ausencia=2101),
        start_date='2025-01-01',
        end_date='2025-01-31',
        classification_mode='db_is_holiday',
    )

    assert success, error_msg
    assert df.iloc[0]['tipo_ausencia'] == 'A'
