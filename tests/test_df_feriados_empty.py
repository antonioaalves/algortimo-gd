"""Zero-row df_feriados from the query (columns present) must pass treatment and validation."""

import pandas as pd

from src.data_models.functions.data_treatment_functions import (
    add_date_related_columns,
    treat_df_feriados,
)
from src.data_models.validations.load_process_data_validations import validate_df_feriados

_FERIADOS_QUERY_COLUMNS = [
    'schedule_day',
    'tipo_feriado',
    'fk_unidade',
    'fk_cidade',
    'fk_estado',
    'fk_pais',
]


def _empty_feriados_from_query() -> pd.DataFrame:
    return pd.DataFrame(columns=_FERIADOS_QUERY_COLUMNS)


def test_treat_df_feriados_empty_passes_through():
    df_in = _empty_feriados_from_query()
    success, df_out, error_msg = treat_df_feriados(df_in)

    assert success is True
    assert error_msg == ""
    assert df_out.empty
    assert list(df_out.columns) == _FERIADOS_QUERY_COLUMNS


def test_treat_df_feriados_empty_renames_database_column():
    df_in = pd.DataFrame(columns=['database', 'tipo_feriado'])
    success, df_out, error_msg = treat_df_feriados(df_in)

    assert success is True
    assert error_msg == ""
    assert 'schedule_day' in df_out.columns
    assert 'database' not in df_out.columns


def test_validate_df_feriados_empty_with_schema():
    assert validate_df_feriados(_empty_feriados_from_query()) is True


def test_validate_df_feriados_empty_missing_columns():
    df = pd.DataFrame(columns=['schedule_day'])
    assert validate_df_feriados(df) is False


def test_add_date_related_columns_empty_feriados_requires_allow_empty():
    df_in = _empty_feriados_from_query()
    success, _, error_msg = add_date_related_columns(
        df=df_in,
        date_col='schedule_day',
        use_case=1,
        main_year=2026,
    )
    assert success is False
    assert "empty" in error_msg.lower()


def test_add_date_related_columns_empty_feriados_with_allow_empty():
    df_in = _empty_feriados_from_query()
    success, df_out, error_msg = add_date_related_columns(
        df=df_in,
        date_col='schedule_day',
        use_case=1,
        main_year=2026,
        allow_empty=True,
    )

    assert success is True
    assert error_msg == ""
    assert df_out.empty
    assert set(['index', 'wday', 'ww', 'wd', 'year']).issubset(df_out.columns)
