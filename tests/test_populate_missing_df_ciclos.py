"""Tests for populate_missing_df_ciclos backfill and warning event generation."""

import pandas as pd

from src.data_models.functions.data_treatment_functions import (
    populate_missing_df_ciclos,
    treat_df_ciclos_completos,
)
from src.orquestrador_functions.Logs.message_loader import set_messages


def _sample_colaborador():
    return pd.DataFrame({
        'employee_id': ['10986'],
        'contract_id': [1],
        'labor_union': [100],
        'min_dia_trab': [5],
        'max_dia_trab': [5],
        'begin_date': ['2026-07-01'],
        'end_date': ['2026-07-03'],
        'matricula': [81001884],
    })


def test_populate_missing_df_ciclos_backfills_contract_active_days():
    df_ciclos = pd.DataFrame(columns=[
        'process_id', 'employee_id', 'matricula', 'schedule_day', 'tipo_dia',
        'tipo_ciclo', 'descanso', 'horario_ind', 'hora_ini_1', 'hora_fim_1',
        'hora_ini_2', 'hora_fim_2', 'fk_horario', 'nro_semana', 'dia_semana',
        'work_shift', 'work_shift_start', 'work_shift_end', 'minimumworkday',
        'maximumworkday', 'workload_template',
    ])

    success, df_result, events, error = populate_missing_df_ciclos(
        df_ciclos_completos_folgas_ciclos=df_ciclos,
        df_colaborador=_sample_colaborador(),
        employees_id_list=['10986'],
        process_id=60227,
        start_date='2026-01-01',
        end_date='2026-12-31',
    )

    assert success is True
    assert error == ''
    assert len(df_result) == 3
    assert len(events) == 1
    assert events[0]['employee_id'] == '10986'
    assert events[0]['period_begin'] == '2026-07-01'
    assert events[0]['period_end'] == '2026-07-03'
    assert events[0]['missing_days_count'] == 3

    row = df_result.iloc[0]
    assert row['tipo_dia'] == 'A'
    assert row['tipo_ciclo'] == 'N'
    assert row['descanso'] == 'A'
    assert row['horario_ind'] == 'N'
    assert row['work_shift'] == 'A'
    assert row['workload_template'] == 'A'
    assert pd.isna(row['fk_horario'])
    assert row['minimumworkday'] == 5
    assert row['maximumworkday'] == 5


def test_populate_missing_df_ciclos_skips_when_all_days_present():
    df_ciclos = pd.DataFrame({
        'process_id': [60227, 60227, 60227],
        'employee_id': ['10986', '10986', '10986'],
        'matricula': [81001884, 81001884, 81001884],
        'schedule_day': ['2026-07-01', '2026-07-02', '2026-07-03'],
        'tipo_dia': ['A', 'A', 'A'],
        'tipo_ciclo': ['N', 'N', 'N'],
        'descanso': ['A', 'A', 'A'],
        'horario_ind': ['N', 'N', 'N'],
        'work_shift': ['A', 'A', 'A'],
        'workload_template': ['A', 'A', 'A'],
        'minimumworkday': [5, 5, 5],
        'maximumworkday': [5, 5, 5],
        'nro_semana': [27, 27, 27],
        'dia_semana': [3, 4, 5],
    })

    success, df_result, events, error = populate_missing_df_ciclos(
        df_ciclos_completos_folgas_ciclos=df_ciclos,
        df_colaborador=_sample_colaborador(),
        employees_id_list=['10986'],
        process_id=60227,
        start_date='2026-01-01',
        end_date='2026-12-31',
    )

    assert success is True
    assert error == ''
    assert len(events) == 0
    assert len(df_result) == 3


def test_populate_missing_df_ciclos_backfills_only_missing_days():
    df_ciclos = pd.DataFrame({
        'process_id': [60227],
        'employee_id': ['10986'],
        'matricula': [81001884],
        'schedule_day': ['2026-07-02'],
        'tipo_dia': ['A'],
        'tipo_ciclo': ['N'],
        'descanso': ['A'],
        'horario_ind': ['N'],
        'work_shift': ['A'],
        'workload_template': ['A'],
        'minimumworkday': [5],
        'maximumworkday': [5],
        'nro_semana': [27],
        'dia_semana': [4],
    })

    success, df_result, events, error = populate_missing_df_ciclos(
        df_ciclos_completos_folgas_ciclos=df_ciclos,
        df_colaborador=_sample_colaborador(),
        employees_id_list=['10986'],
        process_id=60227,
        start_date='2026-01-01',
        end_date='2026-12-31',
    )

    assert success is True
    assert error == ''
    assert len(df_result) == 3
    assert len(events) == 1
    assert events[0]['missing_days_count'] == 2
    backfilled_days = sorted(
        pd.to_datetime(df_result['schedule_day']).dt.strftime('%Y-%m-%d').tolist()
    )
    assert backfilled_days == ['2026-07-01', '2026-07-02', '2026-07-03']


def test_backfilled_ciclos_survive_treatment_and_produce_horario():
    success, df_result, events, _ = populate_missing_df_ciclos(
        df_ciclos_completos_folgas_ciclos=pd.DataFrame(),
        df_colaborador=_sample_colaborador(),
        employees_id_list=['10986'],
        process_id=60227,
        start_date='2026-01-01',
        end_date='2026-12-31',
    )
    assert success is True
    assert len(events) == 1

    treat_ok, df_treated, treat_err = treat_df_ciclos_completos(df_result)
    assert treat_ok is True
    assert treat_err == ''
    assert 'horario' in df_treated.columns
    assert (df_treated['workload_template'] == 'A').all()
    assert df_treated['horario'].notna().all()


def test_warn_missing_ciclos_message_renders_all_languages():
    df_messages = pd.DataFrame({
        'VAR': ['WARN_MISSING_CICLOS'],
        'ES': [
            'Subproceso {1}: el colaborador {2}{3} no tiene ciclo configurado para el '
            'periodo [{6}-{7}]. Configure el ciclo en WFM. Puesto {8}'
        ],
        'PT': [
            'Subprocesso {1}: o colaborador {2}{3} nao tem ciclo configurado para o '
            'periodo [{6}-{7}]. Configure o ciclo no WFM. Posto {8}'
        ],
        'EN': [
            'Subprocess {1}: employee {2}{3} has no cycle configured for period '
            '[{6}-{7}]. Configure the cycle in WFM. Post {8}'
        ],
    })
    placeholders = {
        '1': '1',
        '2': '10986',
        '3': ', matrícula 81001884',
        '6': '2026-07-01',
        '7': '2026-07-24',
        '8': '1361',
    }
    assert '10986' in set_messages(df_messages, 'WARN_MISSING_CICLOS', placeholders, lang='ES')
    assert '10986' in set_messages(df_messages, 'WARN_MISSING_CICLOS', placeholders, lang='PT')
    assert '10986' in set_messages(df_messages, 'WARN_MISSING_CICLOS', placeholders, lang='EN')
