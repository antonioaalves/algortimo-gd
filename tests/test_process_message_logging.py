"""STRSOL-1847: batch WFM message writers in process_message_logging."""

from unittest.mock import patch

import pandas as pd

from src.orquestrador_functions.Logs.process_message_labels import build_process_message_labels
from src.orquestrador_functions.Logs.process_message_logging import (
    log_missing_ciclos_warnings,
    log_workload_template_contract_errors,
)


def _df_messages():
    return pd.read_csv('data/df_messages.csv')


def _common_kwargs(df_messages, events):
    return dict(
        connection=object(),
        path_os='/tmp',
        fk_process=99,
        process_type='GD',
        df_messages=df_messages,
        warning_events=events,
        child_num='1',
        posto_id=1361,
    )


@patch('src.orquestrador_functions.Logs.process_message_logging.set_process_errors', return_value=0)
def test_empty_events_returns_zero_without_insert(mock_set):
    assert log_missing_ciclos_warnings(**_common_kwargs(_df_messages(), [])) == 0
    mock_set.assert_not_called()


@patch('src.orquestrador_functions.Logs.process_message_logging.set_process_errors', return_value=0)
def test_no_connection_returns_zero(mock_set):
    kwargs = _common_kwargs(_df_messages(), [{'employee_id': '1'}])
    kwargs['connection'] = None
    assert log_missing_ciclos_warnings(**kwargs) == 0
    mock_set.assert_not_called()


@patch('src.orquestrador_functions.Logs.process_message_logging.set_process_errors', return_value=1)
def test_one_event_persists_warning_with_template(mock_set):
    labels = build_process_message_labels(
        pd.DataFrame({'fk_tipo_posto': [1361], 'nome_tipo_posto': ['Caja']}),
        [pd.DataFrame({'employee_id': ['10986'], 'nome': ['Ana Silva'], 'matricula': ['81001884']})],
    )
    events = [{
        'employee_id': '10986',
        'period_begin': '2026-07-01',
        'period_end': '2026-07-24',
    }]
    n = log_missing_ciclos_warnings(
        **_common_kwargs(_df_messages(), events),
        labels=labels,
    )
    assert n == 1
    mock_set.assert_called_once()
    _, kwargs = mock_set.call_args
    assert kwargs['type_error'] == 'W'
    assert kwargs['employee_id'] == 10986
    assert kwargs['schedule_day'] == '2026-07-01'
    assert 'Ana Silva' in kwargs['description']
    assert '81001884' in kwargs['description']
    assert 'Caja' in kwargs['description']


@patch('src.orquestrador_functions.Logs.process_message_logging.set_process_errors', return_value=1)
def test_missing_template_uses_fallback_description(mock_set):
    df_messages = pd.DataFrame({'VAR': ['WARN_MISSING_CICLOS'], 'ES': [''], 'PT': [''], 'EN': ['']})
    events = [{
        'employee_id': '42',
        'nome': 'Bob',
        'matricula': '123',
        'period_begin': '2026-01-01',
        'period_end': '2026-01-31',
    }]
    log_missing_ciclos_warnings(**_common_kwargs(df_messages, events))
    description = mock_set.call_args.kwargs['description']
    assert 'Bob' in description
    assert 'no tiene ciclo configurado' in description


@patch('src.orquestrador_functions.Logs.process_message_logging.set_process_errors', return_value=1)
def test_fatal_workload_error_type(mock_set):
    df_messages = _df_messages()
    log_workload_template_contract_errors(
        connection=object(),
        path_os='/tmp',
        fk_process=1,
        process_type='GD',
        df_messages=df_messages,
        error_events=[{
            'employee_id': '7',
            'workload_template': '5x2',
            'min_dia_trab': 4,
            'max_dia_trab': 5,
            'period_begin': '2026-03-01',
        }],
    )
    assert mock_set.call_args.kwargs['type_error'] == 'E'
