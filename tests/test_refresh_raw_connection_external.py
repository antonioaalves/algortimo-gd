"""STRSOL-1854: external orchestrator connection health in _refresh_raw_connection."""

from unittest.mock import MagicMock, patch

from src.services.algoritmo_gd import AlgoritmoGDService


def _bare_service(**attrs):
    svc = AlgoritmoGDService.__new__(AlgoritmoGDService)
    svc.external_raw_connection = attrs.get('external_raw_connection')
    svc.raw_connection = attrs.get('raw_connection')
    svc.data_manager = MagicMock()
    svc.logger = MagicMock()
    return svc


@patch('src.services.algoritmo_gd.ensure_connection_with_config')
def test_external_refresh_replaces_stale_handle(mock_ensure):
    old_conn = MagicMock()
    new_conn = MagicMock()
    mock_ensure.return_value = new_conn

    svc = _bare_service(external_raw_connection=old_conn, raw_connection=old_conn)
    svc._refresh_raw_connection()

    mock_ensure.assert_called_once_with(old_conn)
    assert svc.external_raw_connection is new_conn
    assert svc.raw_connection is new_conn


@patch('src.services.algoritmo_gd.ensure_connection_with_config')
def test_external_refresh_keeps_same_handle_when_healthy(mock_ensure):
    conn = MagicMock()
    mock_ensure.return_value = conn

    svc = _bare_service(external_raw_connection=conn)
    svc._refresh_raw_connection()

    assert svc.external_raw_connection is conn
    assert svc.raw_connection is conn


@patch('src.services.algoritmo_gd.set_process_errors', return_value=1)
@patch('src.services.algoritmo_gd.ensure_connection_with_config')
def test_set_wfm_process_error_refreshes_before_insert(mock_ensure, mock_insert):
    conn = MagicMock()
    mock_ensure.return_value = conn
    svc = _bare_service(external_raw_connection=conn, raw_connection=conn)
    df_messages = MagicMock()
    df_messages.empty = False

    svc._set_wfm_process_error(
        df_messages,
        pathOS='/tmp',
        user='WFM',
        fk_process=1,
        type_error='I',
        process_type='GD',
        error_code=None,
        description='test',
        employee_id=None,
        schedule_day=None,
    )

    mock_ensure.assert_called_once()
    mock_insert.assert_called_once()
    assert mock_insert.call_args.kwargs['connection'] is conn
