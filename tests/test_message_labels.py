"""STRSOL-1834: user-facing labels in process-message placeholders."""

import logging

import pandas as pd

from src.orquestrador_functions.Logs.process_message_labels import (
    build_process_message_labels,
    format_employee_label,
    format_matricula_label,
    format_posto_label,
    format_section_label,
    format_unit_label,
    logger as label_logger,
    resolve_employee_labels,
)
from src.orquestrador_functions.Logs.message_loader import set_messages


def _estrutura():
    return pd.DataFrame({
        'fk_tipo_posto': [1361, 1362],
        'nome_tipo_posto': ['Caja', 'Reposición'],
        'nome_secao': ['Alimentación', 'Alimentación'],
        'nome_unidade': ['Tienda Norte', 'Tienda Norte'],
    })


def test_posto_label_uses_name():
    assert format_posto_label(_estrutura(), 1361) == 'Caja'
    assert format_posto_label(_estrutura(), '1362') == 'Reposición'


def test_posto_label_falls_back_to_id(caplog):
    caplog.set_level(logging.WARNING, logger=label_logger.name)
    assert format_posto_label(_estrutura(), 999) == '999'
    assert format_posto_label(pd.DataFrame(), 42) == '42'
    assert any('posto_id=999' in rec.message for rec in caplog.records)
    assert any('posto_id=42' in rec.message for rec in caplog.records)


def test_section_and_store_labels():
    df = _estrutura()
    assert format_section_label(df, 10) == 'Alimentación'
    assert format_unit_label(df, 7) == 'Tienda Norte'


def test_section_and_store_fall_back_to_id(caplog):
    caplog.set_level(logging.WARNING, logger=label_logger.name)
    assert format_section_label(pd.DataFrame(), 55) == '55'
    assert format_unit_label(pd.DataFrame(), 8) == '8'
    assert any('secao_id=55' in rec.message for rec in caplog.records)
    assert any('unit_id=8' in rec.message for rec in caplog.records)


def test_employee_label_hierarchy(caplog):
    caplog.set_level(logging.WARNING, logger=label_logger.name)
    assert format_employee_label('Ana Silva', '81001884', '10986') == 'Ana Silva'
    assert format_employee_label('', '81001884', '10986') == '81001884'
    assert format_employee_label(None, None, 10986) == '10986'
    assert format_matricula_label('81001884') == '81001884'
    assert format_matricula_label('') == 'n/d'
    assert format_matricula_label(None) == 'n/d'


def test_resolve_employee_labels_from_event_and_labels():
    event = {'employee_id': '10986', 'matricula': '', 'nome': ''}
    labels = build_process_message_labels(
        pd.DataFrame(),
        [pd.DataFrame({
            'employee_id': ['10986'],
            'nome': ['Ana Silva'],
            'matricula': [81001884],
        })],
    )
    person, matricula = resolve_employee_labels(event, labels)
    assert person == 'Ana Silva'
    assert matricula == '81001884'

    person_only_id, matricula_missing = resolve_employee_labels(
        {'employee_id': '10986'},
        None,
    )
    assert person_only_id == '10986'
    assert matricula_missing == 'n/d'


def test_process_message_labels_reuses_posto_lookup():
    labels = build_process_message_labels(_estrutura())
    assert format_posto_label(posto_id=1361, labels=labels) == 'Caja'
    assert format_posto_label(posto_id=1361, labels=labels) == 'Caja'
    assert labels.posto_names['1362'] == 'Reposición'


def test_shipped_templates_render_names():
    df_messages = pd.read_csv('data/df_messages.csv')

    err = set_messages(df_messages, 'errSubproc', {'1': '1', '2': 'should-not-appear'}, lang='ES')
    assert '{2}' not in err
    assert '{3}' not in err
    assert '[]' not in err
    assert 'should-not-appear' not in err

    inserted = set_messages(df_messages, 'errInsertWFM', {'1': '1', '2': 'Caja'}, lang='ES')
    assert inserted
    assert 'Caja' in inserted
    assert 'puesto Caja' in inserted

    ok_inserted = set_messages(df_messages, 'okInsertWFM', {'1': '2', '2': 'Caja'}, lang='PT')
    assert ok_inserted
    assert 'Caja' in ok_inserted

    missing = set_messages(
        df_messages,
        'WARN_MISSING_CICLOS',
        {
            '1': '1',
            '2': 'Ana Silva',
            '3': '81001884',
            '6': '2026-07-01',
            '7': '2026-07-24',
            '8': 'Caja',
        },
        lang='ES',
    )
    assert 'colaborador Ana Silva, matrícula 81001884' in missing
    assert 'Puesto Caja' in missing
    assert '10986' not in missing

    cap = set_messages(
        df_messages,
        'WARN_FEASIBILITY_CAP',
        {
            '1': '1',
            '2': 'Ana Silva',
            '3': 'domingos',
            '4': '10',
            '5': '4',
            '6': '2026-01-01',
            '7': '2026-12-31',
            '8': '81001884',
        },
        lang='ES',
    )
    assert 'colaborador Ana Silva, matrícula 81001884' in cap
    assert 'domingos' in cap
