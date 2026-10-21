"""Rendered df_messages must fit wfm SET_PROCESS_ERRORS description (VARCHAR2 242)."""

import pandas as pd

from src.orquestrador_functions.Logs.message_loader import set_messages

WFM_DESCRIPTION_MAX = 242

# Stress placeholders for STRSOL-1847 validation messages (worst-case labels).
_STRESS = {
    'ERR_MAX_CONSECUTIVE_WORKING_DAYS': {
        '1': '1',
        '2': 'Nombre Muy Largo Del Colaborador Ejemplo',
        '3': '81001884',
        '4': '6',
        '5': '12',
        '6': '2026-01-01',
        '7': '2026-12-31',
        '9': 'Nombre Largo Del Puesto Trabajo',
    },
    'ERR_WORKLOAD_TEMPLATE_CONTRACT': {
        '1': '1',
        '2': 'Nombre Muy Largo Del Colaborador Ejemplo',
        '3': '81001884',
        '4': '6',
        '5': '4',
        '6': '6',
        '7': 'Semana 52',
        '8': ' (2026-07-01 a 2026-07-07)',
        '9': 'Nombre Largo Del Puesto Trabajo',
    },
    'WARN_MISSING_CICLOS': {
        '1': '1',
        '2': 'Nombre Muy Largo Del Colaborador Ejemplo',
        '3': '81001884',
        '6': '2026-01-01',
        '7': '2026-12-31',
        '8': 'Nombre Largo Del Puesto Trabajo',
    },
    'WARN_FEASIBILITY_CAP': {
        '1': '1',
        '2': 'Nombre Muy Largo Del Colaborador Ejemplo',
        '3': 'domingos',
        '4': '10',
        '5': '4',
        '6': '2026-01-01',
        '7': '2026-12-31',
        '8': '81001884',
    },
    'WARN_UNCONVERTED_FD': {
        '1': '1',
        '2': 'Nombre Muy Largo Del Colaborador Ejemplo',
        '3': '81001884',
        '4': '2026-07-15',
        '5': 'Nombre Largo Del Puesto Trabajo',
    },
}


def test_strsol_validation_messages_fit_wfm_description_limit():
    df_messages = pd.read_csv('data/df_messages.csv')
    for var, placeholders in _STRESS.items():
        for lang in ('ES', 'PT', 'EN'):
            rendered = set_messages(df_messages, var, placeholders, lang=lang)
            assert rendered, f'{var} {lang}: empty template'
            assert len(rendered) <= WFM_DESCRIPTION_MAX, (
                f'{var} {lang}: len={len(rendered)} > {WFM_DESCRIPTION_MAX}: {rendered!r}'
            )
