"""CAV CICLO COMPLETO identification (Alcampo) — shared with add_seq_turno."""

import pandas as pd

from src.data_models.functions.helper_functions import (
    employee_ids_cav_ciclo_completo,
    mask_cav_ciclo_completo,
)


def test_mask_cav_ciclo_completo():
    df = pd.DataFrame({
        'seq_turno': ['CICLO', 'MoT', 'ciclo'],
        'ciclo': ['COMPLETO', 'COMPLETO', 'completo'],
    })
    mask = mask_cav_ciclo_completo(df)
    assert mask.tolist() == [True, False, True]


def test_employee_ids_respects_scope():
    df = pd.DataFrame({
        'employee_id': ['1', '2'],
        'seq_turno': ['CICLO', 'CICLO'],
        'ciclo': ['COMPLETO', 'COMPLETO'],
    })
    assert employee_ids_cav_ciclo_completo(df, employee_ids_scope=['2']) == ['2']
