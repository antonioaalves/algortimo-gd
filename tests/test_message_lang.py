import pandas as pd

from src.orquestrador_functions.Logs.message_loader import (
    apply_unit_message_lang_from_estrutura,
    get_message_lang,
    resolve_message_lang_from_unit,
    set_messages,
    set_runtime_message_lang,
)


def test_resolve_message_lang_from_nome_pais():
    assert resolve_message_lang_from_unit(nome_pais='Espanha') == 'ES'
    assert resolve_message_lang_from_unit(nome_pais='España') == 'ES'
    assert resolve_message_lang_from_unit(nome_pais='Portugal') == 'PT'
    assert resolve_message_lang_from_unit(nome_pais='France') == 'EN'
    assert resolve_message_lang_from_unit(nome_pais='França') == 'EN'
    assert resolve_message_lang_from_unit(nome_pais='Ireland') == 'EN'
    assert resolve_message_lang_from_unit(nome_pais='Irlanda') == 'EN'
    assert resolve_message_lang_from_unit(nome_pais='Unknown Country') == 'EN'


def test_apply_unit_message_lang_from_estrutura():
    set_runtime_message_lang(None)
    df = pd.DataFrame([{'fk_pais': 99, 'nome_pais': 'Espanha'}])
    lang = apply_unit_message_lang_from_estrutura(df)
    assert lang == 'ES'
    assert get_message_lang() == 'ES'


def test_set_messages_uses_en_for_france():
    set_runtime_message_lang(None)
    df = pd.DataFrame([{
        'VAR': 'ERR_MAX_CONSECUTIVE_WORKING_DAYS',
        'ES': 'ES template {1}',
        'PT': 'PT template {1}',
        'EN': 'EN template {1}',
    }])
    apply_unit_message_lang_from_estrutura(
        pd.DataFrame([{'fk_pais': 1, 'nome_pais': 'France'}])
    )
    assert set_messages(df, 'ERR_MAX_CONSECUTIVE_WORKING_DAYS', {'1': '1'}) == 'EN template 1'
