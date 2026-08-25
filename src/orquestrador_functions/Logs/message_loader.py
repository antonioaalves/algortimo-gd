# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:06:29 2024

@author: jason.vogensen
"""

import os
import unicodedata

import pandas as pd

# Per-process override set when unit country is loaded (see apply_unit_message_lang_from_estrutura).
_runtime_message_lang: str | None = None

# Optional WFM core_country.id → message column; nome_pais is used when an id is not listed.
_FK_PAIS_MESSAGE_LANG: dict[int, str] = {}

_ES_COUNTRY_TOKENS = frozenset({'espana', 'spain', 'espanha'})
_PT_COUNTRY_TOKENS = frozenset({'portugal'})
# Until FR translations exist, France and Ireland use the EN column in df_messages.
_EN_COUNTRY_TOKENS = frozenset({'france', 'franca', 'irlanda', 'ireland', 'irlande'})

_SUPPORTED_MESSAGE_LANGS = ('EN', 'ES', 'PT')
_DEFAULT_MESSAGE_LANG = 'EN'


def _get_logging_config() -> dict:
    try:
        from src.configuration_manager.instance import get_config as get_config_manager
        cfg = get_config_manager()
        return getattr(cfg.system, 'logging_config', {}) or {}
    except Exception:
        return {}


def _normalize_country_name(name: str) -> str:
    normalized = unicodedata.normalize('NFKD', str(name))
    ascii_name = normalized.encode('ascii', 'ignore').decode('ascii')
    return ascii_name.strip().lower()


def resolve_message_lang_from_unit(
    fk_pais=None,
    nome_pais=None,
    default: str | None = None,
) -> str:
    """
    Map unit country (fk_pais / nome_pais) to a df_messages language column (EN, ES, or PT).

    Falls back to logging.message_lang from system settings, then EN.
    """
    if default is None:
        default = _get_logging_config().get('message_lang', _DEFAULT_MESSAGE_LANG) or _DEFAULT_MESSAGE_LANG
    fallback = str(default).upper()

    if fk_pais is not None and str(fk_pais).strip() != '':
        try:
            mapped = _FK_PAIS_MESSAGE_LANG.get(int(fk_pais))
            if mapped:
                return str(mapped).upper()
        except (TypeError, ValueError):
            pass

    if nome_pais is not None and str(nome_pais).strip():
        norm = _normalize_country_name(nome_pais)
        if norm in _ES_COUNTRY_TOKENS or any(token in norm for token in _ES_COUNTRY_TOKENS):
            return 'ES'
        if norm in _PT_COUNTRY_TOKENS or 'portugal' in norm:
            return 'PT'
        if norm in _EN_COUNTRY_TOKENS or any(
            token in norm for token in ('france', 'franca', 'irland', 'ireland')
        ):
            return 'EN'

    return fallback


def set_runtime_message_lang(lang: str | None) -> None:
    """Set or clear the per-process message language override."""
    global _runtime_message_lang
    _runtime_message_lang = str(lang).upper() if lang else None


def apply_unit_message_lang_from_estrutura(df_estrutura_wfm: pd.DataFrame) -> str:
    """Resolve language from df_estrutura_wfm and apply it for subsequent set_messages calls."""
    fk_pais = nome_pais = None
    if df_estrutura_wfm is not None and not df_estrutura_wfm.empty:
        row = df_estrutura_wfm.iloc[0]
        if 'fk_pais' in df_estrutura_wfm.columns:
            fk_pais = row.get('fk_pais')
        if 'nome_pais' in df_estrutura_wfm.columns:
            nome_pais = row.get('nome_pais')
    lang = resolve_message_lang_from_unit(fk_pais=fk_pais, nome_pais=nome_pais)
    set_runtime_message_lang(lang)
    return lang


def get_message_lang(default: str = _DEFAULT_MESSAGE_LANG) -> str:
    """Resolve message language: unit override, then system settings, then default."""
    if _runtime_message_lang:
        return _runtime_message_lang
    lang = _get_logging_config().get('message_lang', default)
    return str(lang).upper() if lang else default


def get_df_messages_path(project_root_dir: str | None = None) -> str:
    """
    Resolve df_messages CSV path from sql_filepaths.json (sql_processing_paths.df_messages).
    """
    try:
        from src.configuration_manager.instance import get_config as get_config_manager
        cfg = get_config_manager()
        path = cfg.paths.sql_processing_paths.get('df_messages', '')
        if path:
            return path
    except Exception:
        pass
    root = project_root_dir or ''
    return os.path.join(root, 'data', 'df_messages.csv') if root else 'data/df_messages.csv'


def get_df_messages_candidate_paths(project_root_dir: str | None = None) -> list[str]:
    """
    Ordered df_messages.csv lookup paths: explicit per-client override first,
    bundled default last. The default is always shipped in the image, so error
    logging never breaks just because a client did not provide a custom file.

    1. DF_MESSAGES_PATH env var -> client mounts a custom file anywhere and
       points to it. Falls through to the default if missing/empty/unreadable.
    2. configured path          -> bundled default (data/df_messages.csv),
       resolved from sql_processing_paths so it still honours config overrides.
    """
    candidates: list[str] = []
    env_path = os.environ.get('DF_MESSAGES_PATH', '').strip()
    if env_path:
        candidates.append(env_path)
    candidates.append(get_df_messages_path(project_root_dir))

    seen: set[str] = set()
    ordered: list[str] = []
    for path in candidates:
        if path and path not in seen:
            seen.add(path)
            ordered.append(path)
    return ordered


def load_df_messages(project_root_dir: str | None = None) -> pd.DataFrame:
    """Load df_messages, trying client overrides first then the bundled default.

    Falls through to the next candidate when a file is missing, unreadable or
    empty, so a broken/absent override never silently disables process-error
    logging (which is gated on df_messages being non-empty).
    """
    for path in get_df_messages_candidate_paths(project_root_dir):
        if not os.path.exists(path):
            continue
        try:
            df_msg = pd.read_csv(path, sep=',', encoding='utf-8')
        except Exception as e:
            print(f"Error loading df_messages from {path}: {e}")
            continue
        if df_msg.empty:
            print(f"df_messages at {path} is empty, trying next candidate")
            continue
        first_col = str(df_msg.columns[0])
        if first_col != 'VAR':
            df_msg.rename(columns={first_col: 'VAR'}, inplace=True)
        print(f"Loaded df_messages from {path} ({len(df_msg)} rows)")
        return df_msg
    print("Error loading df_messages: no usable file found in any candidate path")
    return pd.DataFrame()


def _resolve_message_template(message_row: pd.Series, df_msg: pd.DataFrame, lang: str) -> str:
    """Pick template for lang with fallback to EN, ES, then PT when translation is missing."""
    fallback_order = [lang.upper()]
    for fallback in _SUPPORTED_MESSAGE_LANGS:
        if fallback not in fallback_order:
            fallback_order.append(fallback)
    for col in fallback_order:
        if col not in df_msg.columns:
            continue
        value = message_row.get(col)
        if pd.notna(value) and str(value).strip():
            return str(value)
    return ''


def get_messages(path_os, lang=_DEFAULT_MESSAGE_LANG):
    """
    Reads df_messages and filters to the requested language column.

    Parameters:
        path_os (str): Project root directory (used when settings path is relative).
        lang (str): Language code ('EN', 'ES', or 'PT'). Default is EN.

    Returns:
        pd.DataFrame: DataFrame with VAR and DESC columns, or empty on error.
    """
    try:
        df_msg = load_df_messages(path_os)
        if df_msg.empty:
            return pd.DataFrame()

        lang = str(lang).upper()
        if lang not in df_msg.columns:
            lang = _DEFAULT_MESSAGE_LANG if _DEFAULT_MESSAGE_LANG in df_msg.columns else 'ES'

        melted = df_msg.melt(id_vars=['VAR'], value_vars=[lang], var_name='LANG', value_name='DESC')
        return melted[melted['LANG'] == lang]
    except Exception as e:
        print("Error:", str(e))
        return pd.DataFrame()


def replace_placeholders(template, values):
    """
    Replaces placeholders in the template string with corresponding values from the values dictionary.

    Parameters:
        template (str): The template string with placeholders.
        values (dict): A dictionary with keys corresponding to placeholder names and values as replacements.

    Returns:
        str: The template string with placeholders replaced by values.
    """
    for name, value in values.items():
        placeholder = f"{{{name}}}"
        template = template.replace(placeholder, str(value))
    return template


def set_messages(df_msg, var, values, lang=None):
    """
    Retrieves a message template from DataFrame, replaces placeholders, and returns the formatted message.

    Parameters:
        df_msg (pd.DataFrame): DataFrame containing message templates (columns VAR, ES, PT, ...).
        var (str): The variable name to filter the message by.
        values (dict): Dictionary of placeholders and their corresponding values.
        lang (str, optional): Language code ('EN', 'ES', or 'PT'). Defaults to system message_lang.

    Returns:
        str: The formatted message with placeholders replaced.
    """
    if df_msg is None or df_msg.empty or 'VAR' not in df_msg.columns:
        return ""

    if lang is None:
        lang = get_message_lang()

    message_row = df_msg[df_msg['VAR'] == var]
    if message_row.empty:
        return ""

    template = _resolve_message_template(message_row.iloc[0], df_msg, lang)
    if template:
        return replace_placeholders(template, values)

    if 'DESC' in df_msg.columns:
        desc = message_row.iloc[0].get('DESC')
        if pd.notna(desc) and str(desc).strip():
            return replace_placeholders(str(desc), values)
    return ""
