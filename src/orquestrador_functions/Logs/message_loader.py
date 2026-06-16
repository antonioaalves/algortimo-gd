# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:06:29 2024

@author: jason.vogensen
"""

import os

import pandas as pd


def _get_logging_config() -> dict:
    try:
        from src.configuration_manager.instance import get_config as get_config_manager
        cfg = get_config_manager()
        return getattr(cfg.system, 'logging_config', {}) or {}
    except Exception:
        return {}


def get_message_lang(default: str = 'ES') -> str:
    """Resolve message language from system settings (ES or PT)."""
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
    return os.path.join(root, 'data', 'csvs', 'df_messages.csv') if root else 'data/csvs/df_messages.csv'


def load_df_messages(project_root_dir: str | None = None) -> pd.DataFrame:
    """Load df_messages from the path configured in system settings."""
    path = get_df_messages_path(project_root_dir)
    try:
        df_msg = pd.read_csv(path, sep=',', encoding='utf-8')
        first_col = str(df_msg.columns[0])
        if first_col != 'VAR':
            df_msg.rename(columns={first_col: 'VAR'}, inplace=True)
        return df_msg
    except Exception as e:
        print(f"Error loading df_messages from {path}: {e}")
        return pd.DataFrame()


def _resolve_message_template(message_row: pd.Series, df_msg: pd.DataFrame, lang: str) -> str:
    """Pick template for lang with fallback to ES then PT when translation is missing."""
    fallback_order = [lang.upper()]
    for fallback in ('ES', 'PT'):
        if fallback not in fallback_order:
            fallback_order.append(fallback)
    for col in fallback_order:
        if col not in df_msg.columns:
            continue
        value = message_row.get(col)
        if pd.notna(value) and str(value).strip():
            return str(value)
    return ''


def get_messages(path_os, lang='ES'):
    """
    Reads df_messages and filters to the requested language column.

    Parameters:
        path_os (str): Project root directory (used when settings path is relative).
        lang (str): Language code ('ES' or 'PT'). Default is 'ES'.

    Returns:
        pd.DataFrame: DataFrame with VAR and DESC columns, or empty on error.
    """
    try:
        df_msg = load_df_messages(path_os)
        if df_msg.empty:
            return pd.DataFrame()

        lang = str(lang).upper()
        if lang not in df_msg.columns:
            lang = 'ES'

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
        lang (str, optional): Language code ('ES' or 'PT'). Defaults to system message_lang.

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
