"""
User-facing placeholder values for wfm.esc_processo_erros descriptions (STRSOL-1834).

Built once per process from df_estrutura_wfm and in-memory employee frames, stored on
auxiliary_data[PROCESS_MESSAGE_LABELS_KEY].
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config as get_config_manager

PROCESS_MESSAGE_LABELS_KEY = 'process_message_labels'

logger = get_logger(get_config_manager().system.project_name)


def _label_is_blank(value) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text == '' or text.lower() in ('none', 'nan', '<na>')


def _as_id_text(value) -> str:
    if _label_is_blank(value):
        return ''
    text = str(value).strip()
    if text.endswith('.0'):
        text = text[:-2]
    return text


def _scalar_column_text(df: pd.DataFrame, column: str) -> str:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or column not in df.columns:
        return ''
    for value in df[column].dropna():
        if not _label_is_blank(value):
            return str(value).strip()
    return ''


def _build_posto_names(df_estrutura_wfm: pd.DataFrame) -> Dict[str, str]:
    names: Dict[str, str] = {}
    if df_estrutura_wfm is None or not isinstance(df_estrutura_wfm, pd.DataFrame) or df_estrutura_wfm.empty:
        return names
    if 'fk_tipo_posto' not in df_estrutura_wfm.columns or 'nome_tipo_posto' not in df_estrutura_wfm.columns:
        return names
    for fk, post_name in zip(df_estrutura_wfm['fk_tipo_posto'], df_estrutura_wfm['nome_tipo_posto']):
        key = _as_id_text(fk)
        if not key or key in names or _label_is_blank(post_name):
            continue
        names[key] = str(post_name).strip()
    return names


def _merge_employee_frame(employees: Dict[str, Tuple[str, str]], df: pd.DataFrame) -> None:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return
    frame = df.copy()
    frame.columns = frame.columns.str.lower()
    if 'employee_id' not in frame.columns:
        return
    if 'nome' not in frame.columns:
        frame['nome'] = ''
    if 'matricula' not in frame.columns:
        frame['matricula'] = ''
    frame['_employee_key'] = frame['employee_id'].map(_as_id_text)
    frame = frame[frame['_employee_key'] != '']
    if frame.empty:
        return
    for key, grp in frame.groupby('_employee_key', sort=False):
        row = grp.iloc[0]
        nome = '' if _label_is_blank(row['nome']) else str(row['nome']).strip()
        matricula = _as_id_text(row['matricula'])
        prev_nome, prev_matricula = employees.get(key, ('', ''))
        employees[key] = (nome or prev_nome, matricula or prev_matricula)


@dataclass(frozen=True)
class ProcessMessageLabels:
    posto_names: Dict[str, str]
    employees: Dict[str, Tuple[str, str]]
    section_name: str
    unit_name: str

    def raw_employee(self, employee_id) -> Tuple[str, str]:
        key = _as_id_text(employee_id)
        if not key:
            return '', ''
        return self.employees.get(key, ('', ''))


def build_process_message_labels(
    df_estrutura_wfm=None,
    employee_frames: Optional[Iterable[pd.DataFrame]] = None,
) -> ProcessMessageLabels:
    df = df_estrutura_wfm if isinstance(df_estrutura_wfm, pd.DataFrame) else pd.DataFrame()
    employees: Dict[str, Tuple[str, str]] = {}
    for frame in employee_frames or ():
        _merge_employee_frame(employees, frame)
    return ProcessMessageLabels(
        posto_names=_build_posto_names(df),
        employees=employees,
        section_name=_scalar_column_text(df, 'nome_secao'),
        unit_name=_scalar_column_text(df, 'nome_unidade'),
    )


def collect_employee_frames(
    auxiliary_data: Optional[dict] = None,
    *,
    raw_data: Optional[dict] = None,
    medium_data: Optional[dict] = None,
    extra_frames: Optional[Iterable[pd.DataFrame]] = None,
) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []
    for store in (raw_data, medium_data, auxiliary_data):
        if not isinstance(store, dict):
            continue
        for key in ('df_colaborador', 'df_valid_emp'):
            df = store.get(key)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
    if extra_frames:
        for df in extra_frames:
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
    return frames


def refresh_process_message_labels(
    auxiliary_data: dict,
    *,
    raw_data: Optional[dict] = None,
    medium_data: Optional[dict] = None,
    extra_frames: Optional[Iterable[pd.DataFrame]] = None,
) -> ProcessMessageLabels:
    df_estrutura = auxiliary_data.get('df_estrutura_wfm', pd.DataFrame())
    if not isinstance(df_estrutura, pd.DataFrame):
        df_estrutura = pd.DataFrame()
    frames = collect_employee_frames(
        auxiliary_data,
        raw_data=raw_data,
        medium_data=medium_data,
        extra_frames=extra_frames,
    )
    labels = build_process_message_labels(df_estrutura, frames)
    auxiliary_data[PROCESS_MESSAGE_LABELS_KEY] = labels
    return labels


def get_process_message_labels(auxiliary_data: Optional[dict]) -> Optional[ProcessMessageLabels]:
    if not isinstance(auxiliary_data, dict):
        return None
    labels = auxiliary_data.get(PROCESS_MESSAGE_LABELS_KEY)
    return labels if isinstance(labels, ProcessMessageLabels) else None


def format_posto_label(
    df_estrutura_wfm=None,
    posto_id=None,
    *,
    labels: Optional[ProcessMessageLabels] = None,
) -> str:
    if labels is None:
        df = df_estrutura_wfm if isinstance(df_estrutura_wfm, pd.DataFrame) else pd.DataFrame()
        labels = build_process_message_labels(df)
    name = labels.posto_names.get(_as_id_text(posto_id), '')
    if name:
        return name
    posto_text = _as_id_text(posto_id)
    if posto_text:
        logger.warning("Post name missing for posto_id=%s; using the id", posto_text)
        return posto_text
    logger.warning("Post name missing and posto_id is empty")
    return ''


def format_section_label(
    df_estrutura_wfm=None,
    secao_id=None,
    *,
    labels: Optional[ProcessMessageLabels] = None,
) -> str:
    if labels is None:
        df = df_estrutura_wfm if isinstance(df_estrutura_wfm, pd.DataFrame) else pd.DataFrame()
        labels = build_process_message_labels(df)
    if labels.section_name:
        return labels.section_name
    secao_text = _as_id_text(secao_id)
    if secao_text:
        logger.warning("Section name missing for secao_id=%s; using the id", secao_text)
        return secao_text
    logger.warning("Section name missing and secao_id is empty")
    return ''


def format_unit_label(
    df_estrutura_wfm=None,
    unit_id=None,
    *,
    labels: Optional[ProcessMessageLabels] = None,
) -> str:
    if labels is None:
        df = df_estrutura_wfm if isinstance(df_estrutura_wfm, pd.DataFrame) else pd.DataFrame()
        labels = build_process_message_labels(df)
    if labels.unit_name:
        return labels.unit_name
    unit_text = _as_id_text(unit_id)
    if unit_text:
        logger.warning("Store name missing for unit_id=%s; using the id", unit_text)
        return unit_text
    logger.warning("Store name missing and unit_id is empty")
    return ''


def format_employee_label(nome, matricula, employee_id) -> str:
    if not _label_is_blank(nome):
        return str(nome).strip()
    matricula_text = _as_id_text(matricula)
    employee_text = _as_id_text(employee_id)
    if matricula_text:
        logger.warning(
            "Employee name missing for employee_id=%s; using matricula %s",
            employee_text or 'n/d',
            matricula_text,
        )
        return matricula_text
    if employee_text:
        logger.warning(
            "Employee name and matricula missing for employee_id=%s; using the id",
            employee_text,
        )
        return employee_text
    logger.warning("Employee name, matricula, and id are missing")
    return ''


def format_matricula_label(matricula) -> str:
    matricula_text = _as_id_text(matricula)
    return matricula_text if matricula_text else 'n/d'


def resolve_employee_labels(
    event: dict,
    labels: Optional[ProcessMessageLabels] = None,
) -> Tuple[str, str]:
    nome = event.get('nome', '')
    matricula = event.get('matricula', '')
    employee_id = event.get('employee_id', '')
    if labels is not None and (_label_is_blank(nome) or _label_is_blank(matricula)):
        looked_nome, looked_matricula = labels.raw_employee(employee_id)
        if _label_is_blank(nome):
            nome = looked_nome
        if _label_is_blank(matricula):
            matricula = looked_matricula
    return (
        format_employee_label(nome, matricula, employee_id),
        format_matricula_label(matricula),
    )


def posto_label(auxiliary_data: dict, posto_id) -> str:
    """Post name for set_messages, using cached auxiliary_data labels."""
    labels = get_process_message_labels(auxiliary_data)
    if labels is not None:
        return format_posto_label(posto_id=posto_id, labels=labels)
    df = auxiliary_data.get('df_estrutura_wfm', pd.DataFrame()) if isinstance(auxiliary_data, dict) else pd.DataFrame()
    return format_posto_label(df, posto_id)


def section_label(auxiliary_data: dict, secao_id) -> str:
    labels = get_process_message_labels(auxiliary_data)
    if labels is not None:
        return format_section_label(secao_id=secao_id, labels=labels)
    df = auxiliary_data.get('df_estrutura_wfm', pd.DataFrame()) if isinstance(auxiliary_data, dict) else pd.DataFrame()
    return format_section_label(df, secao_id)


def template_has_placeholder(df_messages: pd.DataFrame, message_key: str, slot: str) -> bool:
    if df_messages is None or df_messages.empty or not message_key:
        return False
    rows = df_messages[df_messages['VAR'] == message_key]
    if rows.empty:
        return False
    token = '{' + slot + '}'
    for column in ('ES', 'PT', 'EN'):
        if column not in rows.columns:
            continue
        if token in str(rows[column].iloc[0]):
            return True
    return False


def load_stage_placeholders(
    error_code,
    child_num,
    detail,
    df_messages: pd.DataFrame,
    *,
    log: Optional[logging.Logger] = None,
) -> dict:
    code = '' if error_code is None else str(error_code)
    placeholders = {'1': child_num}
    if detail is None or detail == '':
        return placeholders
    if template_has_placeholder(df_messages, code, '2'):
        placeholders['2'] = str(detail)
    elif log is not None:
        log.error(
            "Process message %s detail kept in the log only: %s",
            code,
            detail,
        )
    return placeholders
