"""
Batch persistence of validation events to wfm.esc_processo_erros (STRSOL-1847).

Template rendering uses message_loader + process_message_labels; inserts use
set_process_errors from process_wfm_errors.
"""

from __future__ import annotations

from typing import Callable, List, Optional

import pandas as pd

from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config as get_config_manager
from src.orquestrador_functions.Logs.process_wfm_errors import set_process_errors
from src.orquestrador_functions.Logs.message_loader import get_message_lang, set_messages
from src.orquestrador_functions.Logs.process_message_labels import (
    ProcessMessageLabels,
    feasibility_cap_dayoff_label,
    format_posto_label,
    resolve_employee_labels,
)

logger = get_logger(get_config_manager().system.project_name)


def _employee_id_from_event(event: dict) -> Optional[int]:
    emp_id = event.get('employee_id')
    try:
        return int(emp_id) if emp_id is not None and str(emp_id).strip() != '' else None
    except (TypeError, ValueError):
        return None


def _persist_message_events(
    connection,
    path_os: str,
    fk_process,
    process_type: str,
    df_messages: pd.DataFrame,
    events: List[dict],
    *,
    message_key: str,
    type_error: str,
    log_tag: str,
    build_placeholders: Callable[[dict], dict],
    build_fallback: Callable[[dict], str],
    schedule_day_for: Callable[[dict], Optional[str]],
    user: str = 'WFM',
    posto_id=None,
) -> int:
    if connection is None or not events or df_messages is None or df_messages.empty:
        return 0

    logged = 0
    for event in events:
        description = set_messages(df_messages, message_key, build_placeholders(event))
        if not description:
            description = build_fallback(event)

        logger.info(
            "%s employee_id=%s posto_id=%s | %s",
            log_tag,
            event.get('employee_id'),
            posto_id,
            description,
        )

        ok = set_process_errors(
            connection=connection,
            pathOS=path_os,
            user=user,
            fk_process=fk_process,
            type_error=type_error,
            process_type=process_type,
            error_code=None,
            description=description,
            employee_id=_employee_id_from_event(event),
            schedule_day=schedule_day_for(event),
        )
        if ok:
            logged += 1
        else:
            logger.error(
                "%s: failed to persist esc_processo_erros | %s",
                log_tag,
                description,
            )
    return logged


def log_feasibility_cap_events(
    connection,
    path_os: str,
    fk_process,
    process_type: str,
    df_messages: pd.DataFrame,
    cap_events: List[dict],
    *,
    user: str = 'WFM',
    child_num: str = '1',
    posto_id=None,
    labels: Optional[ProcessMessageLabels] = None,
) -> int:
    """Feasibility cap adjustments — one WARNING row per event."""
    lang = get_message_lang()

    def placeholders(event: dict) -> dict:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        field = str(event.get('field', ''))
        return {
            '1': child_num,
            '2': person_label,
            '3': feasibility_cap_dayoff_label(field, lang),
            '4': str(event.get('original_value', '')),
            '5': str(event.get('cap_value', '')),
            '6': str(event.get('period_begin', '')),
            '7': str(event.get('period_end', '')),
            '8': matricula_label,
        }

    def fallback(event: dict) -> str:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        field = str(event.get('field', ''))
        return (
            f"Subproceso {child_num}: folgas {field} colaborador {person_label}, "
            f"matrícula {matricula_label} ajustadas {event.get('original_value')}->"
            f"{event.get('cap_value')} ({event.get('period_begin')}-{event.get('period_end')})"
        )

    return _persist_message_events(
        connection,
        path_os,
        fk_process,
        process_type,
        df_messages,
        cap_events,
        message_key='WARN_FEASIBILITY_CAP',
        type_error='W',
        log_tag='WARN_FEASIBILITY_CAP',
        build_placeholders=placeholders,
        build_fallback=fallback,
        schedule_day_for=lambda e: str(e.get('period_begin')) if e.get('period_begin') else None,
        user=user,
        posto_id=posto_id,
    )


def log_workload_template_contract_errors(
    connection,
    path_os: str,
    fk_process,
    process_type: str,
    df_messages: pd.DataFrame,
    error_events: List[dict],
    *,
    user: str = 'WFM',
    child_num: str = '1',
    posto_id=None,
    labels: Optional[ProcessMessageLabels] = None,
) -> int:
    """Workload template vs contract mismatches — fatal (E) per event."""
    posto = format_posto_label(posto_id=posto_id, labels=labels)

    def placeholders(event: dict) -> dict:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        return {
            '1': child_num,
            '2': person_label,
            '3': matricula_label,
            '4': str(event.get('workload_template', '')),
            '5': str(event.get('min_dia_trab', '')),
            '6': str(event.get('max_dia_trab', '')),
            '7': str(event.get('week_label', '')),
            '8': str(event.get('period_suffix', '')),
            '9': posto,
        }

    def fallback(event: dict) -> str:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        return (
            f"Sub {child_num}: {person_label} ({matricula_label}) ciclo "
            f"{event.get('workload_template')}d/sem vs contrato "
            f"({event.get('min_dia_trab')}-{event.get('max_dia_trab')}d). "
            f"{event.get('week_label', '')}{event.get('period_suffix', '')}. Posto {posto}"
        )

    return _persist_message_events(
        connection,
        path_os,
        fk_process,
        process_type,
        df_messages,
        error_events,
        message_key='ERR_WORKLOAD_TEMPLATE_CONTRACT',
        type_error='E',
        log_tag='ERR_WORKLOAD_TEMPLATE_CONTRACT',
        build_placeholders=placeholders,
        build_fallback=fallback,
        schedule_day_for=lambda e: str(e.get('period_begin')) if e.get('period_begin') else None,
        user=user,
        posto_id=posto_id,
    )


def log_missing_ciclos_warnings(
    connection,
    path_os: str,
    fk_process,
    process_type: str,
    df_messages: pd.DataFrame,
    warning_events: List[dict],
    *,
    user: str = 'WFM',
    child_num: str = '1',
    posto_id=None,
    labels: Optional[ProcessMessageLabels] = None,
) -> int:
    """Missing cycle configuration — WARNING; process continues."""
    posto = format_posto_label(posto_id=posto_id, labels=labels)

    def placeholders(event: dict) -> dict:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        return {
            '1': child_num,
            '2': person_label,
            '3': matricula_label,
            '6': str(event.get('period_begin', '')),
            '7': str(event.get('period_end', '')),
            '8': posto,
        }

    def fallback(event: dict) -> str:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        return (
            f"Subproceso {child_num}: el colaborador {person_label}, matrícula {matricula_label} "
            f"no tiene ciclo configurado para el periodo "
            f"[{event.get('period_begin')}-{event.get('period_end')}]. "
            f"Puesto {posto}"
        )

    return _persist_message_events(
        connection,
        path_os,
        fk_process,
        process_type,
        df_messages,
        warning_events,
        message_key='WARN_MISSING_CICLOS',
        type_error='W',
        log_tag='WARN_MISSING_CICLOS',
        build_placeholders=placeholders,
        build_fallback=fallback,
        schedule_day_for=lambda e: str(e.get('period_begin')) if e.get('period_begin') else None,
        user=user,
        posto_id=posto_id,
    )


def log_unconverted_fd_days(
    connection,
    path_os: str,
    fk_process,
    process_type: str,
    df_messages: pd.DataFrame,
    fd_events: List[dict],
    *,
    user: str = 'WFM',
    child_num: str = '1',
    posto_id=None,
    labels: Optional[ProcessMessageLabels] = None,
) -> int:
    """F/D rows that missed rule-type conversion — WARNING."""
    posto = format_posto_label(posto_id=posto_id, labels=labels)

    def placeholders(event: dict) -> dict:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        return {
            '1': child_num,
            '2': person_label,
            '3': matricula_label,
            '4': str(event.get('schedule_day', '')),
            '5': posto,
        }

    def fallback(event: dict) -> str:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        return (
            f"Subproceso {child_num}: el colaborador {person_label}, matrícula {matricula_label} "
            f"el dia {event.get('schedule_day', '')} queda F/D. "
            f"No se convirtio al tipo de la regla y el descanso compensatorio "
            f"se inserta sin movimiento. Puesto {posto}"
        )

    return _persist_message_events(
        connection,
        path_os,
        fk_process,
        process_type,
        df_messages,
        fd_events,
        message_key='WARN_UNCONVERTED_FD',
        type_error='W',
        log_tag='WARN_UNCONVERTED_FD',
        build_placeholders=placeholders,
        build_fallback=fallback,
        schedule_day_for=lambda e: str(e.get('schedule_day')) if e.get('schedule_day') else None,
        user=user,
        posto_id=posto_id,
    )


def log_max_consecutive_working_days_errors(
    connection,
    path_os: str,
    fk_process,
    process_type: str,
    df_messages: pd.DataFrame,
    error_events: List[dict],
    *,
    user: str = 'WFM',
    child_num: str = '1',
    posto_id=None,
    labels: Optional[ProcessMessageLabels] = None,
) -> int:
    """Max consecutive working days pre-check failures — fatal (E) per event."""
    posto = format_posto_label(posto_id=posto_id, labels=labels)

    def placeholders(event: dict) -> dict:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        return {
            '1': child_num,
            '2': person_label,
            '3': matricula_label,
            '4': str(event.get('num_dias_cons', '')),
            '5': str(event.get('streak_days', '')),
            '6': str(event.get('period_begin', '')),
            '7': str(event.get('period_end', '')),
            '9': posto,
        }

    def fallback(event: dict) -> str:
        person_label, matricula_label = resolve_employee_labels(event, labels)
        return (
            f"Sub {child_num}: {person_label} ({matricula_label}) excede "
            f"{event.get('num_dias_cons')} dias trab. seguidos ({event.get('streak_days')}). "
            f"[{event.get('period_begin')}-{event.get('period_end')}]. Posto {posto}"
        )

    return _persist_message_events(
        connection,
        path_os,
        fk_process,
        process_type,
        df_messages,
        error_events,
        message_key='ERR_MAX_CONSECUTIVE_WORKING_DAYS',
        type_error='E',
        log_tag='ERR_MAX_CONSECUTIVE_WORKING_DAYS',
        build_placeholders=placeholders,
        build_fallback=fallback,
        schedule_day_for=lambda e: str(e.get('period_begin')) if e.get('period_begin') else None,
        user=user,
        posto_id=posto_id,
    )
