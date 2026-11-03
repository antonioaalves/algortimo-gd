"""Validations over the dataframe that insert_results will write."""

from typing import Dict, List, Optional

import pandas as pd
from base_data_project.log_config import get_logger

from src.configuration_manager.instance import get_config

logger = get_logger(get_config().project_name)


def _matricula_key(value) -> str:
    """Normalize a matricula so map values and df_final strings compare equal."""
    text = str(value).strip()
    if text.lower() in ('', 'none', 'nan', '<na>'):
        return ''
    if text.endswith('.0'):
        text = text[:-2]
    return text.lstrip('0') or '0'


def _resolve_employee_id(
    matricula: str,
    employee_id_matriculas_map: Optional[Dict],
) -> Optional[int]:
    """Return the WFM employee id for a matricula, or None when it does not match."""
    if not employee_id_matriculas_map or not matricula:
        return None

    target = _matricula_key(matricula)
    if not target:
        return None

    matches: List[int] = []
    for emp_id, mapped_matricula in employee_id_matriculas_map.items():
        if _matricula_key(mapped_matricula) != target:
            continue
        try:
            matches.append(int(emp_id))
        except (TypeError, ValueError):
            continue

    if not matches:
        return None
    if len(matches) > 1:
        logger.warning(
            "Matricula %s matches employee ids %s; using %s",
            matricula,
            matches,
            matches[-1],
        )
    return matches[-1]


def collect_unconverted_fd_day_events(
    final_df: pd.DataFrame,
    employee_id_matriculas_map: Optional[Dict] = None,
) -> List[dict]:
    """
    Employee-days still F/D on the frame insert_results will write.

    convert_types_out maps every solver LD to F/D. apply_compensatory_sched_types
    should then replace that pair with REST_DAY_TYPE / REST_DAY_SUBTYPE from the
    rule. A row that is still F/D was not converted, so the compensatory day off
    will be inserted with no movement. This does not modify final_df.
    """
    if final_df is None or final_df.empty:
        return []
    if 'sched_type' not in final_df.columns or 'sched_subtype' not in final_df.columns:
        logger.warning(
            "collect_unconverted_fd_day_events: sched_type/sched_subtype missing on final_df"
        )
        return []

    date_col = 'data' if 'data' in final_df.columns else 'date' if 'date' in final_df.columns else None
    if date_col is None:
        logger.warning("collect_unconverted_fd_day_events: no data/date column on final_df")
        return []

    id_col = 'colaborador' if 'colaborador' in final_df.columns else 'matricula' if 'matricula' in final_df.columns else None
    if id_col is None:
        logger.warning("collect_unconverted_fd_day_events: no colaborador/matricula column on final_df")
        return []

    mask = (
        final_df['sched_type'].astype(str).str.strip().str.upper().eq('F')
        & final_df['sched_subtype'].astype(str).str.strip().str.upper().eq('D')
    )
    matched = final_df.loc[mask, [id_col, date_col]]

    events: List[dict] = []
    for _, row in matched.iterrows():
        matricula = str(row[id_col]).strip() if pd.notna(row[id_col]) else ''
        if matricula.lower() in ('', 'none', 'nan'):
            matricula = ''
        if matricula.endswith('.0'):
            matricula = matricula[:-2]

        schedule_ts = pd.to_datetime(row[date_col], errors='coerce')
        if pd.isna(schedule_ts):
            logger.warning(
                "collect_unconverted_fd_day_events: skipping F/D row with invalid date for matricula %s",
                matricula or 'n/a',
            )
            continue

        events.append({
            'employee_id': _resolve_employee_id(matricula, employee_id_matriculas_map),
            'matricula': matricula,
            'schedule_day': schedule_ts.strftime('%Y-%m-%d'),
        })

    events.sort(key=lambda event: (event['matricula'], event['schedule_day']))
    return events
