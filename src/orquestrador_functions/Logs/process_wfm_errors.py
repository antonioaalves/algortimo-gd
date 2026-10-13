"""Insert rows into wfm.esc_processo_erros (STRSOL-1847 phase 2)."""

from __future__ import annotations

import os

from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config as get_config_manager
from src.orquestrador_functions.Classes.Connection.connect import ensure_connection_with_config

logger = get_logger(get_config_manager().system.project_name)


def set_process_errors(
    connection,
    pathOS,
    user,
    fk_process,
    type_error,
    process_type,
    error_code,
    description,
    employee_id,
    schedule_day,
):
    """
    Insert process error details into the database.

    Returns:
        1 if successful, 0 otherwise.
    """
    try:
        if connection is None:
            logger.error("set_process_errors: no database connection")
            return 0

        if hasattr(connection, 'ping') and callable(getattr(connection, 'ping')):
            connection = ensure_connection_with_config(connection)

        query_file_path = os.path.join(
            pathOS, 'data', 'Queries', 'WFM_Process', 'Setters', 'set_process_errors.sql'
        )
        with open(query_file_path, 'r') as f:
            query = f.read().strip().replace('\n', ' ')

        if hasattr(connection, 'dbapi_connection'):
            if connection.dbapi_connection is not None:
                cursor_context = connection.dbapi_connection.cursor()
            else:
                logger.error("set_process_errors: SQLAlchemy dbapi_connection is None")
                return 0
        elif hasattr(connection, 'cursor') and callable(getattr(connection, 'cursor')):
            cursor_context = connection.cursor()
        else:
            logger.error("set_process_errors: unknown connection type %s", type(connection))
            return 0

        params = {
            'i_user': user,
            'i_fk_process': fk_process,
            'i_type_error': type_error,
            'i_process_type': process_type,
            'i_error_code': error_code if error_code is not None else None,
            'i_description': description,
            'i_employee_id': employee_id if employee_id is not None else None,
            'i_schedule_day': schedule_day if schedule_day is not None else None,
        }

        with cursor_context as cursor:
            cursor.execute(query, params)
            if hasattr(connection, 'dbapi_connection') and connection.dbapi_connection is not None:
                connection.dbapi_connection.commit()
            elif hasattr(connection, 'commit') and callable(getattr(connection, 'commit')):
                connection.commit()

        logger.debug(
            "set_process_errors ok fk_process=%s type=%s code=%s employee_id=%s",
            fk_process,
            type_error,
            error_code,
            employee_id,
        )
        return 1

    except Exception as e:
        logger.error("Error in set_process_errors: %s", e, exc_info=True)
        return 0
