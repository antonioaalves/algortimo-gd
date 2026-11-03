# -*- coding: utf-8 -*-
"""WFM process status updates (SQL files under data/Queries/WFM_Process/Setters/)."""

import os

from src.orquestrador_functions.Classes.Connection.connect import ensure_connection_with_config


def set_process_param_status(connection, pathOS, user, process_id, new_status):
    """
    Updates the status of a process parameter in the database.

    Returns:
        int: 1 if successful, 0 otherwise.
    """
    connection = ensure_connection_with_config(connection)
    query_file_path = os.path.join(
        pathOS, 'data', 'Queries', 'WFM_Process', 'Setters', 'set_process_parameter_status.sql'
    )
    try:
        with open(query_file_path, 'r') as f:
            query = f.read().strip().replace('\n', ' ')

        with connection.cursor() as cursor:
            params = {
                'i_user_input': user,
                'i_process_id_input': process_id,
                'i_new_status_input': new_status,
            }
            cursor.execute(query, params)

        connection.commit()
        return 1
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return 0


def set_process_status(connection, pathOS, user, process_id, status='P'):
    """
    Updates the status of a process in the database.

    Returns:
        int: 1 if successful, 0 otherwise.
    """
    connection = ensure_connection_with_config(connection)
    query_file_path = os.path.join(
        pathOS, 'data', 'Queries', 'WFM_Process', 'Setters', 'set_process_status.sql'
    )

    try:
        with open(query_file_path, 'r') as f:
            query = f.read().strip().replace('\n', ' ')

        with connection.cursor() as cursor:
            params = {
                'i_user_input': user,
                'i_process_id_input': process_id,
                'i_new_status_input': status,
            }
            cursor.execute(query, params)

        connection.commit()
        return 1
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return 0
