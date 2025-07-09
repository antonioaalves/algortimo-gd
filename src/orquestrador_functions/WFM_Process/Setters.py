# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 10:05:58 2024


THIS IS THE SETTERS FOR THE FUNCTIONS REGARDING THE WFM PROCESSES AND CONTROL OF STATUS



@author: jason.vogensen
"""
import os
from Connection.connect import ensure_connection

def set_process_errors(connection, pathOS, user, fk_process, type_error, process_type, error_code, description, employee_id, schedule_day):
    """
    Inserts process error details into the database.

    Args:
        connection: Active database connection.
        pathOS (str): Base path for configurations and query files.
        user (str): Username for the operation.
        fk_process (int): Foreign key for the process.
        type_error (str): Type of error.
        process_type (str): Type of process.
        error_code (int): Error code.
        description (str): Description of the error.
        employee_id (int): ID of the employee.
        schedule_day (str): Scheduled day for the error (formatted as 'yyyy-mm-dd').
    Returns:
        int: 1 if successful, 0 otherwise.
    """
    connection = ensure_connection(connection, os.path.join(pathOS, "Connection"))
    query_file_path = os.path.join(pathOS, 'Data', 'Queries', 'WFM_Process', 'Setters', 'set_process_errors.sql')
    print(fk_process)
    try:
        # Load the query from file
        with open(query_file_path, 'r') as f:
            query = f.read().strip().replace("\n", " ")
        # Execute the query
        with connection.cursor() as cursor:
            params = {
               'i_user': user,
               'i_fk_process': fk_process,
               'i_type_error': type_error,
               'i_process_type': process_type,
               'i_error_code': error_code if error_code is not None else None,
               'i_description': description,
               'i_employee_id': employee_id if employee_id is not None else None,
               'i_schedule_day': schedule_day if schedule_day is not None else None
           }
            cursor.execute(query, params)
            print('inserted')
        return 1
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return 0
    
def set_process_param_status(connection, pathOS, user, process_id, new_status):
    """
    Updates the status of a process parameter in the database.

    Args:
        connection: Active database connection.
        pathOS (str): Base path for configurations and query files.
        user (str): Username for the operation.
        process_id (int): ID of the process.
        new_status (str): New status to set for the process.

    Returns:
        int: 1 if successful, 0 otherwise.
    """
    connection = ensure_connection(connection, os.path.join(pathOS, "Connection"))
    query_file_path = os.path.join(pathOS, 'Data', 'Queries', 'WFM_Process', 'Setters', 'set_process_parameter_status.sql')
    try:
        # Load the query from file
        with open(query_file_path, 'r') as f:
            query = f.read().strip().replace("\n", " ")
        
        # Execute the query
        with connection.cursor() as cursor:
            params = {
                'i_user_input': user,
                'i_process_id_input': process_id,
                'i_new_status_input': new_status
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

    Args:
        connection: Active database connection.
        pathOS (str): Base path for configurations and query files.
        user (str): Username for the operation.
        process_id (int): ID of the process.
        status (str): New status to set for the process (default is 'P').

    Returns:
        int: 1 if successful, 0 otherwise.
    """
    connection = ensure_connection(connection, os.path.join(pathOS, "Connection"))
    query_file_path = os.path.join(pathOS, 'Data', 'Queries', 'WFM_Process', 'Setters', 'set_process_status.sql')
    
    try:
        # Load the query from file
        with open(query_file_path, 'r') as f:
            query = f.read().strip().replace("\n", " ")
         
        # Execute the query
        with connection.cursor() as cursor:
            params = {
                'i_user_input': user,
                'i_process_id_input': process_id,
                'i_new_status_input': status
            }
            cursor.execute(query, params)
        
        connection.commit()
        return 1
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return 0

