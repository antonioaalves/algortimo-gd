# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 12:07:14 2024


THIS IS THE GETTERS FOR THE FUNCTIONS REGARDING THE WFM PROCESSES AND CONTROL OF STATUS


@author: jason.vogensen
"""

from src.orquestrador_functions.Classes.Connection.connect import ensure_connection_with_config
import os
import pandas as pd
import cx_Oracle
def get_process_valid_emp(pathOS, process_id,connection):
    """
    Retrieves valid employees for a given process ID from the database.
    
    Args:
        pathOS (str): The base path for configurations and query files.
        process_id (int): The ID of the process to fetch valid employees for.
    
    Returns:
        pd.DataFrame: A dataframe containing valid employees for the given process ID.
    """
    
    # Load database connection details
    try:
        # Ensure connection is active using config_manager
        connection = ensure_connection_with_config(connection)
        
        # Read SQL query from file
        query_file_path = os.path.join(pathOS, 'Data', 'Queries', 'WFM_Process', 'Getters', "get_process_valid_employess.sql")
        with open(query_file_path, 'r') as file:
            query = file.read().strip().replace("\n", " ")
        
        # Replace the placeholder in the query with the process_id
        query = query.replace(":process_id", str(process_id))
        
        # Execute the query and fetch results into a DataFrame
        try:
            # Execute the query using a cursor
           with connection.cursor() as cursor:
               cursor.execute(query)
               
               # Fetch all results (or fetch one by one as needed)
               rows = cursor.fetchall()
    
               # Fetch column names
               columns = [col[0] for col in cursor.description]
    
               # Convert the results into a Pandas DataFrame
               df_data = pd.DataFrame(rows, columns=columns) 
        except cx_Oracle.DatabaseError as e:
            print(f"Error executing query: {e}")
            df_data = pd.DataFrame()  # Return an empty DataFrame on error
        
        
        return df_data
    
    except Exception as e:
        print(f"Error in connection or execution: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if connection fails

def get_process_by_status(pathOS, user, process_type, event_type, status, connection, use_case: int = 0):
    """
    Retrieves processes by status from the database.
    
    Args:
        pathOS (str): The base path for configurations and query files.
        user (str): The user parameter for the query.
        process_type (str): The process type parameter for the query.
        event_type (str): The event type parameter for the query.
        status (str): The status parameter for the query.
        connection: The database connection object.
    
    Returns:
        pd.DataFrame: A dataframe containing processes by status.
    """
    try:
        if use_case == 0:
            connection = ensure_connection_with_config(connection)

            # Read SQL query from file
            query_file_path = os.path.join(pathOS, "Data", "Queries", "WFM_PROCESS","Getters","get_process_by_status.sql")
            with open(query_file_path, 'r') as file:
                query = file.read().strip().replace("\n", " ")
            
            # Replace placeholders in the query
            query = query.replace(":user", f"'{user}'")
            query = query.replace(":process_type", f"'{process_type}'")
            query = query.replace(":event_type", f"'{event_type}'")
            query = query.replace(":status", f"'{status}'")
            print(query)
            # Execute the query and fetch results into a DataFrame
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                df_data = pd.DataFrame(rows, columns=columns)
            
            return df_data
        elif use_case == 1:
            # Read SQL query from file
            query_file_path = os.path.join(pathOS, "Data", "Queries", "sql", "get_process_by_status.sql")
            with open(query_file_path, 'r') as file:
                query = file.read().strip().replace("\n", " ")
            
            # Replace placeholders in the query
            #query = query.replace(":user", f"'{user}'")
            #query = query.replace(":process_type", f"'{process_type}'")
            #query = query.replace(":event_type", f"'{event_type}'")
            #query = query.replace(":status", f"'{status}'")
            print(query)
            # Execute the query and fetch results into a DataFrame
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                df_data = pd.DataFrame(rows, columns=columns)
            
            return df_data
    
    except Exception as e:
        print(f"Error in get_process_by_status: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def get_process_by_id(pathOS, process_id, connection):
    """
    Retrieves a process by its ID from the database.
    
    Args:
        pathOS (str): The base path for configurations and query files.
        process_id (int): The ID of the process to fetch.
        connection: The database connection object.
    
    Returns:
        pd.DataFrame: A dataframe containing the process by ID.
    """
    try:
        connection = ensure_connection_with_config(connection)

        # Read SQL query from file
        query_file_path = os.path.join(pathOS, "Data", "Queries", "WFM_PROCESS","Getters", "get_process_by_id.sql")
        with open(query_file_path, 'r') as file:
            query = file.read().strip().replace("\n", " ")
        
        # Replace the placeholder in the query with the process_id
        query = query.replace(":i", str(process_id))
        
        # Execute the query and fetch results into a DataFrame
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            df_data = pd.DataFrame(rows, columns=columns)
        
        return df_data
    
    except Exception as e:
        print(f"Error in get_process_by_id: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def get_total_process_by_status(pathOS, connection):
    """
    Retrieves the total number of processes by status from the database.
    
    Args:
        pathOS (str): The base path for configurations and query files.
        connection: The database connection object.
    
    Returns:
        pd.DataFrame: A dataframe containing the total number of processes by status.
    """
    try:
        connection = ensure_connection_with_config(connection)

        # Read SQL query from file
        query_file_path = os.path.join(pathOS, "Data", "Queries", "WFM_PROCESS","Getters", "get_total_process_by_status.sql")
        with open(query_file_path, 'r') as file:
            query = file.read().strip().replace("\n", " ")
        
        # Replace placeholders in the query
        query = query.replace(":i_process_type", "'MPD'")
        query = query.replace(":i_status", "'P'")
        print(query)
        # Execute the query and fetch results into a DataFrame
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            df_data = pd.DataFrame(rows, columns=columns)
        
        return df_data
    
    except Exception as e:
        print(f"Error in get_total_process_by_status: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error
