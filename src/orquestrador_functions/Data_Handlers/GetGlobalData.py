# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 17:08:55 2025

@author: jason.vogensen
"""
import os
import cx_Oracle
import pandas as pd

def get_all_params(path_os, unit_id=None, secao=None, posto=None,  connection=None, p_name=None,):
    """
    Fetch all parameters from the database using cx_Oracle and filter based on input criteria.
    Returns a list of dictionaries (rows) or 'Error' if an error occurs.
    """
    try:
        # Check if connection is valid
        if connection is None:
            print("Error in get_all_params: Connection is None")
            return "Error"
        
        # Read the SQL query from the file
        query_path = os.path.join(path_os, "data/Queries/GlobalData/get_all_parameters.sql")
        if not os.path.exists(query_path):
            print(f"Error in get_all_params: SQL file not found at {query_path}")
            return "Error"
            
        with open(query_path, 'r') as query_file:
            query = query_file.read()

        # Execute the query
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        # Fetch column names
        col_names = [col[0] for col in cursor.description]

        # Convert rows to a list of dictionaries
        df_data = [dict(zip(col_names, row)) for row in rows]

        # Filter data if `p_name` is provided
        if p_name is not None:
            df_data = [row for row in df_data if row.get("SYS_P_NAME") == p_name]

        # Filter by unit_id (scalar value; support int or string unit_id and DB column)
        if unit_id is not None:
            df_data = [row for row in df_data if str(row.get("FK_UNIDADE")) == str(unit_id) or row.get("FK_UNIDADE") is None]

        # Filter by secao (scalar value)
        if secao is not None:
            df_data = [row for row in df_data if row.get("FK_SECAO") == secao or row.get("FK_SECAO") is None]

        # Filter by posto (list of values)
        if posto is not None:
            df_data = [row for row in df_data if row.get("FK_TIPO_POSTO") in posto or row.get("FK_TIPO_POSTO") is None]

        # Ensure uniqueness
        df_data = [dict(t) for t in {tuple(d.items()) for d in df_data}]

        return df_data

    except Exception as e:
        print(f"Error in get_all_params: {e}")
        return "Error"

    finally:
        if 'cursor' in locals():
            cursor.close()




def get_gran_equi(path_os, secao, connection=None):
    """
    Fetch the granularity value for the given secao from the database using cx_Oracle.
    Returns the granularity as a number or 'Error' if an error occurs.
    """
    try:
        # Check if connection is valid
        if connection is None:
            print("Error in get_gran_equi: Connection is None")
            return "Error"
            
        query = """
        SELECT s_pck_core_parameter.getnumberattr('GRANULARIDADE_ESCALA', 'S', :secao) 
        FROM dual
        """
        cursor = connection.cursor()

        # Ensure `secao` is passed as a single scalar value
        cursor.execute(query, {"secao": secao})
        result = cursor.fetchone()

        # Return the granularity or handle empty results
        if result:
            return result[0]
        else:
            print("No data found for secao.")
            return "Error"

    except cx_Oracle.DatabaseError as e:
        print(f"Database error in get_gran_equi: {e}")
        return "Error"

    finally:
        if 'cursor' in locals():
            cursor.close()
            
            
def get_faixa_sec(path_os, uni, sec, dia1, dia2, connection=None):
    """
    Fetch faixas_sec data for the given parameters using cx_Oracle.

    Parameters:
        path_os (str): Base path for files and queries.
        uni (str): Unit ID.
        sec (str): Section ID.
        dia1 (datetime): Start date.
        dia2 (datetime): End date.
        connection (cx_Oracle.Connection): Active database connection.

    Returns:
        pd.DataFrame: DataFrame containing the query result.
        str: "Error" if an error occurs.
    """
    try:
        # Check if connection is valid
        if connection is None:
            print("Error in get_faixa_sec: Connection is None")
            return "Error"
            
        # Load the SQL query from file
        query_path = os.path.join(path_os, "data/Queries/GlobalData/get_faixas_sec.sql")
        if not os.path.exists(query_path):
            print(f"Error in get_faixa_sec: SQL file not found at {query_path}")
            return "Error"
            
        with open(query_path, 'r') as query_file:
            query = query_file.read()

        # Replace placeholders with actual values
        query = query.replace(":d1", f"'{dia1.strftime('%Y-%m-%d')}'")
        query = query.replace(":d2", f"'{dia2.strftime('%Y-%m-%d')}'")
        query = query.replace(":s", f"{sec}")
        query = query.replace(":l", f"'{uni}'")
        # Execute the query
        cursor = connection.cursor()
        print(query)
        cursor.execute(query)
        rows = cursor.fetchall()

        # Fetch column names
        col_names = [str(col[0]) for col in cursor.description]

        # Convert result to DataFrame
        df_data = pd.DataFrame(data=rows, columns=pd.Index(col_names))

        return df_data

    except cx_Oracle.DatabaseError as e:
        print(f"Database error in get_faixa_sec: {e}")
        return "Error"

    finally:
        if 'cursor' in locals():
            cursor.close()