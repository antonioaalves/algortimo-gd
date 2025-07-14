# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:44:37 2024

@author: jason.vogensen
"""

import os
import cx_Oracle
import configparser
import datetime
    
    
def connect_to_oracle(acessos_path, log_file="log.txt"):
    """
    Establishes a connection to an Oracle database using credentials from an access file
    and sets the current schema.

    Parameters:
        acessos_path (str): Path to the 'acessos.txt' file with database credentials.
        log_file (str): Path to a log file where connection errors are recorded.

    Returns:
        connection (cx_Oracle.Connection): Oracle connection object if successful, None otherwise.
    """


    try:
        # Set up Oracle client path if required
        os.environ["PATH"] = r"C:\oracle\instantclient_23_6;" + os.environ["PATH"]
        
        # Read configuration for database access
        config = configparser.ConfigParser()
        config_file = os.path.join(acessos_path, "acessos.txt")
        
        if not os.path.isfile(config_file):
            raise FileNotFoundError(f"Configuration file not found at {config_file}")

        config.read(config_file)
        
        # Extract database credentials and settings
        user = config.get("Config", "user")
        passwd = config.get("Config", "pwd")
        url = config.get("Config", "url")
        port = config.get("Config", "port")
        service_name = config.get("Config", "service_name")
        
        # Create the DSN (Data Source Name) for Oracle
        dsn_tns = cx_Oracle.makedsn(url, port, service_name=service_name)
        
        # Establish connection
        connection = cx_Oracle.connect(user, passwd, dsn_tns, encoding="ISO-8859-1")
        print("Connected to the Oracle database")
        
        # Set the current schema
        with connection.cursor() as cursor:
            cursor.execute("ALTER SESSION SET CURRENT_SCHEMA=WFM")
            print("Schema set to WFM")
        
        return connection

    except Exception as e:
        # Log the error in a log file if connection fails
        error_message = f"{datetime.datetime.now().strftime('%d%m%Y %H:%M:%S')} ERROR CONNECTING TO DATABASE: {str(e)}"
        print(error_message)
        
        with open(log_file, "a+") as myfile:
            myfile.write(error_message + "\n")
        
        return None


def ensure_connection(connection, path):
    """
    Ensures that the database connection is active.
    
    Args:
        connection (cx_Oracle.Connection or None): Current database connection.
        path (str): Path for reconnecting if connection is lost.
    
    Returns:
        cx_Oracle.Connection: A valid database connection.
    """
    try:
        # Check if connection exists and is valid
        if connection is None:
            print("No connection exists. Creating new connection...")
            connection = connect_to_oracle(path)
        else:
            # Check if the connection is valid and open
            connection.ping()  # Ping to test the connection
            #print("Connection is active.")
    except cx_Oracle.InterfaceError as e:
        print(f"Connection is closed or invalid: {e}. Reconnecting...")
        connection = connect_to_oracle(path)
    except cx_Oracle.DatabaseError as e:
        print(f"Database error: {e}. Reconnecting...")
        connection = connect_to_oracle(path)
    except AttributeError as e:
        print(f"Connection object is None: {e}. Creating new connection...")
        connection = connect_to_oracle(path)
    
    return connection




def disconnect_from_oracle(connection, log_file="log.txt"):
    """
    Closes the connection to the Oracle database.

    Parameters:
        connection (cx_Oracle.Connection): The database connection to be closed.
        log_file (str): Path to a log file where disconnection errors are recorded.
    
    Returns:
        bool: True if disconnected successfully, False otherwise.
    """
    import datetime

    try:
        if connection:
            connection.close()
            print("Disconnected from the Oracle database")
            return True
        else:
            print("No active connection to close.")
            return False
    except Exception as e:
        error_message = f"{datetime.datetime.now().strftime('%d%m%Y %H:%M:%S')} ERROR DISCONNECTING FROM DATABASE: {str(e)}"
        print(error_message)
        
        with open(log_file, "a+") as myfile:
            myfile.write(error_message + "\n")
        
        return False