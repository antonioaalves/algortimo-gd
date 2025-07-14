# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:06:29 2024

@author: jason.vogensen
"""

import pandas as pd
import os

def get_messages(path_os, lang='ES'):
    """
    Reads a CSV file containing message translations, reshapes it, and filters based on the specified language.
    
    Parameters:
        path_os (str): The base path to the file.
        lang (str): The language code to filter messages by. Default is 'ES'.
    
    Returns:
        pd.DataFrame: DataFrame with the messages for the specified language or an empty DataFrame on error.
    """
    try:
        # Read CSV file into DataFrame
        df_msg = pd.read_csv(os.path.join(path_os, 'data', 'csvs', 'messages_df.csv'), sep=',')
        df_msg.rename(columns={df_msg.columns[0]: 'VAR'}, inplace=True)
        
        # Reshape DataFrame and filter based on language
        df_msg = df_msg.melt(id_vars=['VAR'], var_name='LANG', value_name='DESC')
        df_msg = df_msg[df_msg['LANG'] == lang]
        
        return df_msg
    except Exception as e:
        print("Error:", str(e))
        return pd.DataFrame()  # Return an empty DataFrame on error


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


def set_messages(df_msg, var, values):
    """
    Retrieves a message template from DataFrame, replaces placeholders, and returns the formatted message.
    
    Parameters:
        df_msg (pd.DataFrame): DataFrame containing message templates.
        var (str): The variable name to filter the message by.
        values (dict): Dictionary of placeholders and their corresponding values.
    
    Returns:
        str: The formatted message with placeholders replaced.
    """
    message_row = df_msg[df_msg['VAR'] == var]
    if not message_row.empty:
        template = message_row.iloc[0]['DESC']
        return replace_placeholders(template, values)
    else:
        return ""
