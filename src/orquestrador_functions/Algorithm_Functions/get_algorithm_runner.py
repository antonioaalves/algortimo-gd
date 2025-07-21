# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 10:49:36 2025

@author: jason.vogensen
"""

from Classes.AlgorithmBaseClasses.ESCRunner import ESCRunner
from Classes.AlgorithmBaseClasses.MPDRunner import MPDRunner

def get_algorithm_runner(algorithm_to_run, process_id, logger, connection, process_available):
    if algorithm_to_run == "ESC":
        return ESCRunner(process_id, logger, connection, process_available)
    elif algorithm_to_run == "MPD":
        return MPDRunner(process_id, logger, connection, process_available)
    else:
        raise ValueError(f"Invalid algorithm: {algorithm_to_run}")