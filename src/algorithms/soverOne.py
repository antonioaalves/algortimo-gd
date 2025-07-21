"""File containing the solverOne class"""

import logging
from typing import Dict, Any, Optional, List, Union, Tuple
import pandas as pd
from datetime import datetime

# Import base algorithm class
from base_data_project.algorithms.base import BaseAlgorithm
from base_data_project.log_config import get_logger

# Import project-specific components
from src.config import PROJECT_NAME, CONFIG

# Set up logger
logger = get_logger(PROJECT_NAME)

class SolverOne(BaseAlgorithm):
    """
    Solver implementation for João.

    This algorithm implements a simple data analysis process:
    1. Adapt data: Prepare input data for processing
    2. Execute algorithm: Perform the core processing logic
    3. Format results: Structure the outputs consistently

    The algorithm tryes to?
    """

    def __init__(self, parameters = None, algo_name: str = 'solverOne'):
        """Initialization method"""
        # Initialize the parent class with algorithm name and parameters
        super().__init__(algo_name=algo_name, parameters=parameters)

        # Define important df attributes
        self.input_data = {
            'df_estimativas': pd.DataFrame(),
            'df_colaborador': pd.DataFrame(),
            'df_calendario': pd.DataFrame(),
        }

        self.output_data = {
            'df_estimativas': pd.DataFrame(),
            'df_colaborador': pd.DataFrame(),
            'df_calendario': pd.DataFrame(),
        }
        
        # Add any algorithm-specific initialization
        self.logger.info(f"Initialized {self.algo_name} with parameters: {self.parameters}")


    def adapt_data(self, data: Dict[str, Any] = None) -> Tuple[bool, Dict[str, pd.DataFrame]]:
        """Adapt input data for processing"""
        # Call the base class method for common functionality
        super().adapt_data(data)

        if data is None:
            self.logger.error("No data provided for adaptation")
            return False, {"df_colaborador": pd.DataFrame(), "df_estimativas": pd.DataFrame(), "df_calendario": pd.DataFrame()}
        
        # Check if required dataframes are present
        required_dfs = ['df_estimativas', 'df_colaborador', 'df_calendario']
        for df_name in required_dfs:
            if df_name not in data or not isinstance(data[df_name], pd.DataFrame):
                self.logger.error(f"Missing or invalid dataframe: {df_name}")
                return False, f"Missing or invalid dataframe: {df_name}"

            # Store the input dataframes
            self.input_data[df_name] = data[df_name].copy()

        success = True
        self.logger.info("Data adaptation successful")
        return success, self.input_data
    
    def execute_algorithm(self, adapted_data = None) -> Tuple[bool, Dict[str, pd.DataFrame]]:
        return super().execute_algorithm(adapted_data)
    
    def format_results(self, algorithm_results = None) -> Tuple[bool, Dict[str, pd.DataFrame]]:
        """Format the results of the algorithm execution"""
        if algorithm_results is None:
            self.logger.error("No results to format")
            return {"error": "No results to format"}
        
        # Call the base class method for common functionality
        super().format_results(algorithm_results)
        return super().format_results(algorithm_results)
    