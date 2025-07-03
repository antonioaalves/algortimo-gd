"""
This file contains the algorithm for salsa client.
"""

# Dependencies
from base_data_project.algorithms.base import BaseAlgorithm
from base_data_project.log_config import get_logger
from typing import Dict, Any
import pandas as pd

# Local stuff
from src.config import PROJECT_NAME, ROOT_DIR


class SalsaAlgorithm(BaseAlgorithm):
    """
    This class implements the algorithm for salsa client.
    """
    def __init__(self, algo_name: str, parameters: dict, project_name: str, process_id: int, start_date: str, end_date: str):
        """
        Initialize the algorithm class.
        Args: 
            algo_name: str, name of the algorithm
            parameters: dict, parameters of the algorithm
        """

        default_parameters = {}
        if parameters:
            default_parameters = parameters.update(parameters)
            self.logger.info(f"Parameters: {default_parameters}")
        else:
            default_parameters = parameters

        super().__init__(algo_name=algo_name, parameters=parameters, project_name=project_name)

        self.logger.info(f"Initializing {algo_name} algorithm")

        # Initialize the algorithm-specific attributes
        self.process_id = process_id
        self.start_date = start_date
        self.end_date = end_date

    def adapt_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Adapt the data to the algorithm.
        Args:
            data: pd.DataFrame, data to adapt
        """
        self.logger.info(f"Adapting data to {self.algo_name} algorithm")
        return {
            'schedule': pd.DataFrame(),
        }

    def format_results(self, algorithm_results: pd.DataFrame = pd.DataFrame()) -> Dict[str, Any]:
        """
        Format the algorithm results for output.
        Args:
            algorithm_results: pd.DataFrame, algorithm results
        """
        self.logger.info(f"Formatting {self.algo_name} algorithm results. Not implemented yet.")
        return {
            'schedule': pd.DataFrame(),
        }

    def execute_algorithm(self, adapted_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Execute the algorithm.
        Args:
            adapted_data: pd.DataFrame, adapted data
        """
        self.logger.info(f"Executing {self.algo_name} algorithm. Not implemented yet.")
        self.logger.info(f"DEBUG: Returning empty df.")
        return pd.DataFrame()


    def run(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Run the complete algorithm pipeline: adapt_data -> execute_algorithm -> format_results.
        
        Args:
            data: Input data dictionary containing DataFrames
            
        Returns:
            Formatted results dictionary
        """
        self.logger.info("Running full Salsa algorithm pipeline")
        
        # Step 1: Adapt data
        adapted_data = self.adapt_data(data)
        
        # Step 2: Execute algorithm
        results = self.execute_algorithm(adapted_data)
        
        # Step 3: Format results
        formatted_results = self.format_results(results)
        
        self.logger.info("Full algorithm pipeline completed successfully")

        return formatted_results