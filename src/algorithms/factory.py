"""File containing the class AlgorithmFactory"""

# Dependencies
import logging
from typing import Optional, Dict, Any
from base_data_project.algorithms.base import BaseAlgorithm
from base_data_project.log_config import get_logger
from base_data_project.storage.models import BaseDataModel

# Local stuff
from src.algorithms.solverOne import SolverOne
from src.algorithms.example_algorithm import ExampleAlgorithm
from src.config import PROJECT_NAME

logger = get_logger(PROJECT_NAME)

class AlgorithmFactory:
    """
    Factory class for creating algorithm instances
    """

    @staticmethod
    def create_algorithm(decision: str, parameters: Optional[Dict[str, Any]] = None) -> BaseAlgorithm:
        """Choose an algorithm based on user decisions"""

        if parameters is None:
            # Use default configuration if not provided 
            from src.config import CONFIG
            parameters = {
                'available_algos': CONFIG.get('available_algorithms')
            }

        if decision.lower() not in parameters.get('available_algos'):
            # If decision not available, raise an exception
            msg = f"Decision made for algorithm selection not available in config file config.py. Please configure the file."
            logger.error(msg)
            raise ValueError(msg)

        if decision.lower() == 'solverOne':
            logger.info()
            return SolverOne(algo_name=decision.lower(), parameters=parameters) # TODO: define the algorithms here
        elif decision.lower() == 'example_algorithm':
            logger.info()
            return ExampleAlgorithm(algo_name=decision.lower(), parameters=parameters)
        else:
            error_msg = f"Unsupported algorithm type: {decision}"
            logger.error(error_msg)
            raise ValueError(error_msg)