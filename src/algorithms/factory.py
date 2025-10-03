"""File containing the class AlgorithmFactory"""

# Dependencies
import logging
from typing import Optional, Dict, Any
from base_data_project.algorithms.base import BaseAlgorithm
from base_data_project.log_config import get_logger
from base_data_project.storage.models import BaseDataModel

# Local stuff
from src.algorithms.alcampoAlgorithm import AlcampoAlgorithm
from src.algorithms.salsaAlgorithm import SalsaAlgorithm
from src.config import PROJECT_NAME, CONFIG

logger = get_logger(PROJECT_NAME)

class AlgorithmFactory:
    """
    Factory class for creating algorithm instances
    """

    @staticmethod
    def create_algorithm(decision: str, parameters: Optional[Dict[str, Any]] = {}, process_id: int = 0, start_date: str = '', end_date: str = '') -> BaseAlgorithm:
        """Choose an algorithm based on user decisions"""

        if parameters is None:
            parameters = {
                'available_algorithms': CONFIG.get('available_algorithms', [])  # Default to empty list
            }
        available_algorithms = CONFIG.get('available_algorithms', [])
        if not isinstance(available_algorithms, list):
            available_algorithms = []
            logger.error(f"available_algorithms is not a list. Please configure the file config.py. available_algorithms: {available_algorithms}, type: {type(available_algorithms)}")

        if decision.lower() not in [algo.lower() for algo in available_algorithms]:
            # If decision not available, raise an exception
            logger.error(f"available_algorithms: {available_algorithms}, decision: {decision}")
            msg = f"Decision made for algorithm selection not available in config file config.py. Please configure the file."
            logger.error(msg)
            raise ValueError(msg)

        if decision.lower() == 'alcampo_algorithm':
            logger.info(f"Creating {decision.lower()} algorithm with parameters: {parameters}")
            return AlcampoAlgorithm(algo_name=decision.lower(), parameters=parameters, project_name=PROJECT_NAME, process_id=process_id, start_date=start_date, end_date=end_date) # TODO: define the algorithms here
        elif decision.lower() == 'salsa_algorithm':
            logger.info(f"Creating {decision.lower()} algorithm with parameters: {parameters}")
            return SalsaAlgorithm(algo_name=decision.lower(), parameters=parameters, project_name=PROJECT_NAME, process_id=process_id, start_date=start_date, end_date=end_date)
        #elif decision.lower() == 'salsa_esp_algorithm':
        #    logger.info(f"Creating {decision.lower()} algorithm with parameters: {parameters}")
        #    return SalsaAlgorithm(algo_name=decision.lower(), parameters=parameters, project_name=PROJECT_NAME, process_id=process_id, start_date=start_date, end_date=end_date)
        else:
            error_msg = f"Unsupported algorithm type: {decision}"
            logger.error(error_msg)
            raise ValueError(error_msg)