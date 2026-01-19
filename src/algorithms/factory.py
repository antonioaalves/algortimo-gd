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
from src.algorithms.adeoAlgorithm import AdeoAlgorithm
from src.algorithms.soverOne import SolverOne
from src.algorithms.example_algorithm import ExampleAlgorithm
from src.configuration_manager import ConfigurationManager
from src.configuration_manager.instance import get_config as get_config_manager

# Initialize logger with project name from config
logger = get_logger(get_config_manager().project_name)

class AlgorithmFactory:
    """
    Factory class for creating algorithm instances
    """

    @staticmethod
    def create_algorithm(decision: str, parameters: Optional[Dict[str, Any]] = None, 
                        project_name: str = None, process_id: int = 0, 
                        start_date: str = '', end_date: str = '', 
                        config_manager: ConfigurationManager = None) -> BaseAlgorithm:
        """
        Choose an algorithm based on user decisions.
        
        Args:
            decision: Algorithm name to create
            parameters: Optional algorithm parameters
            project_name: Project name (will use config if not provided)
            process_id: Process ID for tracking
            start_date: Start date for processing
            end_date: End date for processing
            config_manager: Configuration manager instance (will use singleton if not provided)
            
        Returns:
            BaseAlgorithm: Instance of the requested algorithm
            
        Raises:
            ValueError: If algorithm is not available or configuration is invalid
        """
        # Use provided config manager or singleton
        config = config_manager if config_manager else get_config_manager()
        
        # Use project name from config if not provided
        if project_name is None:
            project_name = config.project_name
            
        logger = get_logger(project_name)
        logger.info(f"Creating algorithm instance for: {decision} in factory.")

        # Get available algorithms from configuration
        available_algorithms = config.system.available_algorithms
        if not isinstance(available_algorithms, list):
            available_algorithms = []
            logger.error(f"available_algorithms is not a list. Available algorithms: {available_algorithms}, type: {type(available_algorithms)}")

        # Validate algorithm is available
        if decision.lower() not in [algo.lower() for algo in available_algorithms]:
            logger.error(f"available_algorithms: {available_algorithms}, decision: {decision}")
            msg = f"Decision made for algorithm selection not available in configuration. Available algorithms: {available_algorithms}. Requested: {decision}"
            logger.error(msg)
            raise ValueError(msg)

        # Merge parameters with defaults if needed
        if parameters is None:
            parameters = config.parameters.get_parameter_defaults()
        else:
            # Merge with defaults to ensure all required parameters are present
            default_params = config.parameters.get_parameter_defaults()
            merged_params = {**default_params, **parameters}
            parameters = merged_params

        # Create algorithm instances based on decision
        if decision.lower() == 'alcampo_algorithm':
            logger.info(f"Creating AlcampoAlgorithm instance")
            return AlcampoAlgorithm(
                parameters=parameters,
                algo_name=decision.lower(),
                project_name=project_name,
                process_id=process_id,
                start_date=start_date,
                end_date=end_date,
                config_manager=config
            )
        elif decision.lower() == 'salsa_algorithm':
            logger.info(f"Creating SalsaAlgorithm instance")
            return SalsaAlgorithm(
                parameters=parameters,
                algo_name=decision.lower(),
                project_name=project_name,
                process_id=process_id,
                start_date=start_date,
                end_date=end_date
            )
        elif decision.lower() == 'adeo_algorithm':
            logger.info(f"Creating AdeoAlgorithm instance")
            return AdeoAlgorithm(
                parameters=parameters,
                algo_name=decision.lower(),
                project_name=project_name,
                process_id=process_id,
                start_date=start_date,
                end_date=end_date
        )
        else:
            error_msg = f"Unsupported algorithm type: {decision}. Available algorithms: {available_algorithms}"
            logger.error(error_msg)
            raise ValueError(error_msg)