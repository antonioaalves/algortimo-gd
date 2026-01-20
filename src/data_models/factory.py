"""
This module contains the factory for creating data models.
"""

# Dependencies
from typing import Type, Any, Dict
from base_data_project.log_config import get_logger
from base_data_project.storage.containers import BaseDataContainer

# Local stuff
from src.configuration_manager import ConfigurationManager
from src.configuration_manager.instance import get_config
from src.data_models.base import BaseDescansosDataModel
from src.data_models.models import DescansosDataModel
from src.data_models.salsa_model import SalsaDataModel
from src.data_models.alcampo_model import AlcampoDataModel
from src.data_models.adeo_model import AdeoDataModel

config_manager = get_config()
logger = get_logger(config_manager.project_name)

class DataModelFactory:
    """
    Factory class for creating data models.
    """

    @staticmethod
    def create_data_model(decision: str, external_data: Dict[str, Any]) -> BaseDescansosDataModel:
        """Choose an algorithm based on user decisions"""

        available_data_models = ['default_data_model', 'salsa_data_model', 'alcampo_data_model', 'adeo_data_model']
        # TODO: check this condition
        if not isinstance(decision, str) or decision.lower() not in available_data_models:
            error_msg = f"Unsupported decision for data model creation: {decision}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if decision.lower() not in [model.lower() for model in available_data_models]:
            # If decision not available, raise an exception
            logger.error(f"available_data_models: {available_data_models}, decision: {decision}")
            error_msg = f"Decision made for data model selection not available in config file config.py. Please configure the file."
            logger.error(error_msg)
            raise ValueError(error_msg)

        if decision.lower() == 'salsa_data_model':
            logger.info(f"Creating {decision.lower()}")
            return SalsaDataModel(
                    data_container=BaseDataContainer(
                        config=config_manager.get_storage_config(),
                        project_name=config_manager.project_name
                    ),
                    project_name=config_manager.project_name,
                    config_manager=config_manager,
                    external_data=external_data if external_data else {}
            )
        elif decision.lower() == 'alcampo_data_model':
            logger.info(f"Creating {decision.lower()}")
            return AlcampoDataModel(
                    data_container=BaseDataContainer(
                        config=config_manager.get_storage_config(),
                        project_name=config_manager.project_name
                    ),
                    project_name=config_manager.project_name,
                    config_manager=config_manager,
                    external_data=external_data if external_data else {}
                )
        elif decision.lower() == 'default_data_model':
            logger.info(f"Creating {decision.lower()}")
            return DescansosDataModel(
                    data_container=BaseDataContainer(
                        config=config_manager.get_storage_config(),
                        project_name=config_manager.project_name
                    ),
                    project_name=config_manager.project_name,
                    config_manager=config_manager,
                    external_data=external_data if external_data else {}
                )
        elif decision.lower() == 'adeo_data_model':
            logger.info(f"Creating {decision.lower()}")
            return AdeoDataModel(
                    data_container=BaseDataContainer(
                        config=config_manager.get_storage_config(),
                        project_name=config_manager.project_name
                    ),
                    project_name=config_manager.project_name,
                    config_manager=config_manager,
                    external_data=external_data if external_data else {}
                )
        
        else:
            error_msg = f"Unsupported data model type: {decision}"
            logger.error(error_msg)
            raise ValueError(error_msg)