"""File containing the data model for Alcampo"""

# Dependencies
from typing import Dict, Any
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.log_config import get_logger

# Local stuff
from src.data_models.base import BaseDescansosDataModel
from src.configuration_manager.base import BaseConfig
from src.configuration_manager.instance import get_config

class AlcampoDataModel(BaseDescansosDataModel):
    """
    Data model for Alcampo algorithm.
    
    This class manages data loading, transformation, and validation
    for the Alcampo shift scheduling algorithm.
    """

    def __init__(self, data_container: BaseDataContainer, project_name: str = 'algoritmo_GD', config_manager: BaseConfig = None, external_data: Dict[str, Any] = None):
        """
        Initialize the AlcampoDataModel.
        
        Args:
            data_container: Container for storing intermediate data
            project_name: Name of the project
            config_manager: Configuration manager instance (uses singleton if None)
            external_data: External data dictionary with process parameters
        """
        # Use provided config_manager or get singleton instance
        self.config_manager = config_manager if config_manager is not None else get_config()
        
        super().__init__(data_container=data_container, project_name=project_name)
        
        # External call data coming from the product
        self.external_call_data = self.config_manager.parameters.external_call_data if self.config_manager else {}
        
        self.logger.info("AlcampoDataModel initialized")
    