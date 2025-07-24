"""File containing the configuration manager class"""

# Dependencies

# Local stuff
from src.configuration_manager.path_config import PathConfig
from src.configuration_manager.parameter_config import ParameterConfig
from src.configuration_manager.system_config import SystemConfig
from src.configuration_manager.base import BaseConfig
from src.configuration_manager.oracle_config import OracleConfig
from src.configuration_manager.stages_config import StagesConfig
from base_data_project.log_config import get_logger

class ConfigurationManager(BaseConfig):
    """Class used to manage the severeal configurations needed for the project"""

    def __init__(self):
        """Initialize the configuration manager"""
        self.logger = get_logger(project_name="algoritmo_GD")

        # Load the configuration files
        self.system_config = self.load_system_config()

        # Retrieve important variables
        use_db = self.system_config["use_db"]
        project_root_dir = self.system_config["project_root_dir"]
        environment = self.system_config["environment"]

        if use_db:
            self.oracle_config = self.load_oracle_config(environment=environment)
        else:
            self.oracle_config = None

        self.path_config = self.load_path_config(project_root_dir=project_root_dir, use_db=use_db)
        self.parameter_config = self.load_parameter_config()
        self.stages_config = self.load_stages_config()

    def load_system_config(self):
        """Load the configuration file"""
        return SystemConfig()

    def load_oracle_config(self, environment: str):
        """Load the Oracle configuration file"""
        return OracleConfig(environment=environment)

    def load_path_config(self, project_root_dir: str, use_db: bool):
        """Load the path configuration file"""
        return PathConfig(project_root_dir=project_root_dir, use_db=use_db)

    def load_parameter_config(self):
        """Load the parameter configuration file"""
        return ParameterConfig()

    def load_stages_config(self):
        """Load the stages configuration file"""
        return StagesConfig()