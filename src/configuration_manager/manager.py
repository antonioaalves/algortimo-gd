"""File containing the configuration manager class"""

# Dependencies

# Local stuff
from src.configuration_manager.path_config import PathConfig
from src.configuration_manager.parameter_config import ParameterConfig


# Configuration manager class

class ConfigurationManager:
    """Class used to manage the severeal configurations needed for the project"""

    def __init__(self):
        """Initialize the configuration manager"""

        # Load the configuration files
        self.config = self.load_base_config()
        self.oracle_config = self.load_oracle_config()
        self.path_config = self.load_path_config()
        self.parameter_config = self.load_parameter_config()
        self.stages_config = self.load_stages_config()

    def load_base_config(self):
        """Load the configuration file"""
        # TODO: Implement the loading function for python config files
        pass

    def load_oracle_config(self):
        """Load the Oracle configuration file"""
        # TODO: Implement the loading function for python config files
        pass

    def load_path_config(self):
        """Load the path configuration file"""
        # TODO: Implement the loading logic
        return PathConfig()

    def load_parameter_config(self):
        """Load the parameter configuration file"""
        # TODO: Implement the loading logic
        return ParameterConfig()

    def load_stages_config(self):
        """Load the stages configuration file"""
        # TODO: Implement the loading logic
        pass

