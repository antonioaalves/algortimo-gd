"""
Algorithm configuration management.
"""

# Dependencies

# Local stuff
from typing import Dict, Any
from base_data_project.log_config import get_logger

class AlgorithmConfig:
    """
    """

    def __init__(self, project_name: str):
        """
        """

        self.logger = get_logger(project_name=project_name)

        self._config_data = None # TODO: add method call


    def _load_algorithm_config(self) -> Dict[str, Any]:
        """
        """

