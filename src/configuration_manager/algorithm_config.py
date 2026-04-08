"""
Algorithm configuration management for the algorithm component.

Loads and validates settings from src/settings/algorithm-component/:
    - algorithm_parameters.json
    - restriction_parameters.json
    - solver_parameters.json
"""

# Dependencies
import json
import os
from typing import Dict, Any, Optional

# Local stuff
from base_data_project.log_config import get_logger


class AlgorithmConfig:
    """
    Algorithm configuration management.

    Exposes:
        algorithm_parameters: Dict[str, Any]
        restriction_parameters: Dict[str, Any]
        constraint_selections: Dict[str, Any]
        solver_parameters: Dict[str, Any]
    """

    def __init__(self, project_name: str):
        """
        Initialize AlgorithmConfig by loading JSON settings and validating structure.
        """
        self.logger = get_logger(project_name=project_name)

        self.algorithm_parameters: Dict[str, Any] = {}
        self.restriction_parameters: Dict[str, Any] = {}
        self.constraint_selections: Dict[str, Any] = {}
        self.solver_parameters: Dict[str, Any] = {}

        self._load_all()
        self._validate()
        self.logger.info("Algorithm component configuration loaded successfully")

    def _load_json(self, path: str) -> Dict[str, Any]:
        try:
            with open(path, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Algorithm config file not found: {path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in '{path}': {e}")

    def _load_all(self) -> None:
        # Resolve base directory relative to this file: src/configuration_manager/ -> src/
        src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base_dir = os.path.join(src_dir, "settings", "algorithm-component")

        # algorithm_parameters.json
        algo_params = self._load_json(os.path.join(base_dir, "algorithm_parameters.json"))
        if not isinstance(algo_params, dict):
            raise ValueError("algorithm_parameters.json must be a JSON object")
        self.algorithm_parameters = algo_params

        # restriction_parameters.json
        restr = self._load_json(os.path.join(base_dir, "restriction_parameters.json"))
        if not isinstance(restr, dict):
            raise ValueError("restriction_parameters.json must be a JSON object")
        self.restriction_parameters = restr.get("restriction_parameters", {})
        # Support both legacy typo and corrected key during migration
        if "constraint_selections" in restr:
            self.constraint_selections = restr.get("constraint_selections", {})
        else:
            if "contraint_selections" in restr:
                self.logger.warning("Using legacy key 'contraint_selections'. Please rename to 'constraint_selections'.")
            self.constraint_selections = restr.get("contraint_selections", {})

        # solver_parameters.json
        solver = self._load_json(os.path.join(base_dir, "solver_parameters.json"))
        if not isinstance(solver, dict):
            raise ValueError("solver_parameters.json must be a JSON object")
        self.solver_parameters = solver

    def _validate(self) -> None:
        # algorithm_parameters minimal validation
        if "data_treatment" not in self.algorithm_parameters:
            self.logger.warning("'data_treatment' not found in algorithm_parameters.json")

        # restriction_parameters structure
        if not isinstance(self.restriction_parameters, dict):
            raise ValueError("'restriction_parameters' must be an object")
        if not isinstance(self.constraint_selections, dict):
            raise ValueError("'contraint_selections' must be an object")

        # solver_parameters should contain profiles (e.g., 'salsa_tst')
        if not self.solver_parameters:
            self.logger.warning("solver_parameters.json is empty")

    # Public getters
    def get_algorithm_parameter(self, *path: str, default: Optional[Any] = None) -> Any:
        """
        Retrieve nested parameter from algorithm_parameters by path.
        Example: get_algorithm_parameter("data_treatment", "admissao_proporcional", "default_value")
        """
        current: Any = self.algorithm_parameters
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current

    def get_restriction_parameters(self) -> Dict[str, Any]:
        return self.restriction_parameters.copy()

    def get_constraint_selections(self) -> Dict[str, Any]:
        return self.constraint_selections.copy()

    def get_solver_profile(self, profile_name: str) -> Optional[Dict[str, Any]]:
        return self.solver_parameters.get(profile_name)

    def export_config_summary(self) -> Dict[str, Any]:
        return {
            "algorithm_parameters_keys": list(self.algorithm_parameters.keys()),
            "restriction_parameters_count": len(self.restriction_parameters),
            "constraint_selections_count": len(self.constraint_selections),
            "solver_profiles": list(self.solver_parameters.keys()),
        }

    def __repr__(self) -> str:
        return (
            "AlgorithmConfig("
            f"algo_keys={len(self.algorithm_parameters)}, "
            f"restrictions={len(self.restriction_parameters)}, "
            f"constraints={len(self.constraint_selections)}, "
            f"solver_profiles={len(self.solver_parameters)})"
        )
