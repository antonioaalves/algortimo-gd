"""
Process stages and workflow configuration management.

This module handles workflow stage definitions including:
- Stage sequences and dependencies
- Stage-specific configurations
- Workflow orchestration settings

Configuration is loaded from src/settings/project_structure.py
"""

from typing import Dict, Any, List, Optional
from base_data_project.log_config import get_logger


class StagesConfig:
    """
    Process stages and workflow configuration management.
    
    This class manages workflow stage definitions that control the
    execution sequence and configuration of different process phases.
    
    Dynamic dictionary (structure depends on configuration):
        stages: Dict[str, Any] - Complete stages configuration including:
            - Stage definitions with sequences
            - Stage-specific parameters
            - Dependencies and execution order
            - Any custom stage configurations
    """
    
    def __init__(self, project_name: str):
        """
        Initialize stages configuration.
        
        Args:
            project_name: Project name for logging purposes
            
        Raises:
            FileNotFoundError: If project_structure.py cannot be imported
            ValueError: If configuration validation fails
        """
        self.logger = get_logger(project_name)
        
        # Load stages configuration
        self.stages: Dict[str, Any] = self._load_stages_config()
        
        self._validate_stages()
        self.logger.info("Stages configuration loaded successfully")
    
    def _load_stages_config(self) -> Dict[str, Any]:
        """
        Load stages configuration from project structure module.
        
        Returns:
            Dict[str, Any]: Complete stages configuration
            
        Raises:
            FileNotFoundError: If project_structure.py cannot be imported
            ValueError: If configuration loading fails
        """
        try:
            from src.settings.project_structure import project_structure
            self.logger.info("Stages configuration imported successfully")
            # Extract the actual stages dict (unwrap the "stages" key)
            if "stages" in project_structure:
                return project_structure["stages"]
            return project_structure
        except ImportError:
            raise FileNotFoundError("Could not import project_structure from project_structure.py")
        except Exception as e:
            raise ValueError(f"Error loading stages configuration: {e}")
    
    def _validate_stages(self) -> None:
        """
        Validate stages configuration structure.
        
        Raises:
            ValueError: If stages configuration is invalid
        """
        if not isinstance(self.stages, dict):
            raise ValueError("Stages configuration must be a dictionary")
        
        if not self.stages:
            self.logger.warning("Stages configuration is empty")
        else:
            # Validate that each stage has basic required structure
            for stage_name, stage_config in self.stages.items():
                if not isinstance(stage_config, dict):
                    raise ValueError(f"Stage '{stage_name}' configuration must be a dictionary")
        
        self.logger.info("Stages configuration validation passed")
    
    def get_all_stages(self) -> Dict[str, Any]:
        """
        Get all configured stages.
        
        Returns:
            Dict[str, Any]: Complete stages configuration
        """
        return self.stages.copy()
    
    def get_stage_names(self) -> List[str]:
        """
        Get list of all configured stage names.
        
        Returns:
            List[str]: List of stage names
        """
        return list(self.stages.keys())
    
    def get_stage_config(self, stage_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific stage.
        
        Args:
            stage_name: Name of the stage
            
        Returns:
            Optional[Dict[str, Any]]: Stage configuration or None if not found
        """
        return self.stages.get(stage_name)
    
    def has_stage(self, stage_name: str) -> bool:
        """
        Check if a stage is configured.
        
        Args:
            stage_name: Name of the stage to check
            
        Returns:
            bool: True if stage exists in configuration
        """
        return stage_name in self.stages
    
    def get_stage_sequence(self, stage_name: str) -> Optional[int]:
        """
        Get the sequence number for a specific stage.
        
        Args:
            stage_name: Name of the stage
            
        Returns:
            Optional[int]: Stage sequence number or None if not found/configured
        """
        stage_config = self.get_stage_config(stage_name)
        if stage_config:
            return stage_config.get("sequence")
        return None
    
    def get_stages_by_sequence(self) -> List[tuple]:
        """
        Get stages ordered by their sequence number.
        
        Returns:
            List[tuple]: List of (stage_name, sequence_number) ordered by sequence
        """
        stages_with_sequence = []
        
        for stage_name, stage_config in self.stages.items():
            sequence = stage_config.get("sequence")
            if sequence is not None:
                stages_with_sequence.append((stage_name, sequence))
        
        # Sort by sequence number
        stages_with_sequence.sort(key=lambda x: x[1])
        return stages_with_sequence
    
    def get_ordered_stage_names(self) -> List[str]:
        """
        Get stage names ordered by sequence.
        
        Returns:
            List[str]: Stage names in execution order
        """
        ordered_stages = self.get_stages_by_sequence()
        return [stage_name for stage_name, _ in ordered_stages]
    
    def get_next_stage(self, current_stage: str) -> Optional[str]:
        """
        Get the next stage in the sequence after the current stage.
        
        Args:
            current_stage: Name of the current stage
            
        Returns:
            Optional[str]: Name of the next stage or None if current is last/not found
        """
        ordered_stages = self.get_ordered_stage_names()
        
        try:
            current_index = ordered_stages.index(current_stage)
            if current_index < len(ordered_stages) - 1:
                return ordered_stages[current_index + 1]
        except ValueError:
            # Current stage not found in ordered list
            pass
        
        return None
    
    def get_previous_stage(self, current_stage: str) -> Optional[str]:
        """
        Get the previous stage in the sequence before the current stage.
        
        Args:
            current_stage: Name of the current stage
            
        Returns:
            Optional[str]: Name of the previous stage or None if current is first/not found
        """
        ordered_stages = self.get_ordered_stage_names()
        
        try:
            current_index = ordered_stages.index(current_stage)
            if current_index > 0:
                return ordered_stages[current_index - 1]
        except ValueError:
            # Current stage not found in ordered list
            pass
        
        return None
    
    def is_first_stage(self, stage_name: str) -> bool:
        """
        Check if a stage is the first in the sequence.
        
        Args:
            stage_name: Name of the stage to check
            
        Returns:
            bool: True if stage is first in sequence
        """
        ordered_stages = self.get_ordered_stage_names()
        return ordered_stages and ordered_stages[0] == stage_name
    
    def is_last_stage(self, stage_name: str) -> bool:
        """
        Check if a stage is the last in the sequence.
        
        Args:
            stage_name: Name of the stage to check
            
        Returns:
            bool: True if stage is last in sequence
        """
        ordered_stages = self.get_ordered_stage_names()
        return ordered_stages and ordered_stages[-1] == stage_name
    
    def get_stage_parameter(self, stage_name: str, parameter_name: str, default: Any = None) -> Any:
        """
        Get a specific parameter value for a stage.
        
        Args:
            stage_name: Name of the stage
            parameter_name: Name of the parameter
            default: Default value if parameter not found
            
        Returns:
            Any: Parameter value or default
        """
        stage_config = self.get_stage_config(stage_name)
        if stage_config:
            return stage_config.get(parameter_name, default)
        return default
    
    def validate_stage_sequence(self) -> Dict[str, str]:
        """
        Validate that stage sequences are properly configured.
        
        Returns:
            Dict[str, str]: Validation errors (empty if all valid)
        """
        errors = {}
        sequences = []
        
        # Collect all sequence numbers
        for stage_name, stage_config in self.stages.items():
            sequence = stage_config.get("sequence")
            if sequence is not None:
                if not isinstance(sequence, int):
                    errors[stage_name] = f"Sequence must be an integer, got {type(sequence)}"
                elif sequence < 0:
                    errors[stage_name] = "Sequence must be non-negative"
                else:
                    sequences.append((stage_name, sequence))
        
        # Check for duplicate sequences
        sequence_counts = {}
        for stage_name, sequence in sequences:
            if sequence in sequence_counts:
                existing_stage = sequence_counts[sequence]
                errors[stage_name] = f"Duplicate sequence {sequence} with stage '{existing_stage}'"
            else:
                sequence_counts[sequence] = stage_name
        
        return errors
    
    def export_workflow_summary(self) -> Dict[str, Any]:
        """
        Export a summary of the workflow configuration for debugging/logging.
        
        Returns:
            Dict[str, Any]: Workflow summary
        """
        ordered_stages = self.get_stages_by_sequence()
        validation_errors = self.validate_stage_sequence()
        
        summary = {
            "total_stages": len(self.stages),
            "stages_with_sequence": len(ordered_stages),
            "execution_order": [stage for stage, _ in ordered_stages],
            "validation_errors": validation_errors,
            "has_validation_errors": bool(validation_errors)
        }
        
        return summary
    
    def __repr__(self) -> str:
        """String representation of StagesConfig."""
        stage_count = len(self.stages)
        sequenced_count = len(self.get_stages_by_sequence())
        return f"StagesConfig(total_stages={stage_count}, sequenced_stages={sequenced_count})"