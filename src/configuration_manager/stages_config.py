"""File containing the stages configuration class"""

# Dependencies
from typing import Dict, Any

# Local stuff
from base_data_project.log_config import get_logger

class StagesConfig:
    """Class used to manage the stages configuration"""

    def __init__(self):
        """Initialize the stages configuration"""
        self.logger = get_logger(project_name="algoritmo_GD")

        # Load the stages configuration
        self.stages_config_dict = self.load_stages_config()

        # Validate the stages configuration
        if not self.validate_stages_config():
            raise ValueError("Stages configuration validation failed")

    def __getitem__(self, key):
        """Allow dictionary-style access to config values"""
        return self.stages_config_dict[key]
    
    def __contains__(self, key):
        """Allow 'in' operator to check if key exists"""
        return key in self.stages_config_dict
    
    def get(self, key, default=None):
        """Safe dictionary-style access with default value"""
        return self.stages_config_dict.get(key, default)
    
    def keys(self):
        """Return config keys"""
        return self.stages_config_dict.keys()
    
    def items(self):
        """Return config items"""
        return self.stages_config_dict.items()

    def load_stages_config(self) -> Dict[str, Any]:
        """Load the stages configuration from config.py"""
        try:
            from src.settings.project_structure import project_structure
            self.logger.info(f"Stages configuration loaded successfully")
            return project_structure
        except ImportError:
            self.logger.error(f"Error importing project_structure from project_structure.py")
            raise ImportError(f"Error importing project_structure from project_structure.py")
        except Exception as e:
            self.logger.error(f"Error loading stages configuration: {e}")
            raise e

    def validate_stages_config(self) -> bool:
        """Validate the stages configuration - TO BE IMPLEMENTED BY USER"""
        # TODO: Implement validation logic as needed
        # For now, just check if stages config exists
        if not self.stages_config_dict:
            self.logger.error("Stages configuration is empty")
            return False

        # Check if the stages configuration is valid
        if not self.stages_config_dict:
            self.logger.error("Stages configuration is empty")
            return False
        
        if not self.stages_config_dict.get("stages"):
            self.logger.error("Stages configuration is empty or it does not contain the 'stages' key")
            return False

        if len(self.stages_config_dict.get("stages")) == 0:
            self.logger.error("Stages configuration is empty or it does not contain any stages")
            return False

        # Cycle through the stages and ensure everything is conforming
        stages = self.stages_config_dict.get("stages", {})
        
        # Collect stage sequences for validation
        stage_sequences = []
        stage_sequences_by_name = {}
        
        for stage_name, stage_config in stages.items():
            if not isinstance(stage_config, dict):
                self.logger.error(f"Stage '{stage_name}' configuration must be a dictionary")
                return False
            
            # Validate required fields
            required_fields = ["sequence", "requires_previous", "validation_required"]
            for field in required_fields:
                if field not in stage_config:
                    self.logger.error(f"Stage '{stage_name}' missing required field: {field}")
                    return False
            
            # Collect sequence for later validation
            sequence = stage_config.get("sequence")
            if sequence is not None:
                stage_sequences.append(sequence)
                stage_sequences_by_name[stage_name] = sequence
                
            for key, value in stage_config.items():
                if key == "sequence":
                    if not isinstance(value, int) or value < 0:
                        self.logger.error(f"Stage '{stage_name}' has an invalid sequence number: {value}")
                        return False
                elif key == "requires_previous":
                    if not isinstance(value, bool):
                        self.logger.error(f"Stage '{stage_name}' has an invalid requires_previous value: {value}")
                        return False
                elif key == "validation_required":
                    if not isinstance(value, bool):
                        self.logger.error(f"Stage '{stage_name}' has an invalid validation_required value: {value}")
                        return False
                elif key == "decisions":
                    if not isinstance(value, dict):
                        self.logger.error(f"Stage '{stage_name}' has an invalid decisions value: {value}")
                        return False
                elif key == "substages":
                    if not isinstance(value, dict):
                        self.logger.error(f"Stage '{stage_name}' has an invalid substages value: {value}")
                        return False
                    
                    # Validate substages
                    substage_sequences = []
                    for substage_name, substage_config in value.items():
                        if not isinstance(substage_config, dict):
                            self.logger.error(f"Substage '{substage_name}' in stage '{stage_name}' must be a dictionary")
                            return False
                        
                        # Validate required substage fields
                        required_substage_fields = ["sequence", "description", "required"]
                        for field in required_substage_fields:
                            if field not in substage_config:
                                self.logger.error(f"Substage '{substage_name}' in stage '{stage_name}' missing required field: {field}")
                                return False
                            
                        for substage_key, substage_value in substage_config.items():
                            if substage_key == "sequence":
                                if not isinstance(substage_value, int) or substage_value < 0:
                                    self.logger.error(f"Substage '{substage_name}' in stage '{stage_name}' has an invalid sequence number: {substage_value}")
                                    return False
                                substage_sequences.append(substage_value)
                            elif substage_key == "description":
                                if not isinstance(substage_value, str):
                                    self.logger.error(f"Substage '{substage_name}' in stage '{stage_name}' has an invalid description: {substage_value}")
                                    return False
                            elif substage_key == "required":
                                if not isinstance(substage_value, bool):
                                    self.logger.error(f"Substage '{substage_name}' in stage '{stage_name}' has an invalid required value: {substage_value}")
                                    return False
                            elif substage_key == "decisions":
                                if not isinstance(substage_value, dict):
                                    self.logger.error(f"Substage '{substage_name}' in stage '{stage_name}' has an invalid decisions value: {substage_value}")
                                    return False
                    
                    # Validate substage sequence continuity
                    if substage_sequences:
                        substage_sequences.sort()
                        expected_substage_sequence = 1
                        for actual_sequence in substage_sequences:
                            if actual_sequence != expected_substage_sequence:
                                self.logger.error(f"Substage sequence gap in stage '{stage_name}': expected {expected_substage_sequence}, got {actual_sequence}")
                                return False
                            expected_substage_sequence += 1
                            
                elif key == "auto_complete_on_substages":
                    if not isinstance(value, bool):
                        self.logger.error(f"Stage '{stage_name}' has an invalid auto_complete_on_substages value: {value}")
                        return False
        
        # Validate stage sequence continuity
        if stage_sequences:
            stage_sequences.sort()
            expected_sequence = 1
            for actual_sequence in stage_sequences:
                if actual_sequence != expected_sequence:
                    self.logger.error(f"Stage sequence gap: expected {expected_sequence}, got {actual_sequence}")
                    return False
                expected_sequence += 1
        
        # Validate stage dependencies
        for stage_name, stage_config in stages.items():
            if stage_config.get("requires_previous", False):
                current_sequence = stage_config.get("sequence")
                if current_sequence is None or current_sequence <= 1:
                    self.logger.error(f"Stage '{stage_name}' requires previous but is first stage (sequence: {current_sequence})")
                    return False
                
                # Check if previous stage exists
                previous_stage_exists = False
                for other_stage_name, other_stage_config in stages.items():
                    if other_stage_config.get("sequence") == current_sequence - 1:
                        previous_stage_exists = True
                        break
                
                if not previous_stage_exists:
                    self.logger.error(f"Stage '{stage_name}' requires previous stage but no stage with sequence {current_sequence - 1} exists")
                    return False

        self.logger.info("Stages configuration validation successful")
        return True

    @property
    def stages(self) -> Dict[str, Any]:
        """Access stages with same interface as CONFIG"""
        return self.stages_config_dict
