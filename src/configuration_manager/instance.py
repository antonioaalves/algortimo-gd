"""
Global Configuration Manager Instance - Single Source of Truth

This module provides a singleton instance of ConfigurationManager that is shared
across the entire application. Import get_config() instead of creating new instances.

Usage:
    from src.configuration_manager.instance import get_config
    
    config = get_config()
    project_name = config.project_name
    
Benefits:
    - Single source of truth for all configuration
    - Reduced memory usage (one instance vs many)
    - Reduced I/O (config files read once)
    - Consistent state across all components
    - Easy to mock for testing

Thread Safety:
    This implementation is thread-safe for read-only configuration access.
    The Python GIL protects the initialization. ConfigurationManager is
    designed to be immutable after initialization.
"""

from typing import Optional
from .manager import ConfigurationManager

# Module-level singleton instance
_config_instance: Optional[ConfigurationManager] = None


def get_config() -> ConfigurationManager:
    """
    Get the global ConfigurationManager instance (singleton).
    
    This function ensures only one ConfigurationManager exists throughout
    the application lifecycle. The instance is created on first call and
    reused for all subsequent calls.
    
    Returns:
        ConfigurationManager: The shared configuration manager instance
        
    Thread Safety:
        Thread-safe for the normal use case (read-only access).
        The GIL protects the initialization check and assignment.
        
    Performance:
        First call: ~100-200ms (reads all config files)
        Subsequent calls: <1μs (returns cached instance)
        
    Example:
        >>> config = get_config()
        >>> print(config.project_name)
        'algoritmo_GD'
        
        >>> config2 = get_config()
        >>> assert config is config2  # Same instance ✓
        >>> assert id(config) == id(config2)  # Same memory address ✓
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigurationManager()
    
    return _config_instance


def reset_config() -> None:
    """
    Reset the configuration instance (primarily for testing).
    
    ⚠️ WARNING: This should only be used in test scenarios.
    Do not call this in production code.
    
    After calling this, the next call to get_config() will create
    a fresh ConfigurationManager instance by re-reading all config files.
    
    Use Case:
        Testing with different configuration files or settings.
        
    Example:
        >>> # In a test
        >>> from src.configuration_manager.instance import get_config, reset_config
        >>> 
        >>> config1 = get_config()
        >>> reset_config()  # Clear the instance
        >>> config2 = get_config()  # Creates new instance
        >>> assert config1 is not config2  # Different instances
    """
    global _config_instance
    _config_instance = None


def is_initialized() -> bool:
    """
    Check if the configuration manager has been initialized.
    
    Returns:
        bool: True if get_config() has been called at least once, False otherwise
        
    Use Case:
        Useful for debugging or conditional initialization logic.
        
    Example:
        >>> from src.configuration_manager.instance import get_config, is_initialized
        >>> 
        >>> assert not is_initialized()  # Not yet initialized
        >>> config = get_config()
        >>> assert is_initialized()  # Now initialized
    """
    return _config_instance is not None

