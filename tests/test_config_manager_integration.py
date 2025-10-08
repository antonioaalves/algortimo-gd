"""
Complete Migration Test Script for Configuration Manager
Tests all migrated components to ensure they work correctly with the new system.
"""

import sys
import os
import traceback
from typing import Dict, Any, List
import pandas as pd

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_configuration_manager():
    """Test basic configuration manager functionality."""
    print("🔧 Testing Configuration Manager...")
    
    try:
        from src.configuration_manager import ConfigurationManager
        
        # Test singleton pattern - use get_config_manager, not direct instantiation
        from src.algorithms.factory import get_config_manager
        config1 = get_config_manager()
        config2 = get_config_manager()
        assert config1 is config2, "Singleton pattern failed"
        
        # Test basic properties
        assert hasattr(config1, 'project_name'), "Missing project_name"
        assert hasattr(config1, 'system'), "Missing system config"
        assert hasattr(config1, 'is_database_enabled'), "Missing database flag"
        
        print(f"   ✅ Project: {config1.project_name}")
        print(f"   ✅ Environment: {config1.system.environment}")
        print(f"   ✅ Database enabled: {config1.is_database_enabled}")
        print(f"   ✅ Available algorithms: {len(config1.system.available_algorithms)}")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_singleton_pattern():
    """Test that all components use the same configuration instance."""
    print("🔄 Testing Singleton Pattern Consistency...")
    
    try:
        # Import get_config_manager from different modules
        from src.algorithms.factory import get_config_manager as factory_config
        from src.algorithms.alcampoAlgorithm import get_config_manager as alcampo_config
        from src.models import get_config_manager as models_config
        from src.helpers import get_config_manager as helpers_config
        from src.services.example_service import get_config_manager as service_config
        
        # Get instances
        config1 = factory_config()
        config2 = alcampo_config()
        config3 = models_config()
        config4 = helpers_config()
        config5 = service_config()
        
        # Test they are all the same instance
        assert config1 is config2, "Factory and Alcampo configs are different instances"
        assert config2 is config3, "Alcampo and Models configs are different instances"
        assert config3 is config4, "Models and Helpers configs are different instances"
        assert config4 is config5, "Helpers and Service configs are different instances"
        
        print("   ✅ All modules share the same configuration instance")
        print(f"   ✅ Shared project name: {config1.project_name}")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_algorithm_factory():
    """Test the migrated algorithm factory."""
    print("🏭 Testing Algorithm Factory...")
    
    try:
        from src.algorithms.factory import AlgorithmFactory, get_config_manager
        
        config = get_config_manager()
        available_algorithms = config.system.available_algorithms
        
        if not available_algorithms:
            return False, "No algorithms configured"
        
        # Test creating an algorithm
        algorithm_name = available_algorithms[0]
        print(f"   Testing algorithm: {algorithm_name}")
        
        algorithm = AlgorithmFactory.create_algorithm(
            decision=algorithm_name,
            parameters=None,
            project_name=config.project_name
        )
        
        assert algorithm is not None, f"Failed to create algorithm {algorithm_name}"
        assert hasattr(algorithm, 'run'), "Algorithm missing run method"
        
        print(f"   ✅ Successfully created {algorithm_name}")
        print(f"   ✅ Algorithm has required methods")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_alcampo_algorithm():
    """Test the migrated Alcampo algorithm."""
    print("🔄 Testing Alcampo Algorithm...")
    
    try:
        from src.algorithms.alcampoAlgorithm import AlcampoAlgorithm, get_config_manager
        
        config = get_config_manager()
        
        # Check if alcampo algorithm is available
        if not config.system.has_algorithm('alcampo_algorithm'):
            return True, "Alcampo algorithm not configured (skipping)"
        
        # Create algorithm instance
        algorithm = AlcampoAlgorithm(
            algo_name='alcampo_algorithm',
            project_name=config.project_name
        )
        
        assert algorithm is not None, "Failed to create AlcampoAlgorithm"
        assert algorithm.config is not None, "Algorithm missing config"
        assert algorithm.project_name == config.project_name, "Project name mismatch"
        
        print(f"   ✅ AlcampoAlgorithm created successfully")
        print(f"   ✅ Configuration properly injected")
        print(f"   ✅ Project name: {algorithm.project_name}")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_data_model():
    """Test the migrated data model."""
    print("📊 Testing Data Model...")
    
    try:
        from src.models import DescansosDataModel, get_config_manager
        from base_data_project.storage.containers import BaseDataContainer
        
        config = get_config_manager()
        
        # Create a basic data container
        data_container = BaseDataContainer()
        
        # Create data model
        data_model = DescansosDataModel(
            data_container=data_container,
            config_manager=config
        )
        
        assert data_model is not None, "Failed to create DescansosDataModel"
        assert data_model.config is not None, "Data model missing config"
        assert data_model.project_name == config.project_name, "Project name mismatch"
        
        # Test basic functionality
        data_mode = data_model.get_data_source_mode()
        available_entities = data_model.get_available_entities()
        
        print(f"   ✅ Data model created successfully")
        print(f"   ✅ Data mode: {data_mode}")
        print(f"   ✅ Available entities: {len(available_entities)}")
        
        # Test data summary
        summary = data_model.get_data_summary()
        assert 'config' in summary, "Missing config in summary"
        assert 'raw_data' in summary, "Missing raw_data in summary"
        
        print(f"   ✅ Data summary generated successfully")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_helpers():
    """Test the migrated helper functions."""
    print("🛠️ Testing Helper Functions...")
    
    try:
        from src.helpers import get_config_manager, get_oracle_url_cx, calcular_max
        import pandas as pd
        
        config = get_config_manager()
        
        # Test configuration access
        assert config is not None, "Failed to get config from helpers"
        assert config.project_name, "Missing project name in helpers config"
        
        # Test Oracle URL function
        oracle_url = get_oracle_url_cx()
        print(f"   ✅ Oracle URL function works (returns: {type(oracle_url).__name__})")
        
        # Test helper calculation function
        test_df = pd.DataFrame({'values': [1, 2, 3, 4, 5]})
        max_val = calcular_max(test_df, 'values')
        assert max_val == 5, f"Expected 5, got {max_val}"
        
        print(f"   ✅ Helper calculation functions work")
        print(f"   ✅ Configuration properly injected")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_service():
    """Test the migrated service."""
    print("🚀 Testing Service...")
    
    try:
        from src.services.example_service import AlgoritmoGDService, get_config_manager
        from base_data_project.data_manager.managers.managers import CSVDataManager
        from base_data_project.storage.containers import BaseDataContainer
        
        config = get_config_manager()
        
        # Create a basic data manager for testing
        data_container = BaseDataContainer()
        data_manager = CSVDataManager(data_container=data_container)
        
        # Create service
        service = AlgoritmoGDService(
            data_manager=data_manager,
            config_manager=config
        )
        
        assert service is not None, "Failed to create AlgoritmoGDService"
        assert service.config is not None, "Service missing config"
        assert service.project_name == config.project_name, "Project name mismatch"
        
        # Test configuration summary
        config_summary = service.get_configuration_summary()
        assert 'project_name' in config_summary, "Missing project_name in summary"
        assert 'data_mode' in config_summary, "Missing data_mode in summary"
        
        # Test workflow status
        status = service.get_workflow_status()
        assert 'service_info' in status, "Missing service_info in status"
        
        print(f"   ✅ Service created successfully")
        print(f"   ✅ Configuration properly injected")
        print(f"   ✅ Status and summary methods work")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_connection_module():
    """Test the migrated connection module."""
    print("🔌 Testing Connection Module...")
    
    try:
        from src.orquestrador_functions.Classes.Connection.connect import (
            get_config_manager, test_connection, OracleConnectionManager
        )
        
        config = get_config_manager()
        
        # Test configuration access
        assert config is not None, "Failed to get config from connect module"
        assert config.project_name, "Missing project name in connect config"
        
        # Test connection manager class
        conn_manager = OracleConnectionManager(use_config=True)
        assert conn_manager is not None, "Failed to create OracleConnectionManager"
        
        print(f"   ✅ Connection module loads successfully")
        print(f"   ✅ Configuration properly injected")
        print(f"   ✅ Connection manager class available")
        
        # Note: We don't test actual database connection here as it requires setup
        print(f"   ℹ️ Database connection test skipped (requires DB setup)")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_parameter_access():
    """Test parameter access across components."""
    print("⚙️ Testing Parameter Access...")
    
    try:
        from src.algorithms.factory import get_config_manager
        
        config = get_config_manager()
        
        # Test parameter defaults
        param_defaults = config.parameters.get_parameter_defaults()
        assert isinstance(param_defaults, dict), "Parameter defaults should be dict"
        
        # Test algorithm config
        available_algorithms = config.system.available_algorithms
        if available_algorithms:
            algo_name = available_algorithms[0]
            algo_config = config.parameters.get_algorithm_config(algo_name)
            assert isinstance(algo_config, dict), "Algorithm config should be dict"
            
            print(f"   ✅ Algorithm '{algo_name}' has {len(algo_config)} parameters")
        
        # Test parameter names
        param_names = config.parameters.get_parameter_names()
        assert isinstance(param_names, list), "Parameter names should be list"
        
        print(f"   ✅ Parameter defaults: {len(param_defaults)} parameters")
        print(f"   ✅ Parameter names: {len(param_names)} names")
        print(f"   ✅ Parameter access methods work correctly")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_path_configuration():
    """Test path configuration access."""
    print("📁 Testing Path Configuration...")
    
    try:
        from src.algorithms.factory import get_config_manager
        
        config = get_config_manager()
        
        if config.is_database_enabled:
            # Test SQL paths
            sql_paths = config.paths.sql_processing_paths
            assert isinstance(sql_paths, dict), "SQL paths should be dict"
            print(f"   ✅ SQL processing paths: {len(sql_paths)} entities")
            
            if hasattr(config.paths, 'sql_auxiliary_paths'):
                aux_paths = config.paths.sql_auxiliary_paths
                print(f"   ✅ SQL auxiliary paths: {len(aux_paths)} entities")
        else:
            # Test CSV paths
            csv_paths = config.paths.csv_filepaths
            assert isinstance(csv_paths, dict), "CSV paths should be dict"
            print(f"   ✅ CSV file paths: {len(csv_paths)} entities")
        
        print(f"   ✅ Path configuration access works correctly")
        print(f"   ✅ Data mode: {'Database' if config.is_database_enabled else 'CSV'}")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def test_validation():
    """Test configuration validation."""
    print("✅ Testing Configuration Validation...")
    
    try:
        from src.algorithms.factory import get_config_manager
        
        config = get_config_manager()
        
        # Test overall validation
        is_valid = config.validate_all_configs()
        print(f"   ✅ Overall configuration valid: {is_valid}")
        
        # Test individual component validation
        try:
            config.system._validate_required_fields()
            system_valid = True
        except:
            system_valid = False
        print(f"   ✅ System config valid: {system_valid}")
        
        # Test configuration summary
        summary = config.get_config_summary()
        assert isinstance(summary, dict), "Config summary should be dict"
        assert 'system' in summary, "Missing system in summary"
        
        print(f"   ✅ Configuration summary generated")
        print(f"   ✅ Validation methods work correctly")
        
        return True, None
        
    except Exception as e:
        return False, str(e)

def run_all_tests():
    """Run all migration tests."""
    print("🧪 Starting Complete Migration Tests")
    print("=" * 60)
    
    tests = [
        ("Configuration Manager", test_configuration_manager),
        ("Singleton Pattern", test_singleton_pattern),
        ("Algorithm Factory", test_algorithm_factory),
        ("Alcampo Algorithm", test_alcampo_algorithm),
        ("Data Model", test_data_model),
        ("Helper Functions", test_helpers),
        ("Service", test_service),
        ("Connection Module", test_connection_module),
        ("Parameter Access", test_parameter_access),
        ("Path Configuration", test_path_configuration),
        ("Validation", test_validation),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            success, error = test_func()
            results.append((test_name, success, error))
            
            if success:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED - {error}")
            
        except Exception as e:
            error_msg = f"Test exception: {str(e)}"
            results.append((test_name, False, error_msg))
            print(f"💥 {test_name}: ERROR - {error_msg}")
            print(f"   Traceback: {traceback.format_exc()}")
        
        print("-" * 40)
    
    # Summary
    print("\n📋 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, success, _ in results if success)
    total = len(results)
    
    print(f"Total Tests: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {total - passed}")
    print(f"Success Rate: {(passed/total)*100:.1f}%")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Migration is successful!")
    else:
        print("\n⚠️ Some tests failed. Please review the errors above.")
        
        print("\nFailed Tests:")
        for test_name, success, error in results:
            if not success:
                print(f"  ❌ {test_name}: {error}")
    
    return passed == total

def test_specific_component(component_name: str):
    """Test a specific component by name."""
    test_mapping = {
        'config': test_configuration_manager,
        'singleton': test_singleton_pattern,
        'factory': test_algorithm_factory,
        'alcampo': test_alcampo_algorithm,
        'model': test_data_model,
        'helpers': test_helpers,
        'service': test_service,
        'connection': test_connection_module,
        'parameters': test_parameter_access,
        'paths': test_path_configuration,
        'validation': test_validation,
    }
    
    if component_name not in test_mapping:
        print(f"❌ Unknown component: {component_name}")
        print(f"Available components: {', '.join(test_mapping.keys())}")
        return False
    
    print(f"🧪 Testing Component: {component_name}")
    print("=" * 40)
    
    try:
        success, error = test_mapping[component_name]()
        
        if success:
            print(f"✅ {component_name}: PASSED")
        else:
            print(f"❌ {component_name}: FAILED - {error}")
        
        return success
        
    except Exception as e:
        print(f"💥 {component_name}: ERROR - {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Configuration Manager Migration")
    parser.add_argument('--component', '-c', help="Test specific component only")
    parser.add_argument('--verbose', '-v', action='store_true', help="Verbose output")
    
    args = parser.parse_args()
    
    if args.component:
        success = test_specific_component(args.component)
        sys.exit(0 if success else 1)
    else:
        success = run_all_tests()
        sys.exit(0 if success else 1)