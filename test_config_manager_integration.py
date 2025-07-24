#!/usr/bin/env python3
"""Test script to verify configuration manager integration."""

import sys
import os
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_configuration_manager():
    """Test that the configuration manager loads and provides all required configurations."""
    print("🧪 Testing Configuration Manager Integration")
    print("=" * 50)
    
    try:
        # Test 1: Import and instantiate configuration manager
        print("1. Testing Configuration Manager Import...")
        from src.configuration_manager.manager import ConfigurationManager
        config_manager = ConfigurationManager()
        print("   ✅ Configuration Manager imported and instantiated successfully")
        
        # Test 2: Verify system configuration
        print("\n2. Testing System Configuration...")
        system_config = config_manager.system_config
        required_system_keys = [
            'environment', 'use_db', 'project_name', 'project_root_dir',
            'storage_strategy', 'available_algorithms', 'logging'
        ]
        
        for key in required_system_keys:
            if key in system_config:
                print(f"   ✅ {key}: {system_config[key]}")
            else:
                print(f"   ❌ Missing {key}")
                return False
        
        # Test 3: Verify path configuration
        print("\n3. Testing Path Configuration...")
        path_config = config_manager.path_config
        
        # Test property accessors
        processing_entities = path_config.available_entities_processing
        aux_entities = path_config.available_entities_aux
        raw_entities = path_config.available_entities_raw
        dummy_data = path_config.dummy_data_filepaths
        
        print(f"   ✅ Processing entities: {len(processing_entities)} found")
        print(f"   ✅ Auxiliary entities: {len(aux_entities)} found")
        print(f"   ✅ Raw entities: {len(raw_entities)} found")
        print(f"   ✅ Dummy data paths: {len(dummy_data)} found")
        
        # Test 4: Verify parameter configuration
        print("\n4. Testing Parameter Configuration...")
        param_config = config_manager.parameter_config
        
        process_params = param_config.process_parameters
        external_call_data = param_config.external_call_data
        
        print(f"   ✅ Process parameters: {len(process_params)} found")
        print(f"   ✅ External call data: {len(external_call_data)} found")
        
        # Test 5: Verify stages configuration
        print("\n5. Testing Stages Configuration...")
        stages_config = config_manager.stages_config
        stages = stages_config.stages
        
        print(f"   ✅ Stages: {len(stages)} found")
        for stage_name, stage_config in stages.items():
            print(f"      - {stage_name}: sequence {stage_config.get('sequence', 'N/A')}")
        
        # Test 6: Verify Oracle configuration (if using database)
        print("\n6. Testing Oracle Configuration...")
        if config_manager.oracle_config:
            oracle_config = config_manager.oracle_config
            required_oracle_keys = ['host', 'port', 'service_name', 'username', 'password', 'schema']
            
            for key in required_oracle_keys:
                if key in oracle_config:
                    value = oracle_config[key] if key != 'password' else '***'
                    print(f"   ✅ {key}: {value}")
                else:
                    print(f"   ❌ Missing {key}")
                    return False
            
            # Test connection URL generation
            try:
                connection_url = oracle_config.get_connection_url()
                print(f"   ✅ Connection URL generated: {connection_url[:50]}...")
            except Exception as e:
                print(f"   ❌ Failed to generate connection URL: {e}")
                return False
        else:
            print("   ⚠️  Oracle configuration not available (use_db=False)")
        
        # Test 7: Test backward compatibility with existing components
        print("\n7. Testing Component Integration...")
        
        # Test helpers.py integration
        from src.helpers import get_oracle_url_cx
        try:
            if config_manager.system_config.get('use_db', True):
                oracle_url = get_oracle_url_cx()
                print(f"   ✅ Helpers integration: Oracle URL generated")
            else:
                print("   ⚠️  Helpers integration: Database not enabled")
        except Exception as e:
            print(f"   ❌ Helpers integration failed: {e}")
            return False
        
        # Test 8: Verify all configuration files are accessible
        print("\n8. Testing Configuration File Access...")
        config_files = [
            'src/settings/system_settings.py',
            'src/settings/process_parameters.json',
            'src/settings/folder_hierarchy.json',
            'src/settings/sql_filepaths.json',
            'src/settings/oracle_connection_parameters.json',
            'src/settings/project_structure.py'
        ]
        
        for config_file in config_files:
            if os.path.exists(config_file):
                print(f"   ✅ {config_file}")
            else:
                print(f"   ❌ Missing {config_file}")
                return False
        
        print("\n🎉 All Configuration Manager Tests Passed!")
        print("=" * 50)
        print("✅ Configuration Manager is properly integrated and feeding all components")
        print("✅ All configuration files are accessible")
        print("✅ All required configuration sections are loaded")
        print("✅ Component integration is working")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Configuration Manager Test Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_component_usage():
    """Test that components can use the configuration manager."""
    print("\n🧪 Testing Component Usage")
    print("=" * 30)
    
    try:
        # Test algorithm factory
        print("1. Testing Algorithm Factory...")
        from src.algorithms.factory import create_algorithm
        from src.configuration_manager.manager import ConfigurationManager
        
        config_manager = ConfigurationManager()
        
        # Test creating an algorithm with config manager
        algorithm = create_algorithm(
            project_name=config_manager.system_config.get('project_name', 'algoritmo_GD'),
            decision='alcampo_algorithm',
            config_manager=config_manager
        )
        
        print(f"   ✅ Algorithm created: {type(algorithm).__name__}")
        
        # Test service creation
        print("2. Testing Service Creation...")
        from base_data_project.utils import create_components
        from src.services.example_service import AlgoritmoGDService
        
        data_manager, process_manager = create_components(
            use_db=config_manager.system_config.get('use_db', False),
            no_tracking=True,
            config=config_manager,
            project_name=config_manager.system_config.get('project_name', 'algoritmo_GD')
        )
        
        service = AlgoritmoGDService(
            data_manager=data_manager,
            project_name=config_manager.system_config.get('project_name', 'algoritmo_GD'),
            config_manager=config_manager
        )
        
        print(f"   ✅ Service created: {type(service).__name__}")
        
        print("\n🎉 Component Usage Tests Passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ Component Usage Test Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting Configuration Manager Integration Tests")
    print("=" * 60)
    
    # Run tests
    config_test_passed = test_configuration_manager()
    component_test_passed = test_component_usage()
    
    if config_test_passed and component_test_passed:
        print("\n🎉 ALL TESTS PASSED!")
        print("✅ Configuration Manager is fully integrated and working")
        print("✅ All components can access configuration properly")
        sys.exit(0)
    else:
        print("\n❌ SOME TESTS FAILED!")
        print("❌ Configuration Manager integration needs attention")
        sys.exit(1) 