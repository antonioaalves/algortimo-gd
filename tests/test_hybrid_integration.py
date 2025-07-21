"""Integration tests for hybrid logging system."""

import unittest
import tempfile
import os
import time
from unittest.mock import Mock, patch
import pandas as pd

from base_data_project.logging import get_hybrid_logger, validate_logging_config, setup_algorithm_gd_logging
from base_data_project.data_manager.factory import DataManagerFactory

class TestHybridLoggingIntegration(unittest.TestCase):
    """Integration tests for end-to-end hybrid logging functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test messages.csv
        csv_content = """VAR;ES
iniProc;"Iniciar proceso {1} - se alcanzo el maximo de hilos, reintentar: {2}"
callSubproc;"Iniciar proceso {1} - llamando al subproceso {2}"
errCallSubProc;"Iniciar proceso {1} - error al llamar al subproceso {2}"
errMaxThreads;"Fin del proceso {1} - se alcanzo el maximo de reintentos para maxThreads"
iniSubproc;"1.1 Iniciar subproceso {1} - parametros procesados"
errNoColab;"1.2 Subproceso {1} - no hay colaboradores para procesar"
"""
        
        self.csv_path = os.path.join(self.temp_dir, 'messages.csv')
        with open(self.csv_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        # Base configuration
        self.base_config = {
            'logging': {
                'environment': 'local',
                'db_logging_enabled': False,
                'df_messages_path': self.csv_path,
                'server_file_logging': True,
                'log_level': 'INFO',
                'log_dir': os.path.join(self.temp_dir, 'logs')
            },
            'external_call_data': {
                'current_process_id': 1961,
                'api_proc_id': 999,
                'wfm_proc_id': 1961,
                'wfm_user': 'WFM_TEST',
                'start_date': '2025-01-01',
                'end_date': '2025-12-31',
                'wfm_proc_colab': None,
            }
        }
        
        # Create logs directory
        os.makedirs(self.base_config['logging']['log_dir'], exist_ok=True)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_end_to_end_local_environment(self):
        """Test complete end-to-end flow in local environment."""
        with patch('base_data_project.log_config.get_logger') as mock_logger:
            mock_file_logger = Mock()
            mock_logger.return_value = mock_file_logger
            
            # Create CSV data manager
            data_manager = Mock()
            data_manager.config = self.base_config
            
            # Create hybrid logger
            logger = get_hybrid_logger('algorithm_GD', data_manager)
            
            # Test template-based logging
            logger.log_template('iniProc', [123, 2])
            logger.log_template('callSubproc', [123, 'subprocess_1'])
            logger.log_template('errCallSubProc', [123, 'subprocess_1'], 'ERROR')
            
            # Test backward compatibility
            logger.info("Traditional info message")
            logger.error("Traditional error message")
            
            # Verify file logging calls
            self.assertEqual(mock_file_logger.info.call_count, 3)  # 2 template + 1 traditional
            self.assertEqual(mock_file_logger.error.call_count, 2)  # 1 template + 1 traditional
            
            # Verify template rendering in logged messages
            info_calls = [call[0][0] for call in mock_file_logger.info.call_args_list]
            error_calls = [call[0][0] for call in mock_file_logger.error.call_args_list]
            
            # Check that templates were rendered correctly
            self.assertTrue(any('[iniProc]' in call and 'Iniciar proceso 123' in call for call in info_calls))
            self.assertTrue(any('[callSubproc]' in call and 'llamando al subproceso subprocess_1' in call for call in info_calls))
            self.assertTrue(any('[errCallSubProc]' in call and 'error al llamar al subproceso subprocess_1' in call for call in error_calls))
    
    def test_end_to_end_server_environment_with_db_fallback(self):
        """Test complete flow in server environment with database fallback."""
        # Configure for server environment
        server_config = self.base_config.copy()
        server_config['logging']['environment'] = 'server'
        server_config['logging']['db_logging_enabled'] = True
        
        with patch('base_data_project.log_config.get_logger') as mock_logger:
            mock_file_logger = Mock()
            mock_logger.return_value = mock_file_logger
            
            # Create mock data manager with failing database
            data_manager = Mock()
            data_manager.config = server_config
            data_manager.set_process_errors = Mock(return_value=False)  # Simulate DB failure
            
            # Create hybrid logger
            logger = get_hybrid_logger('algorithm_GD', data_manager)
            
            # Test template logging with database fallback
            logger.log_template('iniProc', [456, 3], 'INFO')
            
            # Verify database attempt was made
            data_manager.set_process_errors.assert_called_once_with(
                message_key='iniProc',
                rendered_message='Iniciar proceso 456 - se alcanzo el maximo de hilos, reintentar: 3',
                error_type='INFO'
            )
            
            # Verify file fallback occurred
            mock_file_logger.info.assert_called_once()
            call_args = mock_file_logger.info.call_args[0][0]
            self.assertIn('[DB_FALLBACK]', call_args)
            self.assertIn('[iniProc]', call_args)
    
    def test_end_to_end_server_environment_with_db_success(self):
        """Test complete flow in server environment with successful database logging."""
        # Configure for server environment
        server_config = self.base_config.copy()
        server_config['logging']['environment'] = 'server'
        server_config['logging']['db_logging_enabled'] = True
        
        with patch('base_data_project.log_config.get_logger') as mock_logger:
            mock_file_logger = Mock()
            mock_logger.return_value = mock_file_logger
            
            # Create mock data manager with successful database
            data_manager = Mock()
            data_manager.config = server_config
            data_manager.set_process_errors = Mock(return_value=True)  # Simulate DB success
            
            # Create hybrid logger
            logger = get_hybrid_logger('algorithm_GD', data_manager)
            
            # Test template logging
            logger.log_template('errMaxThreads', [789], 'ERROR')
            
            # Verify database logging was successful
            data_manager.set_process_errors.assert_called_once_with(
                message_key='errMaxThreads',
                rendered_message='Fin del proceso 789 - se alcanzo el maximo de reintentos para maxThreads',
                error_type='ERROR'
            )
            
            # Verify file logging also occurred (server_file_logging = True)
            mock_file_logger.error.assert_called_once()
            call_args = mock_file_logger.error.call_args[0][0]
            self.assertIn('[errMaxThreads]', call_args)
            self.assertNotIn('[DB_FALLBACK]', call_args)  # No fallback needed
    
    def test_environment_switching(self):
        """Test switching between dev and prod configurations."""
        # Dev configuration
        dev_config = self.base_config.copy()
        dev_config['logging']['environment'] = 'local'
        dev_config['logging']['db_logging_enabled'] = False
        
        # Prod configuration
        prod_config = self.base_config.copy()
        prod_config['logging']['environment'] = 'server'
        prod_config['logging']['db_logging_enabled'] = True
        
        with patch('base_data_project.log_config.get_logger') as mock_logger:
            mock_logger.return_value = Mock()
            
            # Test dev environment
            dev_data_manager = Mock()
            dev_data_manager.config = dev_config
            
            dev_logger = get_hybrid_logger('algorithm_GD', dev_data_manager)
            dev_status = dev_logger.get_logging_status()
            
            self.assertEqual(dev_status['environment'], 'local')
            self.assertFalse(dev_status['db_logging_available'])
            
            # Test prod environment
            prod_data_manager = Mock()
            prod_data_manager.config = prod_config
            prod_data_manager.set_process_errors = Mock()
            
            prod_logger = get_hybrid_logger('algorithm_GD', prod_data_manager)
            prod_status = prod_logger.get_logging_status()
            
            self.assertEqual(prod_status['environment'], 'server')
            self.assertTrue(prod_status['db_logging_available'])
    
    def test_template_error_scenarios(self):
        """Test various template error scenarios."""
        with patch('base_data_project.log_config.get_logger') as mock_logger:
            mock_file_logger = Mock()
            mock_logger.return_value = mock_file_logger
            
            data_manager = Mock()
            data_manager.config = self.base_config
            
            logger = get_hybrid_logger('algorithm_GD', data_manager)
            
            # Test missing template
            logger.log_template('nonexistent_template', [1, 2])
            
            # Test insufficient parameters
            logger.log_template('iniProc', [123])  # Missing second parameter
            
            # Verify error handling
            info_calls = [call[0][0] for call in mock_file_logger.info.call_args_list]
            
            # Should handle missing template gracefully
            self.assertTrue(any('[MISSING_TEMPLATE:nonexistent_template]' in call for call in info_calls))
            
            # Should handle insufficient parameters by using template as-is
            self.assertTrue(any('[iniProc]' in call for call in info_calls))
    
    def test_configuration_validation(self):
        """Test configuration validation functionality."""
        # Valid configuration
        valid_result = validate_logging_config(self.base_config)
        self.assertTrue(valid_result[0])
        self.assertEqual(len(valid_result[1]), 0)
        
        # Invalid configuration - missing logging section
        invalid_config1 = {}
        invalid_result1 = validate_logging_config(invalid_config1)
        self.assertFalse(invalid_result1[0])
        self.assertIn("Missing 'logging' section", invalid_result1[1][0])
        
        # Invalid configuration - bad environment
        invalid_config2 = {
            'logging': {
                'environment': 'invalid_env',
                'df_messages_path': 'test.csv'
            }
        }
        invalid_result2 = validate_logging_config(invalid_config2)
        self.assertFalse(invalid_result2[0])
        self.assertTrue(any('Invalid environment' in error for error in invalid_result2[1]))
        
        # Server configuration missing external_call_data
        server_config_invalid = {
            'logging': {
                'environment': 'server',
                'db_logging_enabled': True,
                'df_messages_path': 'test.csv'
            }
        }
        invalid_result3 = validate_logging_config(server_config_invalid)
        self.assertFalse(invalid_result3[0])
        self.assertTrue(any('external_call_data' in error for error in invalid_result3[1]))
    
    def test_setup_algorithm_gd_logging_quick_setup(self):
        """Test the quick setup function for algorithm_GD."""
        with patch('base_data_project.data_manager.factory.DataManagerFactory.create_data_manager') as mock_factory:
            mock_data_manager = Mock()
            mock_data_manager.config = self.base_config
            mock_factory.return_value = mock_data_manager
            
            with patch('base_data_project.log_config.get_logger') as mock_logger:
                mock_logger.return_value = Mock()
                
                # Test local setup
                logger = setup_algorithm_gd_logging(self.base_config, 'local')
                
                # Verify factory was called correctly
                mock_factory.assert_called_once()
                call_args = mock_factory.call_args
                self.assertEqual(call_args[1]['data_source_type'], 'csv')
                self.assertEqual(call_args[1]['project_name'], 'algorithm_GD')
                
                # Verify logger was created
                self.assertIsNotNone(logger)
                self.assertEqual(logger.project_name, 'algorithm_GD')
    
    def test_backward_compatibility_integration(self):
        """Test that existing logging code works unchanged."""
        with patch('base_data_project.log_config.get_logger') as mock_logger:
            mock_file_logger = Mock()
            mock_logger.return_value = mock_file_logger
            
            data_manager = Mock()
            data_manager.config = self.base_config
            
            logger = get_hybrid_logger('algorithm_GD', data_manager)
            
            # Simulate existing code patterns
            try:
                # Some operation that might log
                logger.info("Starting algorithm execution")
                
                # Some operation that might fail
                raise ValueError("Test error")
                
            except ValueError as e:
                logger.error(f"Algorithm failed: {str(e)}")
            
            finally:
                logger.info("Algorithm execution completed")
            
            # Verify all logging calls worked as expected
            self.assertEqual(mock_file_logger.info.call_count, 2)
            self.assertEqual(mock_file_logger.error.call_count, 1)
            
            # Verify message content
            info_calls = [call[0][0] for call in mock_file_logger.info.call_args_list]
            error_calls = [call[0][0] for call in mock_file_logger.error.call_args_list]
            
            self.assertIn("Starting algorithm execution", info_calls[0])
            self.assertIn("Algorithm execution completed", info_calls[1])
            self.assertIn("Algorithm failed: Test error", error_calls[0])

class TestDatabaseIntegrationReal(unittest.TestCase):
    """Tests for actual database integration (requires Oracle connection)."""
    
    def setUp(self):
        """Set up for database tests."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test messages.csv
        csv_content = """VAR;ES
test_connectivity;"Test de conectividad {1}"
"""
        
        self.csv_path = os.path.join(self.temp_dir, 'messages.csv')
        with open(self.csv_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        # Configuration for database testing
        self.db_config = {
            'logging': {
                'environment': 'server',
                'db_logging_enabled': True,
                'df_messages_path': self.csv_path,
                'server_file_logging': True
            },
            'external_call_data': {
                'current_process_id': 1961,
                'api_proc_id': 999,
                'wfm_proc_id': 1961,
                'wfm_user': 'WFM_TEST',
                'start_date': '2025-01-01',
                'end_date': '2025-12-31',
                'wfm_proc_colab': None,
            },
            'use_db': True,
            'db_url': 'oracle://test_connection'  # Would need real connection string
        }
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @unittest.skip("Requires actual Oracle database connection")
    def test_real_database_logging(self):
        """Test with real database connection (skipped by default)."""
        # This test would require actual Oracle database setup
        # and would be run manually when testing with real database
        
        try:
            # Create real data manager
            data_manager = DataManagerFactory.create_data_manager(
                data_source_type='db',
                config=self.db_config,
                project_name='algorithm_GD'
            )
            
            # Create hybrid logger
            logger = get_hybrid_logger('algorithm_GD', data_manager)
            
            # Test database connectivity
            success = logger.test_database_logging()
            self.assertTrue(success)
            
            # Test actual logging
            logger.log_template('test_connectivity', [123])
            
            # Verify no exceptions were raised
            self.assertTrue(True)
            
        except Exception as e:
            self.fail(f"Database integration test failed: {str(e)}")

if __name__ == '__main__':
    # Run all tests except the ones requiring real database
    unittest.main()