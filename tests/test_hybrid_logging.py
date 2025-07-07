"""Unit tests for hybrid logging system - CORRECTED VERSION."""

import unittest
import tempfile
import os
from unittest.mock import Mock, patch
from base_data_project.logging.template_manager import MessageTemplateManager
from base_data_project.logging.hybrid_manager import HybridLogManager
from base_data_project.logging.retention import cleanup_old_logs, get_log_file_stats


class TestMessageTemplateManager(unittest.TestCase):
    """Test cases for MessageTemplateManager."""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            'logging': {
                'df_messages_path': os.path.join(self.temp_dir, 'messages.csv')
            }
        }
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_test_csv(self, content: str):
        csv_path = self.config['logging']['df_messages_path']
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write(content)
    
    @patch('base_data_project.log_config.get_logger')
    def test_load_templates_success(self, mock_get_logger):
        mock_get_logger.return_value = Mock()
        
        csv_content = """VAR;ES
iniProc;"Iniciar proceso {1} - reintentar: {2}"
callSubproc;"Iniciar proceso {1} - llamando al subproceso {2}"
"""
        self._create_test_csv(csv_content)
        
        manager = MessageTemplateManager(self.config, 'test_project')
        
        self.assertTrue(manager.is_loaded())
        self.assertEqual(len(manager.get_all_templates()), 2)
    
    @patch('base_data_project.log_config.get_logger')
    def test_render_template_success(self, mock_get_logger):
        mock_get_logger.return_value = Mock()
        
        csv_content = """VAR;ES
iniProc;"Iniciar proceso {1} - reintentar: {2}"
"""
        self._create_test_csv(csv_content)
        
        manager = MessageTemplateManager(self.config, 'test_project')
        result = manager.render('iniProc', [123, 5])
        self.assertEqual(result, "Iniciar proceso 123 - reintentar: 5")


class TestHybridLogManager(unittest.TestCase):
    """Test cases for HybridLogManager."""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = {
            'logging': {
                'environment': 'local',
                'db_logging_enabled': False,
                'df_messages_path': os.path.join(self.temp_dir, 'messages.csv'),
                'server_file_logging': True
            },
            'external_call_data': {
                'current_process_id': 1961,
                'wfm_user': 'WFM_TEST',
                'start_date': '2025-01-01'
            }
        }
        
        csv_content = """VAR;ES
iniProc;"Iniciar proceso {1} - reintentar: {2}"
"""
        csv_path = self.config['logging']['df_messages_path']
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('base_data_project.log_config.get_logger')
    def test_log_template_local_environment(self, mock_get_logger):
        mock_file_logger = Mock()
        mock_get_logger.return_value = mock_file_logger
        
        mock_data_manager = Mock()
        mock_data_manager.config = self.config
        
        manager = HybridLogManager(mock_data_manager, 'test_project')
        manager.log_template('iniProc', [123, 2], 'INFO')
        
        # Verify the file logger was called
        mock_file_logger.info.assert_called()
        
        # Get the actual call arguments
        call_args = mock_file_logger.info.call_args[0][0]
        self.assertIn('[iniProc]', call_args)
        self.assertIn('Iniciar proceso 123 - reintentar: 2', call_args)
    
    @patch('base_data_project.log_config.get_logger')
    def test_backward_compatibility_methods(self, mock_get_logger):
        mock_file_logger = Mock()
        mock_get_logger.return_value = mock_file_logger
        
        mock_data_manager = Mock()
        mock_data_manager.config = self.config
        
        manager = HybridLogManager(mock_data_manager, 'test_project')
        
        manager.info("Info message")
        manager.error("Error message")
        
        mock_file_logger.info.assert_called_with("Info message")
        mock_file_logger.error.assert_called_with("Error message")


if __name__ == '__main__':
    unittest.main()