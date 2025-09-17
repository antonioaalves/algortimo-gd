"""Configuration for the my_new_project project."""

# Dependencies
import os
from pathlib import Path
import subprocess

# Project name - used for logging and process tracking
# It is here to not create a circular import
PROJECT_NAME = "algoritmo_GD"

# Local stuff
from src.helpers import get_oracle_url_cx

# Get application root directory
ROOT_DIR = Path(__file__).resolve().parents[1]

# Add R configuration to CONFIG dictionary

CONFIG = {
    # Database configuration
    'use_db': True,
    'db_url': get_oracle_url_cx(),
    #'db_url': f"sqlite:///{os.path.join(ROOT_DIR, 'data', 'production.db')}",
    
    # Base directories
    'data_dir': os.path.join(ROOT_DIR, 'data'),
    'output_dir': os.path.join(ROOT_DIR, 'data', 'output'),
    'log_dir': os.path.join(ROOT_DIR, 'logs'),

    'storage_strategy': {
        'mode': 'memory',  # Options: 'memory', 'persist', 'hybrid'
        'persist_intermediate_results': False,
        'stages_to_persist': [],  # Empty list means all stages
        'cleanup_policy': 'keep_latest',  # Options: 'keep_all', 'keep_latest', 'keep_none'
        'persist_format': '',  # Options: 'csv', 'db'
        'storage_dir': 'data/intermediate'  # For CSV storage 
    }, # TODO: ensure data/intermediate is created if it doesnt exist

    'logging': {
        'environment': 'server',  # or 'local' for development
        'db_logging_enabled': True,
        'df_messages_path': 'data/csvs/messages.csv',
        'log_errors_db': True,  # Enable/disable database error logging with set_process_errors
    },
    
    # File paths for CSV data sources
    'external_call_data': {
        'current_process_id': 5260,#253762,# 249468,
        'api_proc_id': 999,
        'wfm_proc_id': 5260,
        'wfm_user': 'WFM',
        'start_date': '2025-01-01',
        'end_date': '2025-12-31',
        'wfm_proc_colab': None, 
    }, # TODO: create the default values to run locally

    # Params names and defaults - understand why they are here
    'parameters_names': [
        'algorithm_name', # TODO: Define the real params names
        'GD_gloablMaxThreads',
        'GD_maxThreads',
        'gloablMaxThreads',
        'maxThreads',
        'maxRetries',
        'sleepTime',
        'test_param_scheduling_threshold',
        'GD_algorithmName',
        'GD_consideraFestivos',
        'GD_convenioBD',
    ],

    'parameters_defaults': {
        #'algorithm_name': 'salsa_algorithm',
        'GD_gloablMaxThreads': 4,
        'GD_maxThreads': 2,
        'gloablMaxThreads': 4,
        'maxThreads': 2,
        'maxRetries': 3,
        'sleepTime': 1000,
        'test_param_scheduling_threshold': 0.75,
        #'GD_algorithmName': '',
        'GD_consideraFestivos': 1,
        'GD_convenioBD': 'ALCAMPO',
    },

    'dummy_data_filepaths': {
        # Example data files mapping - replace with your actual data files
        'valid_emp': os.path.join(ROOT_DIR, 'data', 'csvs', 'valid_emp.csv'),
        'df_ausencias_ferias': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_ausencias_ferias.csv'),
        'df_ciclos_90': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_ciclos_90.csv'),
        'df_colaborador': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_colaborador.csv'),
        'df_estrutura_wfm': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_estrutura_wfm.csv'),
        'df_faixa_horario': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_faixa_horario.csv'),
        'df_feriados': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_feriados.csv'),
        'df_festivos':  os.path.join(ROOT_DIR, 'data', 'csvs', 'df_festivos.csv'),
        'df_messages': os.path.join(ROOT_DIR, 'data', 'csvs', 'messages_df.csv'),
        'df_orcamento': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_orcamento.csv'),
        'df_pre_gerados': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_pre_gerados.csv'),
        'params_algo': os.path.join(ROOT_DIR, 'data', 'csvs', 'params_algo.csv'),
        'params_lq': os.path.join(ROOT_DIR, 'data', 'csvs', 'params_lq.csv'),
        'df_calendario_passado': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_calendario_passado.csv'),
        'df_granularidade': os.path.join(ROOT_DIR, 'data', 'csvs', 'df_granularidade.csv'),
    },
    
    'available_entities_processing': {
        'valid_emp': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetValidEmployees_v2.sql'),
        'df_messages': os.path.join(ROOT_DIR, 'src', 'sql_querys', ''),
        'params_algo': os.path.join(ROOT_DIR, 'src', 'sql_querys', ''),
        'params_lq': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'qry_params_LQ.sql'),
        'df_festivos':  os.path.join(ROOT_DIR, 'src', 'sql_querys', 'qry_festivos.sql'),
        'df_closed_days': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'qry_closed_days.sql'),
        'params_df': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetAlgoParameters.sql'),
        'parameters_cfg': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetParametersCFG.sql'),
    },

    'available_entities_aux': {
        #'df_pre_gerados': os.path.join(ROOT_DIR, 'src', 'sql_queries', 'queryGetPregerados.sql'), # TODO: was taken out, remove
        'df_ausencias_ferias': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetAusencias.sql'),
        'df_ciclos_90': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGet90CyclesInfo.sql'),
        'df_estrutura_wfm': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetEstruturaWFM.sql'),
        'df_feriados': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetFeriadosAbertos.sql'),
        'df_faixa_horario': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetEscFaixaHorario.sql'),
        'df_orcamento': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetEscOrcamento.sql'),
        'df_calendario_passado': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetCoreSchedule.sql'),
        'df_granularidade': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetEscEstimado.sql'),
        'df_days_off': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetDaysOff.sql'),
        'df_core_pro_emp_horario_det': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetCoreProEmpHorarioDet.sql'),
        'df_contratos': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'queryGetCoreProEmpContract.sql')
    },

    'available_entities_raw': {
        'df_calendario': os.path.join(ROOT_DIR, 'src', 'sql_querys', ''),
        'df_colaborador': os.path.join(ROOT_DIR, 'src', 'sql_querys', 'qry_ma.sql'),
        'df_estimativas': os.path.join(ROOT_DIR, 'src', 'sql_querys', ''),
    },

    # Available algorithms for the project
    'available_algorithms': [
        'alcampo_algorithm',
        'example_algorithm',
        'salsa_algorithm',
        # Add your custom algorithms here
    ],
     
    # Process configuration - stages and decision points
    'stages': {
        # Stage 1: Data Loading
        'data_loading': {
            'sequence': 1,               # Stage order
            'requires_previous': False,  # First stage doesn't require previous stages
            'validation_required': True, # Validate data after loading (normally performed by a method associated with the data container in src/models.py)
            'decisions': {
                'selections': {          # Decision point for data selection
                    'selected_entities': ['valid_employees'],  # Default entities to load
                    'load_all': False,   # Whether to load all available entities
                },
                'params_algo': {
                    'param_pessoas_objetivo': 0.5,
                    'param_NLDF': 2,
                    'param_NL10': 0,
                    'param_relax_colabs': 2
                }, 
            }
        },
        
        # Stage 2: Data Transformation
        'processing': {
            'sequence': 2,
            'requires_previous': True,   # Requires previous stage completion
            'validation_required': True,
            ### NOT NEEDED SINCE NO DECISIONS FOR THIS PROJECT
            'decisions': {
                'transformations': {     # Decision point for transformation options
                    'apply_filtering': False,
                    'filter_column': '',
                    'filter_value': '',
                    'normalize_numeric': True,  # Whether to normalize numerical data
                    'fill_missing': True,       # Whether to fill missing values
                    'fill_method': 'mean'       # Method for filling missing values
                },
                #'algorithm': {
                #    'name': 'salsa_algorithm',  # Default algorithm to use - should come from the parameters_defaults in the future
                #    'parameters': {}
                #},
                'insertions': {
                    'insert_results': False
                }
            },
            'substages': {
                'treat_params': {
                    'sequence': 1,
                    'description': 'Establishing connection to data source',
                    'required': True,
                    'decisions': {}
                },
                'load_matrices': {
                    'sequence': 2,
                    'description': 'Load dataframes containing all the data',
                    'required': True,
                    'decisions': {}                  
                },
                'func_inicializa': {
                    'sequence': 3,
                    'description': 'Function that initializes data transformation for each matrix',
                    'required': True,
                    'decisions': {}                     
                },
                'allocation_cycle': {
                    'sequence': 4,
                    'description': 'Allocation cycle for all the required days.',
                    'required': True,
                    'decisions': {
                        # Very important define the algorithms here
                        #'algorithms': ['salsa_algorithm']
                    }                     
                },
                'format_results': {
                    'sequence': 5,
                    'description': 'Format results to be inserted',
                    'required': True,
                    'decisions': {}
                },
                'insert_results': {
                    'sequence': 6,
                    'description': 'Insert results to the database',
                    'required': False,
                    'decisions': {}
                }
            },
            'auto_complete_on_substages': False, # Auto-complete stage when all substages are done
        },
        

    },
    
    # Algorithm parameters (defaults for each algorithm)
    'algorithm_defaults': {
        'example_algorithm': {
            'threshold': 50.0,
            'include_outliers': False,
            'outlier_threshold': 2.0
        },
        # Add defaults for your custom algorithms here
    },
    
    # Output configuration
    'output': {
        'base_dir': 'data/output',
        'visualizations_dir': 'data/output/visualizations',
        'diagnostics_dir': 'data/diagnostics'
    },
    
    # Logging configuration
    'log_level': 'INFO',
    'log_format': '%(asctime)s | %(levelname)8s | %(filename)s:%(lineno)d | %(message)s',
    'log_dir': 'logs'
}

# For algorithm_GD development
ALGORITHM_GD_DEV_CONFIG = {
    'logging': {
        'environment': 'local',
        'db_logging_enabled': False,
        'df_messages_path': 'data/csvs/messages.csv',
        'server_file_logging': True,
        'log_level': 'DEBUG',
        'log_dir': 'logs'
    },
    'external_call_data': {
        'current_process_id': 1961,
        'api_proc_id': 999,
        'wfm_proc_id': 1961,
        'wfm_user': 'WFM_DEV',
        'start_date': '2025-01-01',
        'end_date': '2025-12-31',
        'wfm_proc_colab': None,
    }
}

# For algorithm_GD production
ALGORITHM_GD_PROD_CONFIG = {
    'logging': {
        'environment': 'server',
        'db_logging_enabled': True,
        'df_messages_path': 'data/csvs/messages.csv',
        'db_logging_query': 'queries/log_process_errors.sql',
        'server_file_logging': True,
        'retention_days_server': 60,
        'log_level': 'INFO',
        'log_dir': 'logs'
    },
    'external_call_data': {
        'current_process_id': None,  # Set at runtime
        'api_proc_id': 999,
        'wfm_proc_id': None,  # Set at runtime
        'wfm_user': 'WFM',
        'start_date': None,  # Set at runtime
        'end_date': None,    # Set at runtime
        'wfm_proc_colab': None,
    }
}

# Helper function to merge environment-specific config
def get_environment_config(base_config: dict, env_config: dict) -> dict:
    """
    Merge environment-specific configuration with base configuration.
    
    Args:
        base_config: Base configuration dictionary
        env_config: Environment-specific overrides
        
    Returns:
        Merged configuration dictionary
    """
    from base_data_project.utils import merge_configs
    return merge_configs(base_config, env_config)