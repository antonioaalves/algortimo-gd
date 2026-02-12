"""
File containing the base data model class.
"""

# Dependencies
from typing import Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.log_config import get_logger
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.data_manager.managers.managers import CSVDataManager, DBDataManager

# Local stuff
from src.configuration_manager.instance import get_config
from src.helpers import calcular_max

# Get configuration singleton
_config = get_config()
project_name = _config.project_name
root_dir = _config.system.project_root_dir
from src.data_models.functions.loading_functions import load_valid_emp_csv
from src.data_models.functions.helper_functions import (
    count_dates_per_year,
    convert_types_out,
    bulk_insert_with_query,
    filter_insert_results,
    get_df_faixa_horario,
)
from src.data_models.functions.data_treatment_functions import (
    adjust_estimativas_special_days,
    filter_df_dates,
    add_date_related_columns,
    create_df_estimativas,
    add_pessoa_obj_whole_day,
    treat_df_orcamento,
)
from src.data_models.validations.load_process_data_validations import (
    validate_posto_id,
)
from src.algorithms.factory import AlgorithmFactory


# Set up logger
logger = get_logger(project_name)


class BaseDescansosDataModel(ABC):
    """
    Base class for all data models.
    """
    def __init__(self, data_container: Optional[BaseDataContainer] = None, project_name: str = 'base_data_project'):
        """
        Initialize the data model.
        
        Args:
            data_container: Optional data container to initialize with
            project_name: Project name for logging
        """
        self.project_name = project_name
        self.logger = get_logger(project_name)
        
        ## Create default data container with project name if none provided
        #if data_container is None:
        #    data_container = BaseDataContainer(project_name=project_name, config=config)
            
        self.data_container = data_container
        
        self.logger.info(f"Initialized {self.__class__.__name__} with project {project_name}")
    

    @abstractmethod
    def load_process_data():
        """
        """
        pass

    @abstractmethod
    def treat_params(self):
        """
        """
        pass

    @abstractmethod
    def load_colaborador_info(self):
        """
        """
        pass

    @abstractmethod
    def load_calendario_info():
        """
        """
        pass

    @abstractmethod
    def load_calendario_transformations(self):
        """
        """
        pass
    
    # TODO: put the estimativas methods
    def load_estimativas_info(self, data_manager: BaseDataManager, posto_id: int = 0, start_date: str = '', end_date: str = '') -> Tuple[bool, str, str]:
        """
        Load necessities from data manager and treat them data
        """
        try:
            self.logger.info(f"Starting load_estimativas_info method.")
            # Get current_posto_id from auxiliary_data
            try:
                posto_id = self.auxiliary_data['current_posto_id']
                df_estrutura_wfm = self.auxiliary_data['df_estrutura_wfm'].copy()
                self.logger.info(f"Loaded information from auxiliary_data into posto_id: {posto_id}")
            except Exception as e:
                self.logger.error(f"Error getting posto_id from auxiliary_data: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Validate input parameters
            if not validate_posto_id(posto_id):
                self.logger.error(f"posto_id provided is invalid: {posto_id}")
                return False, "errSubproc", "Invalid posto_id"

            # Get first_date_passado and last_date_passado from auxiliary_data (same extended range as df_calendario)
            try:
                first_date_passado = self.auxiliary_data.get('first_date_passado')
                last_date_passado = self.auxiliary_data.get('last_date_passado')
                if not first_date_passado or not last_date_passado:
                    self.logger.warning("first_date_passado or last_date_passado not found in auxiliary_data, falling back to start_date/end_date")
                    first_date_passado = start_date
                    last_date_passado = end_date
                else:
                    self.logger.info(f"Using extended date range from df_calendario: first_date_passado={first_date_passado}, last_date_passado={last_date_passado}")
            except Exception as e:
                self.logger.warning(f"Error getting passado dates from auxiliary_data: {e}, falling back to start_date/end_date")
                first_date_passado = start_date
                last_date_passado = end_date

            # TODO: create validate function
            if start_date == '' or start_date == None or end_date == '' or end_date == None:
                self.logger.error(f"start_date or end_date provided are empty. start: {start_date}, end_date: {end_date}")
                return False, "errSubproc", "Invalid date parameters"

            try:
                # TODO: Review this
                self.logger.info("Initializing df_estimativas as an empty dataframe")
                # df_estimativas borns as an empty dataframe
                df_estimativas = pd.DataFrame()

                columns_select = ['nome', 'matricula', 'employee_id', 'fk_tipo_posto', 'loja', 'secao', 'limite_superior_manha', 'limite_inferior_tarde']
                self.logger.info(f"Columns to select: {columns_select}")
            except Exception as e:
                self.logger.error(f"Error initializing estimativas info: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info(f"Loading df_turnos from raw_data df_colaborador with columns selected: {columns_select}")
                # Get sql file path, if the cvs is being used, it gets the the path defined on dummy_data_filepaths
                # turnos information: doesnt need a query since is information resent on core_alg_params
                # Filter by posto_id BEFORE extracting columns for better performance
                df_colaborador = self.raw_data['df_colaborador'].copy()
                # If df_valid_emp is available, use it to filter employees by posto_id early
                if 'df_valid_emp' in self.auxiliary_data and self.auxiliary_data['df_valid_emp'] is not None:
                    df_valid_emp = self.auxiliary_data['df_valid_emp']
                    if not df_valid_emp.empty and 'fk_tipo_posto' in df_valid_emp.columns and 'employee_id' in df_valid_emp.columns:
                        employees_id_list_from_posto = df_valid_emp[df_valid_emp['fk_tipo_posto'] == posto_id]['employee_id']
                        if 'employee_id' in df_colaborador.columns:
                            df_colaborador = df_colaborador[df_colaborador['employee_id'].isin(employees_id_list_from_posto)].copy()
                            self.logger.info(f"Pre-filtered df_colaborador by posto_id={posto_id}, shape: {df_colaborador.shape}")
                
                df_turnos = df_colaborador[columns_select].copy()
                self.logger.info(f"df_turnos shape (rows {df_turnos.shape[0]}, columns {df_turnos.shape[1]}): {df_turnos.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error processing df_turnos from colaborador data: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info("Getting df_feriados from auxiliary_data (already loaded and treated)")
                # Use already-loaded and treated df_feriados from auxiliary_data
                df_feriados = self.auxiliary_data.get('df_feriados', pd.DataFrame())
                if df_feriados.empty:
                    self.logger.warning("df_feriados not found in auxiliary_data or is empty")
                else:
                    self.logger.info(f"df_feriados shape (rows {df_feriados.shape[0]}, columns {df_feriados.shape[1]}): {df_feriados.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error getting df_feriados from auxiliary_data: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info("Loading df_orcamento from data manager")
                # orcamento information
                query_path = _config.paths.sql_auxiliary_paths.get('df_orcamento', '')
                if query_path == '':
                    self.logger.warning("df_orcamento query path not found in config")
                # Use extended date range (first_date_passado to last_date_passado) like df_calendario
                start_date_quoted = "'" + first_date_passado + "'"
                end_date_quoted = "'" + last_date_passado + "'"
                df_orcamento = data_manager.load_data(
                    'df_orcamento', 
                    query_file=query_path, 
                    posto_id=posto_id, 
                    start_date=start_date_quoted, 
                    end_date=end_date_quoted
                )
                self.logger.info(f"df_orcamento shape (rows {df_orcamento.shape[0]}, columns {df_orcamento.shape[1]}): {df_orcamento.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_orcamento: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            success, df_orcamento, error_msg = treat_df_orcamento(df_orcamento)
            if not success:
                self.logger.error(f"Closed days treatment failed: {error_msg}")
                return False, "errSubproc", error_msg

            # df_faixa_horario from get_df_faixa_horario (df_orcamento + optional df_faixa_secao fallback from load_process_data)
            try:
                df_faixa_secao = self.auxiliary_data.get('df_faixa_secao', pd.DataFrame())
                if df_faixa_secao.empty:
                    self.logger.info("df_faixa_secao not in auxiliary_data or empty; get_df_faixa_horario will use internal fallback if needed")
                success, df_faixa_horario, error_msg = get_df_faixa_horario(
                    df_orcamento=df_orcamento,
                    df_turnos=df_turnos,
                    use_case=1,
                    df_faixa_secao=df_faixa_secao if not df_faixa_secao.empty else None,
                )
                if not success:
                    self.logger.error(f"Failed to get df_faixa_horario: {error_msg}")
                    return False, "errSubproc", error_msg
                self.logger.info(f"df_faixa_horario shape (rows {df_faixa_horario.shape[0]}, columns {df_faixa_horario.shape[1]}): {df_faixa_horario.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error getting df_faixa_horario: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info("Loading df_granularidade from data manager")
                # granularidade information
                query_path = _config.paths.sql_auxiliary_paths.get('df_granularidade', '')
                if query_path == '':
                    self.logger.warning("df_granularidade query path not found in config")
                # Use extended date range (first_date_passado to last_date_passado) like df_calendario
                start_date_quoted = "'" + first_date_passado + "'"
                end_date_quoted = "'" + last_date_passado + "'"
                df_granularidade = data_manager.load_data(
                    'df_granularidade', 
                    query_file=query_path, 
                    start_date=start_date_quoted, 
                    end_date=end_date_quoted, 
                    posto_id=posto_id
                )
                self.logger.info(f"df_granularidade shape (rows {df_granularidade.shape[0]}, columns {df_granularidade.shape[1]}): {df_granularidade.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_granularidade: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info("Saving dataframes to auxiliary_data and raw_data")
                # TODO: save the dataframes if they are needed elsewhere, if not let them die here
                self.raw_data['df_estimativas'] = df_estimativas.copy()
                self.auxiliary_data['df_turnos'] = df_turnos.copy()
                self.auxiliary_data['df_faixa_horario'] = df_faixa_horario.copy()
                self.auxiliary_data['df_orcamento'] = df_orcamento.copy()
                self.auxiliary_data['df_granularidade'] = df_granularidade.copy()
                
                if not self.auxiliary_data:
                    self.logger.warning("No data was loaded into auxiliary_data")
                    return False, "errSubproc", "No data loaded into auxiliary_data"
                    
                self.logger.info(f"load_estimativas_info completed successfully.")
                return True, "", ""
            except KeyError as e:
                self.logger.error(f"KeyError when saving dataframes: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except ValueError as e:
                self.logger.error(f"ValueError when saving dataframes: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except Exception as e:
                self.logger.error(f"Error saving dataframes to auxiliary_data and raw_data: {e}", exc_info=True)
                return False, "errSubproc", str(e)
        
        except Exception as e:
            self.logger.error(f"Error in load_estimativas_info method: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_estimativas_info(self) -> tuple[bool, list[str]]:
        """Function to validate the estimativas info"""
        try:
            self.logger.info("Starting validate_estimativas_info processing")
            validation_success = True
            invalid_entities = []
            # Get estimativas info data
            # Not needed since df_estimativas is empty
            #try:
            #    df_estimativas = self.raw_data['df_estimativas'].copy()
            #except KeyError as e:
            #    self.logger.error(f"Missing required DataFrame in validate_estimativas_info: {e}", exc_info=True)
            #    validation_success = False
            #    invalid_entities.append('df_estimativas')
            #except Exception as e:
            #    self.logger.error(f"Error loading data in validate_estimativas_info: {e}", exc_info=True)
            #    validation_success = False
            #    invalid_entities.append('df_estimativas')

            ## Validate estimativas info
            #if df_estimativas.empty or len(df_estimativas) < 1:
            #    self.logger.error(f"df_estimativas is empty. df_estimativas shape: {df_estimativas.shape}")
            #    validation_success = False
            #    invalid_entities.append('df_estimativas')

            # Validate granularidade info
            try:
                df_granularidade = self.auxiliary_data['df_granularidade'].copy()
            except KeyError as e:
                self.logger.error(f"Missing required DataFrame in validate_estimativas_info: {e}", exc_info=True)
                validation_success = False
                invalid_entities.append('df_granularidade')
            except Exception as e:
                self.logger.error(f"Error loading data in validate_estimativas_info: {e}", exc_info=True)
                validation_success = False
                invalid_entities.append('df_granularidade')

            # Validate granularidade info
            if df_granularidade.empty or len(df_granularidade) < 1:
                self.logger.error(f"df_granularidade is empty. df_granularidade shape: {df_granularidade.shape}")
                validation_success = False
                invalid_entities.append('df_granularidade')

            # Validate df_faixa_horario info
            try:
                df_faixa_horario = self.auxiliary_data['df_faixa_horario'].copy()
            except KeyError as e:
                self.logger.error(f"Missing required df_faixa_horario DataFrame in auxiliary_data: {e}", exc_info=True)
                validation_success = False
                invalid_entities.append('df_faixa_horario')
            except Exception as e:
                self.logger.error(f"Error loading data in validate_estimativas_info: {e}", exc_info=True)
                validation_success = False
                invalid_entities.append('df_faixa_horario')

            if df_faixa_horario.empty or len(df_faixa_horario) < 1:
                self.logger.error(f"df_faixa_horario is empty. df_faixa_horario shape: {df_faixa_horario.shape}")
                validation_success = False
                invalid_entities.append('df_faixa_horario')

            # Overall validation result return
            if not validation_success:
                self.logger.error(f"Invalid entities: {invalid_entities}")
                return False, invalid_entities
            # If validation is successful, return True and an empty list of invalid entities
            return True, []

        except Exception as e:
            self.logger.error(f"Error in validate_estimativas_info method: {e}", exc_info=True)
            return False, []

    def load_estimativas_transformations(self) -> Tuple[bool, str, str]:
        """
        Convert R output_turnos function to Python.
        Process shift/schedule data and calculate shift statistics.
        
        Stores results in:
        - auxiliary_data['df_turnos']: Processed shift data
        - raw_data['df_estimativas']: Final output matrix (matrizB_og equivalent)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # TODO: understand if this is the best way to log with the class name
            #logger = logging.getLogger(self.__class__.__name__)
            self.logger.info("Starting load_estimativas_transformations")
            
            try:
                self.logger.info("Extracting parameters from auxiliary_data and external_data")
                # Get parameters from auxiliary_data and external_data
                start_date = self.external_call_data['start_date']
                end_date = self.external_call_data['end_date']
                # Get extended date range (same as df_calendario) for data processing
                first_date_passado = self.auxiliary_data.get('first_date_passado', start_date)
                last_date_passado = self.auxiliary_data.get('last_date_passado', end_date)
                fk_unidade = self.auxiliary_data['unit_id']
                fk_secao = self.auxiliary_data['secao_id'] 
                fk_tipo_posto = self.auxiliary_data['current_posto_id']
                
                self.logger.info(f"Parameters extracted - start_date: {start_date}, end_date: {end_date}")
                self.logger.info(f"Using extended date range for processing - first_date_passado: {first_date_passado}, last_date_passado: {last_date_passado}")
                self.logger.info(f"Other params - fk_unidade: {fk_unidade}, fk_secao: {fk_secao}, fk_tipo_posto: {fk_tipo_posto}")
            except KeyError as e:
                self.logger.error(f"Missing required parameter: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error extracting parameters: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Loading DataFrames from existing data")
                # Get DataFrames from existing data (df_faixa_horario was computed in load_estimativas_info)
                df_turnos = self.auxiliary_data['df_turnos'].copy()
                df_feriados = self.auxiliary_data['df_feriados'].copy()
                df_orcamento = self.auxiliary_data['df_orcamento'].copy()  # This is dfGranularidade equivalent - TODO: check if this is needed
                df_faixa_horario = self.auxiliary_data['df_faixa_horario'].copy()
                
                self.logger.info(f"DataFrames loaded - df_turnos: {df_turnos.shape}, df_feriados: {df_feriados.shape}, df_orcamento: {df_orcamento.shape}, df_faixa_horario: {df_faixa_horario.shape}")
            except KeyError as e:
                self.logger.error(f"Missing required DataFrame: {e}", exc_info=True)
                return False, "", ""
            except Exception as e:
                self.logger.error(f"Error loading DataFrames: {e}", exc_info=True)
                return False, "", ""
            
            # df_faixa_horario already in auxiliary_data (computed in load_estimativas_info with df_faixa_secao fallback)

            # Create df_estimativas from df_orcamento by aggregating per day/shift
            # M shift: hora_inicio_faixa <= hora_ini < limite_superior_manha
            # T shift: limite_inferior_tarde <= hora_ini < hora_fim_faixa
            # Overlap between shifts is intentional - whole day stats reconcile this
            try:
                success, df_estimativas, error_msg = create_df_estimativas(
                    df_orcamento=df_orcamento,
                    df_faixa_horario=df_faixa_horario,
                    use_case=0
                )
                if not success:
                    self.logger.error(f"Failed to create df_estimativas: {error_msg}")
                    return False, "", ""
            except Exception as e:
                self.logger.error(f"Error creating df_estimativas: {e}", exc_info=True)
                return False, "", ""
            
            # Add whole day statistics (media_dia, max_dia, min_dia, sd_dia)
            try:
                success, df_estimativas, error_msg = add_pessoa_obj_whole_day(
                    df_estimativas=df_estimativas,
                    df_orcamento=df_orcamento
                )
                if not success:
                    self.logger.error(f"Failed to add pessoa_obj_whole_day: {error_msg}")
                    return False, "", ""
            except Exception as e:
                self.logger.error(f"Error adding pessoa_obj_whole_day: {e}", exc_info=True)
                return False, "", ""


            # Filter by date range (Step 3B from func_inicializa guide)
            # Use extended date range (first_date_passado to last_date_passado) to match df_calendario
            # This ensures the algorithm component has the whole period for both dataframes
            try:
                if not df_estimativas.empty:
                    self.logger.info(f"DEBUG: df_estimativas columns: {df_estimativas.columns.tolist()}")
                    self.logger.info(f"DEBUG: df_estimativas head:\n{df_estimativas.head()}")
                success, df_estimativas, error_msg = filter_df_dates(
                    df=df_estimativas,
                    first_date_str=first_date_passado,
                    last_date_str=last_date_passado,
                    date_col_name='schedule_day',
                    use_case=1  # Filter by extended date range (same as df_calendario should have)
                )
                if not success:
                    self.logger.error(f"Failed to filter estimativas by dates: {error_msg}")
                    return False
            except Exception as e:
                self.logger.error(f"Error filtering estimativas by dates: {e}", exc_info=True)
                return False

            # Adjust for special dates (Step 2 from func_inicializa guide)
            try:
                self.logger.info("Adjusting estimativas for special dates (Christmas/New Year)")
                special_days_list = []  # Empty list = auto-generate from data year
                success, df_estimativas, error_msg = adjust_estimativas_special_days(
                    df_estimativas=df_estimativas, 
                    special_days_list=special_days_list,
                    use_case=0
                )
                if not success:
                    self.logger.error(f"Failed to adjust special dates: {error_msg}")
                    return False
            except Exception as e:
                self.logger.error(f"Error adjusting for special dates: {e}", exc_info=True)
                return False
            
            # Add date-related columns (Step 3G/6E from func_inicializa guide)
            try:
                self.logger.info("Adding date-related columns to estimativas (WDAY, WW, WD)")
                main_year = self.auxiliary_data.get('main_year')
                success, df_estimativas, error_msg = add_date_related_columns(
                    df=df_estimativas,
                    date_col='schedule_day',
                    add_id_col=False,
                    use_case=1,
                    main_year=main_year,
                    first_date=first_date_passado,
                    last_date=last_date_passado
                )
                if not success:
                    self.logger.error(f"Failed to add date-related columns: {error_msg}")
                    return False
            except Exception as e:
                self.logger.error(f"Error adding date-related columns: {e}", exc_info=True)
                return False                
            
            try:
                self.logger.info("Storing results in appropriate class attributes")
                # Store processed turnos data in auxiliary_data
                #self.auxiliary_data['df_turnos'] = df_turnos_processing.copy()
                #self.auxiliary_data['df_feriados_filtered'] = df_feriados_filtered.copy()
                
                # Store final output matrix (matrizB_og equivalent) in raw_data
                self.raw_data['df_estimativas'] = df_estimativas.copy()
                
                if not self.auxiliary_data or not self.raw_data:
                    self.logger.warning("Data storage verification failed")
                    return False
                    
                self.logger.info("load_estimativas_transformations completed successfully")
                self.logger.info(f"Stored df_estimativas (matrizB_og) with shape: {df_estimativas.shape}")
                
                return True
            except KeyError as e:
                self.logger.error(f"KeyError when storing results: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError when storing results: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error storing results in load_estimativas_transformations: {e}", exc_info=True)
                return False
            
        except Exception as e:
            self.logger.error(f"Error in load_estimativas_transformations method: {e}", exc_info=True)
            return False

    def allocation_cycle(self, algorithm_name: str, algorithm_params: Dict[str, Any]) -> bool:
        """
        Method responsible for running the defined algorithms.
        Args:
            algorithm_name: The algorithm name to be used (e.g., 'alcampo_algorithm')
            algorithm_params: Dictionary of parameters for the algorithms.
        """

        try:
            self.logger.info(f"Starting allocation_cycle processing")
            
            try:
                self.logger.info("Validating input parameters for allocation cycle")
                if not algorithm_name or not isinstance(algorithm_name, str):
                    self.logger.error(f"Invalid algorithm_name parameter: {algorithm_name}, type: {type(algorithm_name)}")
                    return False
                    
                if algorithm_params is None or not isinstance(algorithm_params, dict):
                    self.logger.error(f"Invalid algorithm_params parameter: {algorithm_params}, type: {type(algorithm_params)}")
                    return False
                    
                self.logger.info(f"Input parameters validated - algorithm_name: {algorithm_name}, algorithm_params keys: {list(algorithm_params.keys())}")
            except Exception as e:
                self.logger.error(f"Error validating allocation cycle input parameters: {e}", exc_info=True)
                return False

            try:
                self.logger.info(f"Creating algorithm instance for: {algorithm_name}")
                algorithm = AlgorithmFactory.create_algorithm(
                    decision=algorithm_name,
                    parameters=algorithm_params,
                    process_id=self.external_call_data.get("current_process_id", 0),
                    start_date=self.external_call_data.get("start_date", ''),
                    end_date=self.external_call_data.get("end_date", '')
                )

                if not algorithm:
                    self.logger.error(f"Algorithm {algorithm_name} not found or could not be created.")
                    return False
                    
                self.logger.info(f"Algorithm {algorithm_name} created successfully")
            except Exception as e:
                self.logger.error(f"Error creating algorithm {algorithm_name}: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Validating medium_data before running algorithm")
                if not self.medium_data:
                    self.logger.error("medium_data is empty or not available for algorithm execution")
                    return False
                    
                required_keys = ['df_colaborador', 'df_calendario', 'df_estimativas']
                missing_keys = [key for key in required_keys if key not in self.medium_data]
                if missing_keys:
                    self.logger.error(f"Missing required keys in medium_data: {missing_keys}")
                    return False
                    
                self.logger.info(f"medium_data validated with keys: {list(self.medium_data.keys())}")
            except Exception as e:
                self.logger.error(f"Error validating medium_data: {e}", exc_info=True)
                return False

            try:
                self.logger.info(f"Running algorithm {algorithm_name}")
                self.logger.info(f"algorithm_treatment_params: {self.algorithm_treatment_params}")
                results = algorithm.run(data=self.medium_data, algorithm_treatment_params=self.algorithm_treatment_params)

                if not results:
                    self.logger.error(f"Algorithm {algorithm_name} returned no results.")
                    return False

                if results.get('summary', {}).get('status') == 'failed':  # Fixed: was checking for 'completed' which is wrong
                    self.logger.error(f"Algorithm {algorithm_name} failed to run. Status: {results.get('summary', {}).get('status')}")
                    return False

                #self.logger.info(f"DEBUG: results: {results}")

                    
                self.logger.info(f"Algorithm {algorithm_name} executed successfully with status: {results.get('status')}")
            except Exception as e:
                self.logger.error(f"Error running algorithm {algorithm_name}: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Storing algorithm results in rare_data")
                #self.rare_data['stage1_schedule'] = pd.DataFrame(results.get('stage1_schedule', pd.DataFrame())) # TODO: define in the algorithm how the results come
                #self.rare_data['stage2_schedule'] = pd.DataFrame(results.get('stage2_schedule', pd.DataFrame())) # TODO: define in the algorithm how the results come
                # TODO: add more data to rare_data if needed
                
                if not self.rare_data:
                    self.logger.warning("rare_data storage verification failed")
                    return False

                self.rare_data['df_results'] = results.get('core_results', {}).get('formatted_schedule', pd.DataFrame())
                #self.logger.info(f"DEBUG: df_results: {self.rare_data['df_results']}")
                
                # Save CSV file for debugging (using config manager if available)
                try:
                    if hasattr(self, 'config_manager') and hasattr(self.config_manager, 'paths'):
                        output_dir = self.config_manager.paths.get_output_dir()
                    else:
                        # Fallback to hardcoded path if config_manager not available
                        output_dir = os.path.join(root_dir, 'data', 'output')
                    
                    process_id = self.external_call_data.get("current_process_id", "")
                    posto_id = self.auxiliary_data.get("current_posto_id", "")
                    self.rare_data['df_results'].to_csv(
                        os.path.join(output_dir, f'df_results-{process_id}-{posto_id}.csv'),
                        index=False,
                        encoding='utf-8'
                    )
                except Exception as csv_error:
                    self.logger.warning(f"Failed to save df_results CSV file: {csv_error}")
                    
                results_df = self.rare_data.get('df_results', {})
                #with open(os.path.join('data', 'output', f'df_results-{self.external_call_data.get("current_process_id", "")}.json'), 'w', encoding='utf-8') as f:
                #    json.dump(results_df, f, indent=2, ensure_ascii=False)
                self.logger.info(f"Results stored - df_results shape: {results_df.shape if results_df is not None else 'None'}")
                self.logger.info(f"Allocation cycle completed successfully with algorithm {algorithm_name}.")
                return True
            except KeyError as e:
                self.logger.error(f"KeyError when storing allocation cycle results: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError when storing allocation cycle results: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error storing algorithm results: {e}", exc_info=True)
                return False
        
        except Exception as e:
            self.logger.error(f"Error in allocation_cycle method: {e}", exc_info=True)
            return False

    def validate_allocation_cycle(self) -> bool:
        """
        Validates func_inicializa operations. Validates data before running the allocation cycle.
        """
        # TODO: Where should it be? here or after formatting results
        try:
            # TODO: Implement validation logic
            self.logger.info("Entered func_inicializa validation. Needs to be implemented.")
            return True
        except Exception as e:
            self.logger.error(f"Error validating allocation_cycle from data manager: {str(e)}")
            return False

    def format_results(self) -> bool:
        """
        Method responsible for formatting results before inserting.
        It is a little bit of a mess, but it works. The purpose of this method is to prepare the final data frame for insertion
        """
        try:
            self.logger.info("Entered format_results method.")
            final_df = self.rare_data['df_results'].copy()
            df_colaborador = self.medium_data['df_colaborador'].copy()
            self.logger.info(f"DEBUG: df_colaborador: {df_colaborador}")
            df_colaborador = df_colaborador[['employee_id', 'matricula', 'data_admissao', 'data_demissao']]

            # Adding validation to fall gracefully
            if final_df.empty:
                self.logger.error("final_df is empty")
                return False
            if final_df.columns.empty:
                self.logger.error("final_df has no columns")
                return False
            if len(final_df) == 0:
                self.logger.error("final_df has 0 rows")
                return False
            if df_colaborador.empty:
                self.logger.error("df_colaborador is empty")
                return False

            self.logger.info(f"Adding wfm_proc_id to final_df")
            final_df['wfm_proc_id'] = self.external_call_data.get("current_process_id", "")
            #self.logger.info(f"DEBUG: final_df: {final_df}")

            self.logger.info("Merging with df_colaborador")
            # The colaborador column contains employee_id values, so merge on employee_id
            # Convert colaborador to int to ensure proper matching
            final_df['colaborador'] = final_df['colaborador'].astype(int)
            df_colaborador['employee_id'] = df_colaborador['employee_id'].astype(int)
            
            # Log merge info for debugging
            self.logger.info(f"DEBUG: final_df colaborador unique values (first 10): {final_df['colaborador'].unique()[:10]}")
            self.logger.info(f"DEBUG: df_colaborador employee_id unique values (first 10): {df_colaborador['employee_id'].unique()[:10]}")
            
            final_df = pd.merge(final_df, df_colaborador, left_on='colaborador', right_on='employee_id', how='left')
            
            # Ensure matricula column is present using employee_id_matriculas_map
            # The merge should bring matricula from df_colaborador, but we use the map to ensure it's complete
            if hasattr(self, 'auxiliary_data') and 'employee_id_matriculas_map' in self.auxiliary_data:
                employee_id_matriculas_map = self.auxiliary_data['employee_id_matriculas_map']
                self.logger.info("Ensuring matricula column is complete using employee_id_matriculas_map")
                # Map employee_id (colaborador) to matricula - this will fill any missing or override if needed
                final_df['matricula'] = final_df['colaborador'].map(employee_id_matriculas_map)
                # Fill any missing values from the merge result if available
                if 'matricula' in df_colaborador.columns:
                    merge_matricula = df_colaborador.set_index('employee_id')['matricula']
                    missing_mask = final_df['matricula'].isna()
                    if missing_mask.any():
                        final_df.loc[missing_mask, 'matricula'] = final_df.loc[missing_mask, 'colaborador'].map(merge_matricula)
                missing_matriculas = final_df['matricula'].isna().sum()
                if missing_matriculas > 0:
                    self.logger.warning(f"Could not map matricula for {missing_matriculas} rows using employee_id_matriculas_map")
            elif 'matricula' not in final_df.columns:
                self.logger.error("matricula column not found after merge and employee_id_matriculas_map not available")
            
            # Check merge success
            unmatched_rows = final_df['data_admissao'].isna().sum()
            if unmatched_rows > 0:
                self.logger.warning(f"Merge resulted in {unmatched_rows} rows with missing data_admissao (out of {len(final_df)} total rows)")
                self.logger.warning(f"Unmatched colaborador values (first 10): {final_df[final_df['data_admissao'].isna()]['colaborador'].unique()[:10]}")
            
            #self.logger.info(f"DEBUG: final_df: {final_df}")
            
            # Filter by process date range and employee (if specified)
            self.logger.info("Filtering by process date range and employee (if specified)")
            start_date = self.external_call_data.get("start_date", "")
            end_date = self.external_call_data.get("end_date", "")
            wfm_proc_colab = self.external_call_data.get("wfm_proc_colab", "")
            
            if start_date and end_date:
                final_df = filter_insert_results(final_df, start_date, end_date, wfm_proc_colab)
            else:
                self.logger.warning("start_date or end_date not found in external_call_data, skipping date range filter")
            
            self.logger.info("Filtering dates greater than each employee's admission date")
            initial_rows = len(final_df)
            # Convert data_admissao to datetime if not already
            #self.logger.info(f"DEBUG: data_admissao: {final_df['data_admissao']}, type: {type(final_df['data_admissao'])}")
            final_df['data_admissao'] = pd.to_datetime(final_df['data_admissao'], format='%Y-%m-%d', errors='coerce')
            
            # Handle data_demissao: replace "0" or non-date values with NaT before conversion
            final_df['data_demissao'] = final_df['data_demissao'].replace(['0', 0], pd.NaT)
            final_df['data_demissao'] = pd.to_datetime(final_df['data_demissao'], format='%Y-%m-%d', errors='coerce')
            #self.logger.info(f"DEBUG: data_admissao: {final_df['data_admissao']}, type: {type(final_df['data_admissao'])}")
            
            # First, filter out rows where data_admissao is NaT (unmatched from merge)
            rows_before_na_filter = len(final_df)
            final_df = final_df[final_df['data_admissao'].notna()].copy()
            rows_after_na_filter = len(final_df)
            if rows_before_na_filter != rows_after_na_filter:
                self.logger.warning(f"Filtered out {rows_before_na_filter - rows_after_na_filter} rows with missing data_admissao (unmatched from merge)")
            
            # Filter per employee: each row's date must be >= that employee's admission date
            if 'data' in final_df.columns:
                final_df['data'] = pd.to_datetime(final_df['data'])
                rows_before_date_filter = len(final_df)
                final_df = final_df[final_df['data'] >= final_df['data_admissao']].copy()
                rows_after_date_filter = len(final_df)
                if rows_before_date_filter != rows_after_date_filter:
                    self.logger.info(f"Filtered out {rows_before_date_filter - rows_after_date_filter} rows where date < data_admissao")
                #self.logger.info(f"DEBUG: data: {final_df['data']}, type: {type(final_df['data'])}")
            elif 'date' in final_df.columns:
                final_df['date'] = pd.to_datetime(final_df['date'])
                rows_before_date_filter = len(final_df)
                final_df = final_df[final_df['date'] >= final_df['data_admissao']].copy()
                rows_after_date_filter = len(final_df)
                if rows_before_date_filter != rows_after_date_filter:
                    self.logger.info(f"Filtered out {rows_before_date_filter - rows_after_date_filter} rows where date < data_admissao")
                #self.logger.info(f"DEBUG: date: {final_df['date']}, type: {type(final_df['date'])}")

            # Filter per employee: each row's date must be < that employee's demission date (only if demission date exists)
            if 'data' in final_df.columns:
                final_df['data'] = pd.to_datetime(final_df['data'])
                # Only filter if data_demissao is not NaT (employee has been terminated)
                final_df = final_df[(final_df['data_demissao'].isna()) | (final_df['data'] <= final_df['data_demissao'])].copy()
                #self.logger.info(f"DEBUG: data: {final_df['data']}, type: {type(final_df['data'])}")
            elif 'date' in final_df.columns:
                final_df['date'] = pd.to_datetime(final_df['date'])
                #self.logger.info(f"DEBUG: date: {final_df['date']}, type: {type(final_df['date'])}")
                # Only filter if data_demissao is not NaT (employee has been terminated)
                final_df = final_df[(final_df['data_demissao'].isna()) | (final_df['date'] <= final_df['data_demissao'])].copy()
            
            filtered_rows = len(final_df)
            self.logger.info(f"Filtered {initial_rows - filtered_rows} rows (from {initial_rows} to {filtered_rows}) based on admission dates")

            #self.logger.info(f"DEBUG: final_df: {final_df}")

            self.logger.info("Converting types out")
            final_df = convert_types_out(pd.DataFrame(final_df))
            #self.logger.info(f"DEBUG: final_df:\n {final_df}")
            # Drop columns that exist (matricula_int may not exist if merge used employee_id)
            # Keep matricula for insertion - we'll create colaborador from it
            cols_to_drop = ['horario', 'employee_id', 'data_admissao', 'colaborador']
            cols_to_drop = [col for col in cols_to_drop if col in final_df.columns]
            final_df = final_df.drop(columns=cols_to_drop)
            
            # Rename matricula to colaborador for final output, but keep matricula for insertion
            if 'matricula' in final_df.columns:
                # Create colaborador from matricula for output format, but keep matricula for insertion
                final_df['colaborador'] = final_df['matricula'].copy()
                final_df['colaborador'] = final_df['colaborador'].astype(str)
            else:
                self.logger.error("matricula column not found after merge and mapping - insertion may fail")
            
            # Save CSV file for debugging (using config manager if available)
            try:
                if hasattr(self, 'config_manager') and hasattr(self.config_manager, 'paths'):
                    output_dir = self.config_manager.paths.get_output_dir()
                else:
                    # Fallback to hardcoded path if config_manager not available
                    output_dir = os.path.join(root_dir, 'data', 'output')
                
                process_id = self.external_call_data.get("current_process_id", "")
                posto_id = self.auxiliary_data.get("current_posto_id", "")
                final_df.to_csv(
                    os.path.join(output_dir, f'df_insert_results-{process_id}-{posto_id}.csv'),
                    index=False,
                    encoding='utf-8'
                )
            except Exception as csv_error:
                self.logger.warning(f"Failed to save final_df CSV file: {csv_error}")
            #self.formatted_data['stage1_schedule'].to_csv(os.path.join('data', 'output', f'stage1_schedule-{self.external_call_data.get("current_process_id", "")}.csv'), index=False, encoding='utf-8')
            #self.formatted_data['stage2_schedule'].to_csv(os.path.join('data', 'output', f'stage2_schedule-{self.external_call_data.get("current_process_id", "")}.csv'), index=False, encoding='utf-8')
            self.logger.info("format_results completed successfully. Storing final_df in formatted_data.")
            #self.logger.info(f"DEBUG: final_df:\n {final_df}")
            self.formatted_data['df_final'] = final_df.copy()
            return True            
        except Exception as e:
            self.logger.error(f"Error performing format_results: {str(e)}", exc_info=True) 
            return False

    def validate_format_results(self) -> bool:
        """
        Method responsible for validating formatted results before inserting.
        """
        try:
            self.logger.info("Entered validate_format_results method. Needs to be implemented.")
            return True            
        except Exception as e:
            self.logger.error(f"Error validating format_results from data manager: {str(e)}")
            return False
        
    def insert_results(self, data_manager: BaseDataManager, query_path: str = os.path.join(root_dir, 'src', 'sql_querys', 'insert_results.sql')) -> bool:
        """
        Method for inserting results in the data source.
        """
        try:
            self.logger.info("Entered insert_results method.")
            final_df = self.formatted_data['df_final'].copy()

            # Adding validation to fall gracefully
            if final_df.empty:
                self.logger.error("final_df is empty")
                return False
            if final_df.columns.empty:
                self.logger.error("final_df has no columns")
                return False
            if len(final_df) == 0:
                self.logger.error("final_df has 0 rows")
                return False

            try:
                self.logger.info(f"Changing column names to match insert_results query")
                # Select desired columns
                final_df = final_df[['colaborador', 'data', 'wfm_proc_id', 'sched_type', 'sched_subtype']]
                # Convert data to string format for insertion
                final_df['data'] = final_df['data'].dt.strftime('%Y-%m-%d')
                # Rename columns to match insert_results query
                final_df.rename(columns={'colaborador': 'employee_id', 'data': 'schedule_dt', 'wfm_proc_id': 'fk_processo'}, inplace=True)
            except Exception as e:
                self.logger.error(f"Error treating final_df column names for insertion: {str(e)}")
                return False

            try:
                query_path = self.config_manager.paths.sql_processing_paths['insert_results_df']
                valid_insertion = bulk_insert_with_query(
                    data_manager=data_manager, 
                    data=final_df, 
                    query_file=query_path,
                )
                if not valid_insertion:
                    self.logger.error("Error inserting results")
                    return False
                self.logger.info("Results inserted successfully")
                return True
            except Exception as e:
                self.logger.error(f"Error inserting results with bulk_insert_with_query: {str(e)}", exc_info=True)
                return False

        except Exception as e:
            self.logger.error(f"Error performing insert_results from data manager: {str(e)}", exc_info=True)
            return False

    def validate_insert_results(self, data_manager: BaseDataManager) -> bool:
        """
        Method for validating insertion results.
        """
        try:
            # TODO: Implement validation logic through data source
            self.logger.info("Entered validate_insert_results method. Needs to be implemented.")
            return True
        except Exception as e:
            self.logger.error(f"Error validating insert_results from data manager: {str(e)}")
            return False                    
