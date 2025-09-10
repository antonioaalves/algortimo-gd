"""File containing the data model for Salsa"""

# Dependencies
from typing import Dict, Optional, Any, List
import pandas as pd
import os

# Local stuff
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.data_manager.managers.managers import CSVDataManager, DBDataManager
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.log_config import get_logger
from src.data_models.base import BaseDescansosDataModel
from src.data_models.functions.helper_functions import (
    count_dates_per_year, 
    get_param_for_posto, 
    load_wfm_scheds, 
    get_first_and_last_day_passado,
)
from src.data_models.functions.data_treatment_functions import (
    treat_df_valid_emp, 
    treat_df_closed_days, 
    treat_df_calendario_passado,
    create_df_calendario,
    add_calendario_passado,
    add_ausencias_ferias,
    add_folgas_ciclos,
    add_ciclos_90,
    add_days_off,
)
from src.data_models.functions.loading_functions import load_valid_emp_csv
from src.data_models.validations.load_process_data_validations import (
    validate_parameters_cfg, 
    validate_employees_id_list, 
    validate_posto_id_list, 
    validate_posto_id,
    validate_df_valid_emp,
)
from src.config import PROJECT_NAME, ROOT_DIR, CONFIG

# Set up logger
logger = get_logger(PROJECT_NAME)

class SalsaDataModel(BaseDescansosDataModel):
    """"""

    def __init__(self, data_container: BaseDataContainer, project_name: str = PROJECT_NAME, external_data: Dict[str, Any] = CONFIG.get('defaults_external_data', {})):
        """Initialize the DescansosDataModel with data dictionaries for storing dataframes.
        
        Args:
            data_container: Container for storing intermediate data
            project_name: Name of the project
            external_data: External data dictionary with process parameters
            
        Data Structures:
            raw_data: Dictionary containing raw dataframes loaded from queries
                - df_calendario: Calendar information from database
                - df_colaborador: Employee information from database
                - df_estimativas: Workload estimates from database
                
            auxiliary_data: Dictionary containing processed/intermediate dataframes
                - df_messages: DataFrame for messages
                - final: Final data (TODO: rename)
                - num_fer_doms: Number of holidays and Sundays
                - df_params: Algorithm parameters
                - params_lq: LQ parameters
                - valid_emp: Valid employees filtered for processing
                - colabs_id_list: List of collaborator IDs
                - convenio: Convention information
                - unit_id: Unit ID
                - secao_id: Section ID
                - posto_id_list: List of posto IDs
                - current_posto_id: Current posto ID
                - df_festivos: Holiday information
                - df_closed_days: Closed days information
                - df_turnos: Shift information
                - df_calendario_passado: Past calendar information
                - df_day_aloc: Day allocation information
                - emp_pre_ger: Pre-generated employee information
                - df_count: Count information
                
            medium_data: Dictionary containing transformed data for algorithm input
                - df_calendario: Transformed calendar data
                - df_colaborador: Transformed employee data
                - df_estimativas: Transformed estimates data
                
            formatted_data: Dictionary containing final results
                - df_final: Final output DataFrame
        """
        super().__init__(data_container=data_container, project_name=project_name)
        # Static data, doesn't change during the process run but are essential for data model treatments - See data lifecycle to understand what this data is
        self.auxiliary_data = {
            'df_messages': pd.DataFrame(), # df containing messages to set process errors
            'df_valid_emp': None, # valid employees filtered for processing
            'df_params_lq': None, # LQ parameters
            'df_festivos': None, # holiday information dataframe
            'df_closed_days': None, # closed days information dataframe
            'df_params': None, # algorithm parameters
            'parameters_cfg': None, # parameters configuration
            'unit_id': None, # unit ID
            'secao_id': None, # section ID
            'posto_id_list': None, # list of posto IDs
            'employees_id_list': None, # list of collaborator IDs
            'first_year_date': None, # start date 2
            'last_year_date': None, # end date 2
            'final': None, # TODO: change the name
            'num_fer_doms': 0, # number of feriados and Sundays in the year
            'algorithm_name': None, # algorithm name - now comes from query
            'convenio': None, # convention information
            'current_posto_id': None, # current posto ID
            'df_ausencias_ferias': None, # holidays absences information dataframe
            'df_days_off': None, # days off information dataframe
            'df_turnos': None, # shift information dataframe
            'df_calendario_passado': None, # past calendar information dataframe
            'df_day_aloc': None, # day allocation information dataframe
            'emp_pre_ger': None, # pre-generated employee information dataframe
            'df_count': None, # count information dataframe
        }
        
        # Algorithm treatment params - data to be sent to the algorithm for treatment purposes
        self.algorithm_treatment_params = {
            'admissao_proporcional': None,
        }
        # Data first stage - See data lifecycle to understand what this data is
        self.raw_data: Dict[str, Any] = {
            'df_calendario': None,
            'df_colaborador': None,
            'df_estimativas': None
        }
        # Data second stage - See data lifecycle to understand what this data is
        self.medium_data: Dict[str, Any] = {
            'df_calendario': None,
            'df_colaborador': None,
            'df_estimativas': None
        }
        # Data third stage - See data lifecycle to understand what this data is
        self.rare_data: Dict[str, Any] = {
            'df_results': None,
            'stage1_schedule': None,
            'stage2_schedule': None,
        }
        # Data final stage - See data lifecycle to understand what this data is
        self.formatted_data: Dict[str, Any] = {
            'df_final': None,  # Final output DataFrame
            'stage1_schedule': None,
            'stage2_schedule': None,
        }
        # External call data coming from the product - See data lifecycle to understand what this data is
        self.external_call_data = external_data
        
        self.logger.info("SalsaDescansosDataModel initialized")

    def load_process_data(self, data_manager: BaseDataManager, entities_dict: Dict[str, str]) -> tuple[bool, str, Optional[str]]:
        """
        Load data from the data manager.
        
        Args:
            data_manager: The data manager instance
            enities: list of entities names to load
            
        Returns:
            True if successful, False otherwise
        """
        # Load messages df - CRITICAL for set_process_errors to work
        df_messages = pd.DataFrame()
        # This variable is only initialized because of the type checker
        # In the future it should contain the error message to add to the log
        error_message = None
        try:
            self.logger.info(f"DEBUGGING: Loading df_messages from CSV file")
            messages_path = os.path.join(ROOT_DIR, 'data', 'csvs', 'df_messages.csv')
            df_messages = pd.read_csv(messages_path)
            self.logger.info(f"DEBUGGING: df_messages loaded successfully with {len(df_messages)} rows")
        except Exception as e:
            self.logger.error(f"Error loading df_messages: {e}")
            # Don't return False - continue without df_messages for now
            
        if df_messages.empty:
            self.logger.warning("DEBUGGING: df_messages is empty - set_process_errors will be skipped")
        else:
            self.logger.info(f"DEBUGGING: df_messages has {len(df_messages)} rows - set_process_errors will work")

        try:
            self.logger.info("Loading process data from data manager")

            # Get entities to load from configuration
            if not entities_dict:
                self.logger.warning("No entities passed as argument")
                return False, "errSubproc", 'No entities passed as argument'

            # Load valid_emp
            try:
                self.logger.info(f"Loading df_valid_emp from data manager")
                if isinstance(data_manager, CSVDataManager):
                    self.logger.info(f"Loading df_valid_emp from csv")
                    df_valid_emp = load_valid_emp_csv()
                elif isinstance(data_manager, DBDataManager):
                    # valid emp info
                    self.logger.info(f"Loading df_valid_emp from database")
                    query_path = entities_dict['valid_emp']
                    process_id_str = "'" + str(self.external_call_data['current_process_id']) + "'"
                    df_valid_emp = data_manager.load_data('valid_emp', query_file=query_path, process_id=process_id_str)
                else:
                    self.logger.error(f"No instance found for data_manager: {data_manager.__name__}")

                if validate_df_valid_emp(df_valid_emp):
                    self.logger.error("df_valid_emp is invalid")
                    # TODO: Add set process errors
                    return False, "errNoColab", "df_valid_emp is invalid"
                    
                df_valid_emp = treat_df_valid_emp(df_valid_emp)

                self.logger.info(f"valid_emp shape (rows {df_valid_emp.shape[0]}, columns {df_valid_emp.shape[1]}): {df_valid_emp.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading valid_emp: {e}", exc_info=True)
                return False, "errSubproc", str(e)


            # Load important info into memory
            try:
                self.logger.info(f"Loading important info into memory(unit_id, secao_id, posto_id_list, colabs_id_list, main_year)")
                # Save important this important info to be able to use it on querys
                unit_id = df_valid_emp['fk_unidade'].unique()[0]  # Get first (and only) unique value
                secao_id = df_valid_emp['fk_secao'].unique()[0]   # Get first (and only) unique value
                posto_id_list = df_valid_emp['fk_tipo_posto'].unique().tolist()  # Get list of unique values
                self.logger.info(f"unit_id: {unit_id}, secao_id: {secao_id}, posto_id_list: {posto_id_list} stored in variables")

                if len(df_valid_emp['fk_unidade'].unique()) > 1 or len(df_valid_emp['fk_secao'].unique()) > 1 or len(df_valid_emp['fk_tipo_posto'].unique()) == 0:
                    self.logger.error("More than one fk_secao or fk_unidade associated with the process.")
                    raise ValueError

                # Get colab_ids list
                employees_id_list = df_valid_emp['fk_colaborador'].unique().tolist()
                self.logger.info(f"colabs_id_list: {employees_id_list} stored in variables")

                # Get start and end date
                start_date = self.external_call_data.get('start_date', '')
                end_date = self.external_call_data.get('end_date', '')
                self.logger.info(f"start_date: {start_date}, end_date: {end_date} stored in variables")

                main_year = count_dates_per_year(start_date_str=self.external_call_data.get('start_date', ''), end_date_str=self.external_call_data.get('end_date', ''))
                first_day_passado, last_day_passado = get_first_and_last_day_passado(
                    start_date_str=start_date, 
                    end_date_str=end_date, 
                    main_year=main_year,
                    wfm_proc_colab=self.external_call_data.get('wfm_proc_colab', None)
                )
                self.logger.info(f"main_year: {main_year} stored in variables")
                self.logger.info(f"first_day_passado: {first_day_passado}, last_day_passado: {last_day_passado}")
            except Exception as e:
                self.logger.error(f"Error loading important info into memory(unit_id, secao_id, posto_id_list, employees_id_list, main_year): {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Validate values
            if not validate_employees_id_list(employees_id_list):
                self.logger.error(f"employees_id_list is empty: {employees_id_list}")
                return False, "errSubproc", "employees_id_list is empty"

            if not validate_posto_id_list(posto_id_list):
                self.logger.error(f"posto_id_list is empty: {posto_id_list}")
                return False, "errSubproc", "posto_id_list is empty"

            # Load params_lq
            try:
                self.logger.info(f"Loading df_params_lq from data manager")
                # Logic needed because query cant run against dfs
                if isinstance(data_manager, CSVDataManager):
                    self.logger.info(f"Loading df_params_lq from csv")
                    df_params_lq = data_manager.load_data('params_lq')
                elif isinstance(data_manager, DBDataManager):
                    self.logger.info(f"Loading df_params_lq from database")
                    query_path = entities_dict['params_lq']
                    df_params_lq = data_manager.load_data('params_lq', query_file=query_path)
                else:
                    self.logger.error(f"No instance found for data_manager: {data_manager.__name__}")
                self.logger.info(f"df_params_lq shape (rows {df_params_lq.shape[0]}, columns {df_params_lq.shape[1]}): {df_params_lq.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_params_lq: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Load festivos information
            try:
                self.logger.info(f"Loading df_festivos from data manager")
                # TODO: join the other query and make only one df
                query_path = entities_dict['df_festivos']
                unit_id_str = "'" + str(unit_id) + "'"
                df_festivos = data_manager.load_data('df_festivos', query_file=query_path, unit_id=unit_id_str)
                self.logger.info(f"df_festivos shape (rows {df_festivos.shape[0]}, columns {df_festivos.shape[1]}): {df_festivos.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_festivos: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Load first_year_date and last_year_date
            try:
                self.logger.info(f"Treating first_year_date and last_year_date")
                first_year_date = pd.to_datetime(f"{main_year}-01-01")
                last_year_date = pd.to_datetime(f"{main_year}-12-31")
            except Exception as e:
                self.logger.error(f"Error treating first_year_date and last_year_date: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Load closed days information
            try:
                self.logger.info(f"Loading df_closed_days from data manager")
                query_path = entities_dict['df_closed_days']
                unit_id_str = "'" + str(unit_id) + "'"
                df_closed_days = data_manager.load_data('df_closed_days', query_file=query_path, unit_id=unit_id_str)
                self.logger.info(f"df_closed_days shape (rows {df_closed_days.shape[0]}, columns {df_closed_days.shape[1]}): {df_closed_days.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_closed_days: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            df_closed_days = treat_df_closed_days(df_closed_days, first_year_date, last_year_date)

            if df_closed_days.empty:
                self.logger.error(f"Error treating df_closed_days: df_closed_days is empty")
                return False, "errSubproc", "df_closed_days is empty"

            # Load global parameters - Very important!! This could be done with params_lq query most probably
            try:
                self.logger.info(f"Loading parameters from data manager")
                query_path = entities_dict['df_params']
                unit_id_str = "'" + str(unit_id) + "'"
                df_params = data_manager.load_data('df_params', query_file=query_path, unit_id=unit_id_str)
                self.logger.info(f"df_params shape (rows {df_params.shape[0]}, columns {df_params.shape[1]}): {df_params.columns.tolist()}")
                #self.logger.info(f"DEBUG: df_params {df_params}")
            except Exception as e:
                self.logger.error(f"Error loading parameters: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Load algorithm treatment params
            try:
                self.logger.info(f"Loading algorithm treatment params from data manager")
                query_path = entities_dict['parameters_cfg']
                parameters_cfg = data_manager.load_data('parameters_cfg', query_file=query_path)
                self.logger.info(f"parameters_cfg shape (rows {parameters_cfg.shape[0]}, columns {parameters_cfg.shape[1]}): {parameters_cfg.columns.tolist()}")
                
                # Need to check if parameters_cfg is empty, because it might well be
                if parameters_cfg.empty or len(parameters_cfg) == 0:
                    self.logger.error(f"parameters_cfg is empty")
                    return False, "errSubproc", "parameters_cfg is empty"
                # Store the value to then validate it
                parameters_cfg = str(parameters_cfg["WFM.S_PCK_CORE_PARAMETER.GETCHARATTR('ADMISSAO_PROPORCIONAL')"].iloc[0]).lower()
                self.logger.info(f"parameters_cfg: {parameters_cfg}")
            except Exception as e:
                self.logger.error(f"Error loading algorithm treatment params: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            if not validate_parameters_cfg(parameters_cfg):
                self.logger.error(f"admissao_proporcional is not a valid value: {parameters_cfg}.")
                return False, "errSubproc", "admissao_proporcional is not a valid value"

            # Copy the dataframes into the apropriate dict
            try:
                self.logger.info(f"Copying dataframes into the apropriate dict")
                # Copy the dataframes into the apropriate dict
                # AUX DATA
                self.auxiliary_data['df_messages'] = df_messages.copy()
                self.auxiliary_data['df_valid_emp'] = df_valid_emp.copy()
                self.auxiliary_data['df_params_lq'] = df_params_lq.copy()
                self.auxiliary_data['df_params'] = df_params.copy()
                self.auxiliary_data['df_festivos'] = df_festivos.copy()
                self.auxiliary_data['df_closed_days'] = df_closed_days.copy()
                self.auxiliary_data['unit_id'] = unit_id 
                self.auxiliary_data['secao_id'] = secao_id
                self.auxiliary_data['posto_id_list'] = posto_id_list
                self.auxiliary_data['main_year'] = main_year
                self.auxiliary_data['first_year_date'] = first_year_date
                self.auxiliary_data['last_year_date'] = last_year_date
                self.auxiliary_data['employees_id_list'] = employees_id_list
                
                # ALGORITHM TREATMENT PARAMS
                self.algorithm_treatment_params['admissao_proporcional'] = parameters_cfg

                self.logger.info(f"algorithm_treatment_params: {self.algorithm_treatment_params}")

                if not self.auxiliary_data:
                    self.logger.warning("No data was loaded into auxiliary_data")
                    return False, "errSubproc", "No data was loaded into auxiliary_data"
            except KeyError as e:
                self.logger.error(f"KeyError: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except ValueError as e:
                self.logger.error(f"ValueError: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except Exception as e:
                self.logger.error(f"Error copying dataframes into the apropriate dict, saving them in auxiliary_data: {e}", exc_info=True)
                return False, "errSubproc", str(e)
                
            self.logger.info(f"Successfully loaded {len(self.raw_data)} entities")
            return True, "validSubProc", ''
            
        except Exception as e:
            self.logger.error(f"Error loading process data: {str(e)}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_load_process_data(self) -> bool:
        """
        Validates func_inicializa operations. Validates data before running the allocation cycle.
        """
        try:
            # TODO: Implement validation logic
            self.logger.info("Entered func_inicializa validation. Needs to be implemented.")
            return True
        except Exception as e:
            self.logger.error(f"Error validating func_inicializa from data manager: {str(e)}")
            return False

    def treat_params(self) -> dict[str, Any]:
        """
        Treat parameters and store them in memory
        """
        try:
            # Treat params
            self.logger.info(f"Treating parameters in load_process_data")
            df_params = self.auxiliary_data['df_params'].copy()
            params_names_list = CONFIG.get('parameters_names', [])
            params_defaults = CONFIG.get('parameters_defaults', {})
            self.logger.info(f"df_params before treatment:\n{df_params}")

            # Get all parameters in one call
            retrieved_params = get_param_for_posto(
                df=df_params, 
                posto_id=self.external_call_data['current_posto_id'], 
                unit_id=self.auxiliary_data['unit_id'], 
                secao_id=self.auxiliary_data['secao_id'], 
                params_names_list=params_names_list
            ) or {}

            self.logger.info(f"Retrieved params after get_param_for_posto:\n{retrieved_params}")
            # Merge with defaults (retrieved params take precedence)
            for param_name in params_names_list:
                param_value = retrieved_params.get(param_name, params_defaults.get(param_name))
                self.auxiliary_data[param_name] = param_value
                self.logger.info(f"Parameter {param_name} = {param_value}")

                # Workaround feito para registar o nome do algoritmo - assim usa-se uma funcionalidade base do base-data-project
                if param_name == 'GD_algorithmName':
                    algorithm_name = param_value
            self.logger.info(f"Treating parameters completed successfully")
            return {'success': True, 'algorithm_name': algorithm_name}
        except Exception as e:
            self.logger.error(f"Error treating parameters: {e}", exc_info=True)	
            return {'success': False, 'algorithm_name': ''}

    def validate_params(self):
        """
        Validate parameters
        """
        self.logger.info(f"Validating parameters in validate_params method. Not implemented yet.")
        return True

    def load_colaborador_info(self, data_manager: BaseDataManager, posto_id: int = 0) -> bool:
        """
        transform database data into data raw
        """
        try:
            self.logger.info(f"Starting load_colaborador_info method.")

            # Get needed data
            try:
                # Store values in variables
                employees_id_list = self.auxiliary_data['employees_id_list']
                start_date = self.external_call_data['start_date']
                self.logger.info(f"Loaded information from df_valid_emp into employees_id_list: {employees_id_list}")
            except Exception as e:
                self.logger.error(f"Error loading colaborador info: {e}", exc_info=True)
                return False

            # Create colabs_str to be used in df_colaborador query
            try:
                self.logger.info(f"Creating colabs_str to be used in df_colaborador query.")
                if len(employees_id_list) == 1:
                    self.logger.info(f"employees_id_list has only one value: {employees_id_list[0]}")
                    colabs_str = str(employees_id_list[0])
                elif len(employees_id_list) > 1:
                    # Fix: Create a proper comma-separated list of numbers without any quotes
                    self.logger.info(f"employees_id_list has more than one value: {employees_id_list}")
                    colabs_str = ','.join(str(x) for x in employees_id_list)
                
                self.logger.info(f"colabs_str: {colabs_str}, type: {type(colabs_str)}")
            except Exception as e:
                self.logger.error(f"Error creating colabs_str to be used in query: {e}", exc_info=True)
                return False
            
            # Load df_colaborador info from data manager
            try:
                # colaborador info
                self.logger.info(f"Loading df_colaborador info from data manager")
                query_path = CONFIG.get('available_entities_raw', {}).get('df_colaborador')
                df_colaborador = data_manager.load_data('df_colaborador', query_file=query_path, colabs_id=colabs_str)
                df_colaborador = df_colaborador.rename(columns={'ec.codigo': 'fk_colaborador', 'codigo': 'fk_colaborador'})
                self.logger.info(f"df_colaborador shape (rows {df_colaborador.shape[0]}, columns {df_colaborador.shape[1]}): {df_colaborador.columns.tolist()}")
                
            except Exception as e:
                self.logger.error(f"Error loading df_colaborador info from data source: {e}", exc_info=True)
                return False

            # Get matricula from df_colaborador
            try:
                self.logger.info(f"Getting matriculas_list from df_colaborador")
                matriculas_list = df_colaborador['matricula'].tolist()
                employee_id_matriculas_map = df_colaborador[['fk_colaborador', 'matricula']].set_index('fk_colaborador').to_dict()['matricula']
                self.logger.info(f"Loaded information from df_colaborador into matriculas_list: {matriculas_list}")
            except Exception as e:
                self.logger.error(f"Error getting matriculas_list from df_colaborador: {e}", exc_info=True)
                return False

            # Create employees_id_90_list from df_colaborador
            try:
                self.logger.info(f"Creating employees_id_90_list from df_colaborador")
                employees_id_90_list = df_colaborador[df_colaborador['seq_turno'].str.upper() == 'CICLO']['fk_colaborador'].tolist()
                self.logger.info(f"Loaded information from df_colaborador into employees_id_90_list: {employees_id_90_list}")
            except Exception as e:
                self.logger.error(f"Error creating employees_id_90_list from df_colaborador: {e}", exc_info=True)
                return False

            try:
                self.logger.info(f"Creating past_employee_id_list from df_colaborador")
                start_date_dt = pd.to_datetime(start_date)
                past_employee_id_list = df_colaborador[pd.to_datetime(df_colaborador['data_admissao']) < start_date_dt]['fk_colaborador'].tolist()
                self.logger.info(f"Loaded information from df_colaborador into past_employee_id_list: {past_employee_id_list}")
            except Exception as e:
                self.logger.error(f"Error creating past_employee_id_list from df_colaborador: {e}", exc_info=True)
                return False

            # Saving values into memory
            try:
                self.logger.info(f"Saving df_colaborador in raw_data")

                self.raw_data['df_colaborador'] = df_colaborador.copy()
                # TODO: Remove this, not used
                self.auxiliary_data['num_fer_doms'] = 0
                self.auxiliary_data['employees_id_list'] = employees_id_list
                self.auxiliary_data['employee_id_matriculas_map'] = employee_id_matriculas_map
                self.auxiliary_data['matriculas_list'] = matriculas_list
                self.auxiliary_data['employees_id_90_list'] = employees_id_90_list
                self.auxiliary_data['past_employee_id_list'] = past_employee_id_list
                self.logger.info(f"load_colaborador_info completed successfully.")
                return True
            except KeyError as e:
                self.logger.error(f"KeyError: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error loading colaborador info using data manager. Error: {e}")
                return False
        except Exception as e:
            self.logger.error(f"Error loading colaborador info method: {e}", exc_info=True)
            return False
        
    def validate_colaborador_info(self) -> bool:
        """Function to validate the colaborador info"""
        try:
            self.logger.info("Starting validate_colaborador_info processing")
            # Get colaborador info data
            try:
                df_colaborador = self.raw_data['df_colaborador'].copy()
                #num_fer_dom = self.auxiliary_data['num_fer_dom']
            except KeyError as e:
                self.logger.error(f"Missing required DataFrame in validate_colaborador_info: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error loading data in validate_colaborador_info: {e}", exc_info=True)
                return False

            # Validate colaborador info
            if df_colaborador.empty or len(df_colaborador) < 1:
                self.logger.error(f"df_colaborador is empty. df_colaborador shape: {df_colaborador.shape}")
                return False

            # Validate specific df_colaborador columns
            
            #if num_fer_dom is None:
            #    self.logger.error("num_fer_dom is None")
            #    return False
            return True
        except Exception as e:
            self.logger.error(f"Error in validate_colaborador_info method: {e}", exc_info=True)
            return False

    def load_estimativas_info(self, data_manager: BaseDataManager, posto_id: int = 0, start_date: str = '', end_date: str = ''):
        """
        Load necessities from data manager and treat them data
        """
        try:
            self.logger.info(f"Starting load_estimativas_info method.")
            # Get current_posto_id from auxiliary_data
            try:
                posto_id = self.auxiliary_data['current_posto_id']
                self.logger.info(f"Loaded information from auxiliary_data into posto_id: {posto_id}")
            except Exception as e:
                self.logger.error(f"Error getting posto_id from auxiliary_data: {e}", exc_info=True)
                return False

            # Validate input parameters
            if not validate_posto_id(posto_id):
                self.logger.error(f"posto_id provided is invalid: {posto_id}")
                return False
            if start_date == '' or start_date == None or end_date == '' or end_date == None:
                self.logger.error(f"start_date or end_date provided are empty. start: {start_date}, end_date: {end_date}")
                return False

            try:
                # TODO: Review this
                self.logger.info("Initializing df_estimativas as an empty dataframe")
                # df_estimativas borns as an empty dataframe
                df_estimativas = pd.DataFrame()

                columns_select = ['nome', 'emp', 'fk_tipo_posto', 'loja', 'secao', 'h_tm_in', 'h_tm_out', 'h_tt_in', 'h_tt_out', 'h_seg_in', 'h_seg_out', 'h_ter_in', 'h_ter_out', 'h_qua_in', 'h_qua_out', 'h_qui_in', 'h_qui_out', 'h_sex_in', 'h_sex_out', 'h_sab_in', 'h_sab_out', 'h_dom_in', 'h_dom_out', 'h_fer_in', 'h_fer_out'] # TODO: define what columns to select
                self.logger.info(f"Columns to select: {columns_select}")
            except Exception as e:
                self.logger.error(f"Error initializing estimativas info: {e}", exc_info=True)
                return False

            try:
                self.logger.info(f"Loading df_turnos from raw_data df_colaborador with columns selected: {columns_select}")
                # Get sql file path, if the cvs is being used, it gets the the path defined on dummy_data_filepaths
                # turnos information: doesnt need a query since is information resent on core_alg_params
                
                df_turnos = self.raw_data['df_colaborador'].copy()
                df_turnos = df_turnos[columns_select]
                self.logger.info(f"df_turnos shape (rows {df_turnos.shape[0]}, columns {df_turnos.shape[1]}): {df_turnos.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error processing df_turnos from colaborador data: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Loading df_estrutura_wfm from data manager")
                # Estrutura wfm information
                query_path = CONFIG.get('available_entities_aux', {}).get('df_estrutura_wfm', '')
                if query_path == '':
                    self.logger.warning("df_estrutura_wfm query path not found in config")
                df_estrutura_wfm = data_manager.load_data('df_estrutura_wfm', query_file=query_path)
                self.logger.info(f"df_estrutura_wfm shape (rows {df_estrutura_wfm.shape[0]}, columns {df_estrutura_wfm.shape[1]}): {df_estrutura_wfm.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_estrutura_wfm: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Loading df_feriados from data manager")
                # feriados information
                query_path = CONFIG.get('available_entities_aux', {}).get('df_feriados', '')
                if query_path == '':
                    self.logger.warning("df_feriados query path not found in config")
                df_feriados = data_manager.load_data('df_feriados', query_file=query_path)
                self.logger.info(f"df_feriados shape (rows {df_feriados.shape[0]}, columns {df_feriados.shape[1]}): {df_feriados.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_feriados: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Loading df_faixa_horario from data manager")
                # faixa horario information
                query_path = CONFIG.get('available_entities_aux', {}).get('df_faixa_horario', '')
                if query_path == '':
                    self.logger.warning("df_faixa_horario query path not found in config")
                df_faixa_horario = data_manager.load_data('df_faixa_horario', query_file=query_path)
                self.logger.info(f"df_faixa_horario shape (rows {df_faixa_horario.shape[0]}, columns {df_faixa_horario.shape[1]}): {df_faixa_horario.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_faixa_horario: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Loading df_orcamento from data manager")
                # orcamento information
                query_path = CONFIG.get('available_entities_aux', {}).get('df_orcamento', '')
                if query_path == '':
                    self.logger.warning("df_orcamento query path not found in config")
                start_date_quoted = "'" + start_date + "'"
                end_date_quoted = "'" + end_date + "'"
                df_orcamento = data_manager.load_data('df_orcamento', query_file=query_path, posto_id=posto_id, start_date=start_date_quoted, end_date=end_date_quoted)
                self.logger.info(f"df_orcamento shape (rows {df_orcamento.shape[0]}, columns {df_orcamento.shape[1]}): {df_orcamento.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_orcamento: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Loading df_granularidade from data manager")
                # granularidade information
                query_path = CONFIG.get('available_entities_aux', {}).get('df_granularidade', '')
                if query_path == '':
                    self.logger.warning("df_granularidade query path not found in config")
                start_date_quoted = "'" + start_date + "'"
                end_date_quoted = "'" + end_date + "'"
                df_granularidade = data_manager.load_data('df_granularidade', query_file=query_path, start_date=start_date_quoted, end_date=end_date_quoted, posto_id=posto_id)
                self.logger.info(f"df_granularidade shape (rows {df_granularidade.shape[0]}, columns {df_granularidade.shape[1]}): {df_granularidade.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_granularidade: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Saving dataframes to auxiliary_data and raw_data")
                # TODO: save the dataframes if they are needed elsewhere, if not let them die here
                self.raw_data['df_estimativas'] = df_estimativas.copy()
                self.auxiliary_data['df_turnos'] = df_turnos.copy()
                self.auxiliary_data['df_estrutura_wfm'] = df_estrutura_wfm.copy()
                self.auxiliary_data['df_feriados'] = df_feriados.copy()
                self.auxiliary_data['df_faixa_horario'] = df_faixa_horario.copy()
                self.auxiliary_data['df_orcamento'] = df_orcamento.copy()
                self.auxiliary_data['df_granularidade'] = df_granularidade.copy()
                
                if not self.auxiliary_data:
                    self.logger.warning("No data was loaded into auxiliary_data")
                    return False
                    
                self.logger.info(f"load_estimativas_info completed successfully.")
                return True
            except KeyError as e:
                self.logger.error(f"KeyError when saving dataframes: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError when saving dataframes: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error saving dataframes to auxiliary_data and raw_data: {e}", exc_info=True)
                return False
        
        except Exception as e:
            self.logger.error(f"Error in load_estimativas_info method: {e}", exc_info=True)
            return False

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

    
    def load_calendario_info(self, data_manager: BaseDataManager, process_id: int = 0, posto_id: int = 0, start_date: str = '', end_date: str = '', past_employee_id_list: List[int] = []):
        """
        Load calendario from data manager and treat the data
        """
        try:
            self.logger.info(f"Starting load_calendario_info method.")
            
            # Validate input parameters
            # TODO: posto_id should come from auxiliary_data
            if posto_id == 0 or posto_id == None:
                self.logger.error("posto_id is 0, returning False. Check load_calendario_info call method.")
                return False
        
            # Get needed data
            try:
                self.logger.info("Loading tipo_contrato info from df_colaborador")
                matriculas_list = self.auxiliary_data['matriculas_list']
                employees_id_90_list = self.auxiliary_data['employees_id_90_list']
                past_employee_id_list = self.auxiliary_data['past_employee_id_list']
                employee_id_matriculas_map = self.auxiliary_data['employee_id_matriculas_map']

                # TODO: Consider removing main_year
                main_year = self.auxiliary_data['main_year']
                first_date_passado = self.auxiliary_data['first_year_date']
                last_date_passado = pd.to_datetime(self.auxiliary_data['last_year_date']) + pd.Timedelta(days=7)
                last_date_passado = last_date_passado.strftime('%Y-%m-%d')
                self.logger.info(f"Extracted {len(matriculas_list)} collaborators from emp column")
                #df_tipo_contrato = df_colaborador[['emp', 'tipo_contrato']] # TODO: ensure it is needed
            except Exception as e:
                self.logger.error(f"Error processing colaborador info: {e}", exc_info=True)
                return False

            # Treatment data functions calls
            try:
                self.logger.info("Treating data functions calls")
                df_calendario_passado = treat_df_calendario_passado(df_calendario_passado)
            except Exception as e:
                self.logger.error(f"Error treating data functions calls: {e}", exc_info=True)
                return False

            # Treat past_employee_id_list str
            try:
                self.logger.info("Treating past_employee_id_list str")
                self.logger.info(f"Creating colabs_str to be used in df_colaborador query.")
                #if len(past_employee_id_list) == 0:
                #    self.logger.error(f"Error in load_colaborador_info method: past_employee_id_list provided is empty (invalid): {past_employee_id_list}")
                #    return False
                if len(past_employee_id_list) == 1:
                    self.logger.info(f"past_employee_id_list has only one value: {past_employee_id_list[0]}")
                    colabs_passado_str = str(past_employee_id_list[0])
                elif len(past_employee_id_list) > 1:
                    # Fix: Create a proper comma-separated list of numbers without any quotes
                    self.logger.info(f"past_employee_id_list has more than one value: {past_employee_id_list}")
                    colabs_passado_str = ','.join(str(x) for x in past_employee_id_list)
                self.logger.info(f"colabs_passado_str: {colabs_passado_str}")
            except Exception as e:
                self.logger.error(f"Error treating past_employee_id_list str: {e}", exc_info=True)
                colabs_passado_str = ''

            try:
                # Only query if we have employees and the date range makes sense
                start_date_dt = pd.to_datetime(start_date)
                if len(past_employee_id_list) > 0 and start_date_dt != pd.to_datetime(first_date_passado):
                    self.logger.info("Loading df_calendario_passado since conditions are met")
                    query_path = CONFIG.get('available_entities_aux', {}).get('df_calendario_passado', '')
                    if not query_path:
                        self.logger.warning("df_calendario_passado query path not found in config")
                        df_calendario_passado = pd.DataFrame()
                    else:
                        df_calendario_passado = data_manager.load_data(
                            'df_calendario_passado', 
                            query_file=query_path, 
                            start_date=first_date_passado, 
                            end_date=last_date_passado, 
                            colabs=colabs_passado_str
                        )
                        self.logger.info(f"df_calendario_passado shape (rows {df_calendario_passado.shape[0]}, columns {df_calendario_passado.shape[1]}): {df_calendario_passado.columns.tolist()}")
                else:
                    self.logger.info("Conditions not met for loading df_calendario_passado")
                    df_calendario_passado = pd.DataFrame()
            except Exception as e:
                # TODO: Check default behavior
                self.logger.error(f"Error loading df_calendario_passado: {e}", exc_info=True)
                df_calendario_passado = pd.DataFrame()

            
            try:
                self.logger.info("Loading df_ausencias_ferias from data manager")
                # Ausencias ferias information
                query_path = CONFIG.get('available_entities_aux', {}).get('df_ausencias_ferias', '')
                if query_path != '':
                    colabs_id="'" + "','".join([str(x) for x in matriculas_list]) + "'"
                    df_ausencias_ferias = data_manager.load_data(
                        'df_ausencias_ferias', 
                        query_file=query_path, 
                        colabs_id=colabs_id
                    )
                    self.logger.info(f"df_ausencias_ferias shape (rows {df_ausencias_ferias.shape[0]}, columns {df_ausencias_ferias.shape[1]}): {df_ausencias_ferias.columns.tolist()}")
                else:
                    self.logger.warning("df_ausencias_ferias query path not found")
                    df_ausencias_ferias = pd.DataFrame()
            except Exception as e:
                self.logger.error(f"Error loading ausencias_ferias: {e}", exc_info=True)
                df_ausencias_ferias = pd.DataFrame()

            # DF_CORE_PRO_EMP_HORARIO_DET - pre folgas do ciclo
            try:
                self.logger.info("Loading df_core_pro_emp_horario_det from data manager")
                query_path = CONFIG.get('available_entities_aux', {}).get('df_core_pro_emp_horario_det', '')
                if query_path != '':
                    df_core_pro_emp_horario_det = data_manager.load_data(
                        'df_core_pro_emp_horario_det', 
                        query_file=query_path, 
                        process_id=process_id, 
                        start_date=start_date, 
                        end_date=end_date
                    )
                    self.logger.info(f"df_core_pro_emp_horario_det shape (rows {df_core_pro_emp_horario_det.shape[0]}, columns {df_core_pro_emp_horario_det.shape[1]}): {df_core_pro_emp_horario_det.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_core_pro_emp_horario_det: {e}", exc_info=True)
                df_core_pro_emp_horario_det = pd.DataFrame()

            try:
                self.logger.info("Loading df_ciclos_90 from data manager")
                # Ciclos de 90
                if len(employees_id_90_list) > 0:
                    query_path = CONFIG.get('available_entities_aux', {}).get('df_ciclos_90', '')
                    if query_path != '':
                        df_ciclos_90 = data_manager.load_data(
                            'df_ciclos_90', 
                            query_file=query_path, 
                            process_id=process_id, 
                            start_date=start_date, 
                            end_date=end_date, 
                            colab90ciclo=','.join(map(str, employees_id_90_list))
                        )
                        self.logger.info(f"df_ciclos_90 shape (rows {df_ciclos_90.shape[0]}, columns {df_ciclos_90.shape[1]}): {df_ciclos_90.columns.tolist()}")
                    else:
                        self.logger.warning("df_ciclos_90 query path not found")
                        df_ciclos_90 = pd.DataFrame()
                else:
                    self.logger.info("No employees with 90-day cycles")
                    df_ciclos_90 = pd.DataFrame()
            except Exception as e:
                self.logger.error(f"Error loading ciclos_90: {e}", exc_info=True)
                df_ciclos_90 = pd.DataFrame()

            try:
                self.logger.info("Loading df_days_off from data manager")
                query_path = CONFIG.get('available_entities_aux', {}).get('df_days_off', '')
                if query_path != '':
                    colabs_id="'" + "','".join([str(x) for x in matriculas_list]) + "'"
                    df_days_off = data_manager.load_data(
                        'df_days_off', 
                        query_file=query_path, 
                        colabs_id=colabs_id
                    )
                    if df_days_off.empty:
                        df_days_off = pd.DataFrame(columns=pd.Index(['employee_id', 'schedule_dt', 'sched_type']))
                        self.logger.info("df_days_off was empty, created with default columns")
                    else:
                        self.logger.info(f"df_days_off shape (rows {df_days_off.shape[0]}, columns {df_days_off.shape[1]}): {df_days_off.columns.tolist()}")
                else:
                    self.logger.warning("df_days_off query path not found")
                    df_days_off = pd.DataFrame()
            except Exception as e:
                self.logger.error(f"Error loading df_days_off: {e}", exc_info=True)
                df_days_off = pd.DataFrame()

            try:
                self.logger.info("Saving results to auxiliary_data and raw_data")
                # Saving results in memory
                self.auxiliary_data['df_calendario_past'] = pd.DataFrame()
                self.auxiliary_data['df_ausencias_ferias'] = df_ausencias_ferias.copy()
                self.auxiliary_data['df_days_off'] = df_days_off.copy()
                self.auxiliary_data['df_ciclos_90'] = df_ciclos_90.copy()
                self.auxiliary_data['df_core_pro_emp_horario_det'] = df_core_pro_emp_horario_det.copy()
                
                if not self.auxiliary_data:
                    self.logger.warning("No data was loaded into auxiliary_data")
                    return False
                    
                self.logger.info("load_calendario_info completed successfully")
                return True
            except KeyError as e:
                self.logger.error(f"KeyError when saving calendario data: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError when saving calendario data: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error saving calendario results to memory: {e}", exc_info=True)
                return False
            
        except Exception as e:
            self.logger.error(f"Error in load_calendario_info method: {e}", exc_info=True)
            return False

    def validate_calendario_info(self) -> tuple[bool, list[str]]:
        """
        Validate the calendario info.
        """
        try:
            # TODO: implement validation
            self.logger.info("Validating calendario info")
            validation_success = True
            invalid_entities = []

            # Validate df_calendario
            #try:
            #    df_calendario = self.auxiliary_data['df_calendario']
            #except KeyError as e:
            #    self.logger.error(f"Missing required DataFrame: {e}", exc_info=True)
            #    validation_success = False
            #    invalid_entities.append('df_calendario')
            #except Exception as e:
            #    self.logger.error(f"Error loading DataFrames: {e}", exc_info=True)
            #    validation_success = False
            #    invalid_entities.append('df_calendario_missing')
            #
            #if df_calendario.empty and len(df_calendario) == 0:
            #    validation_success = False
            #    invalid_entities.append('df_calendario_empty')

            if not validation_success:
                self.logger.error(f"Validation failed for entities: {invalid_entities}")
                return False, []
            
            self.logger.info("Validation completed successfully")
            return True, []
        except Exception as e:  
            self.logger.error(f"Error in validate_calendario_info method: {e}", exc_info=True)
            return False, []

    def load_calendario_transformations(self) -> bool:
        try:
            self.logger.info("Starting load_calendario_transformations processing")
            
            # Load data
            try:
                # Lists
                employees_id_list = self.auxiliary_data['employees_id_list']
                matriculas_list = self.auxiliary_data['matriculas_list']
                # Dict
                employee_id_matriculas_map = self.auxiliary_data['employee_id_matriculas_map']
                # Strings
                start_date = self.auxiliary_data['start_date']
                end_date = self.auxiliary_data['end_date']
                # Dataframe
                df_calendario_passado = self.auxiliary_data['df_calendario_passado']
                df_ausencias_ferias = self.auxiliary_data['df_ausencias_ferias']
                df_core_pro_emp_horario_det = self.auxiliary_data['df_core_pro_emp_horario_det']
                df_ciclos_90 = self.auxiliary_data['df_ciclos_90']
                df_days_off = self.auxiliary_data['df_days_off']
            except KeyError as e:
                self.logger.error(f"Missing required DataFrame in load_calendario_transformations: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error loading DataFrame in load_calendario_transformations: {e}", exc_info=True)
                return False

            try:
                # TODO: add create_df_calendario logic
                df_calendario = create_df_calendario(
                    start_date=start_date, 
                    end_date=end_date,
                    employee_id_matriculas_map=employee_id_matriculas_map
                )

                # TODO: add df_ausencias_ferias to df_calendario
                df_calendario = add_ausencias_ferias(df_calendario, df_ausencias_ferias)

                # TODO: add df_ciclos_90 to df_calendario
                df_calendario = add_ciclos_90(df_calendario, df_ciclos_90)
                
                # TODO: add df_days_off to df_calendario
                df_calendario = add_days_off(df_calendario, df_days_off)
                
                # TODO: add df_calendario_passado to df_calendario
                df_calendario = add_calendario_passado(df_calendario, df_calendario_passado)

                # TODO: add df_core_pro_emp_horario_det to df_calendario
                df_calendario = add_folgas_ciclos(df_calendario, df_core_pro_emp_horario_det)
        
                pass
            except Exception as e:
                self.logger.error(f"Error in load_calendario_transformations method: {e}", exc_info=True)
                return False

        except Exception as e:
            self.logger.error(f"Error in load_calendario_transformations method: {e}", exc_info=True)
            return False