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
from src.data_models.functions.helper_functions import count_dates_per_year, get_param_for_posto, load_wfm_scheds, convert_types_in, load_pre_ger_scheds
from src.data_models.functions.loading_functions import load_valid_emp_csv
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
                - messages_df: DataFrame for messages
                - final: Final data (TODO: rename)
                - num_fer_doms: Number of holidays and Sundays
                - params_df: Algorithm parameters
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
            'messages_df': pd.DataFrame(), # df containing messages to set process errors
            'final': None, # TODO: change the name
            'num_fer_doms': 0, # number of feriados and Sundays in the year
            'params_df': None, # algorithm parameters
            'algorithm_name': None, # algorithm name - now comes from query
            'params_lq': None, # LQ parameters
            'valid_emp': None, # valid employees filtered for processing
            'colabs_id_list': None, # list of collaborator IDs
            'convenio': None, # convention information
            'unit_id': None, # unit ID
            'secao_id': None, # section ID
            'posto_id_list': None, # list of posto IDs
            'current_posto_id': None, # current posto ID
            'df_festivos': None, # holiday information dataframe
            'df_closed_days': None, # closed days information dataframe
            'df_ausencias_ferias': None, # holidays absences information dataframe
            'df_days_off': None, # days off information dataframe
            'df_turnos': None, # shift information dataframe
            'df_calendario_passado': None, # past calendar information dataframe
            'df_day_aloc': None, # day allocation information dataframe
            'emp_pre_ger': None, # pre-generated employee information dataframe
            'df_count': None, # count information dataframe
            'start_date2': None, # start date 2
            'end_date2': None # end date 2
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
        messages_df = pd.DataFrame()
        # This variable is only initialized because of the type checker
        # In the future it should contain the error message to add to the log
        error_message = None
        try:
            self.logger.info(f"DEBUGGING: Loading messages_df from CSV file")
            messages_path = os.path.join(ROOT_DIR, 'data', 'csvs', 'messages_df.csv')
            messages_df = pd.read_csv(messages_path)
            self.logger.info(f"DEBUGGING: messages_df loaded successfully with {len(messages_df)} rows")
        except Exception as e:
            self.logger.error(f"Error loading messages_df: {e}")
            # Don't return False - continue without messages_df for now
            
        if messages_df.empty:
            self.logger.warning("DEBUGGING: messages_df is empty - set_process_errors will be skipped")
        else:
            self.logger.info(f"DEBUGGING: messages_df has {len(messages_df)} rows - set_process_errors will work")

        try:
            self.logger.info("Loading process data from data manager")

            # Get entities to load from configuration
            if not entities_dict:
                self.logger.warning("No entities passed as argument")
                return False, "errSubproc", 'No entities passed as argument'

            # Load valid_emp
            try:
                self.logger.info(f"Loading valid_emp from data manager")
                if isinstance(data_manager, CSVDataManager):
                    self.logger.info(f"Loading valid_emp from csv")
                    valid_emp = load_valid_emp_csv()
                elif isinstance(data_manager, DBDataManager):
                    # valid emp info
                    self.logger.info(f"Loading valid_emp from database")
                    query_path = entities_dict['valid_emp']
                    process_id_str = "'" + str(self.external_call_data['current_process_id']) + "'"
                    valid_emp = data_manager.load_data('valid_emp', query_file=query_path, process_id=process_id_str)
                else:
                    self.logger.error(f"No instance found for data_manager: {data_manager.__name__}")


                if valid_emp.empty:
                    self.logger.error("valid_emp is empty")
                    # TODO: Add set process errors
                    return False, "errNoColab", "valid_emp is empty"
                    
                valid_emp['prioridade_folgas'] = valid_emp['prioridade_folgas'].fillna(0.0)
                valid_emp['prioridade_folgas'] = valid_emp['prioridade_folgas'].astype(int)
                valid_emp['prioridade_folgas'] = valid_emp['prioridade_folgas'].astype(str)
                
                # Convert prioridade_folgas values: '1' -> 'manager', '2' -> 'keyholder'
                valid_emp['prioridade_folgas'] = valid_emp['prioridade_folgas'].replace({
                    '1': 'manager',
                    '2': 'keyholder',
                    '1.0': 'manager',
                    '2.0': 'keyholder',
                    '0': 'normal'
                })
                valid_emp['prioridade_folgas'] = valid_emp['prioridade_folgas'].fillna('')
                self.logger.info(f"valid_emp:\n{valid_emp}")

                self.logger.info(f"valid_emp shape (rows {valid_emp.shape[0]}, columns {valid_emp.shape[1]}): {valid_emp.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading valid_emp: {e}", exc_info=True)
                return False, "errSubproc", str(e)


            # Load important info into memory
            try:
                self.logger.info(f"Loading important info into memory(unit_id, secao_id, posto_id_list, colabs_id_list, main_year)")
                # Save important this important info to be able to use it on querys
                unit_id = valid_emp['fk_unidade'].unique()[0]  # Get first (and only) unique value
                secao_id = valid_emp['fk_secao'].unique()[0]   # Get first (and only) unique value
                posto_id_list = valid_emp['fk_tipo_posto'].unique().tolist()  # Get list of unique values
                self.logger.info(f"unit_id: {unit_id}, secao_id: {secao_id}, posto_id_list: {posto_id_list} stored in variables")

                if len(valid_emp['fk_unidade'].unique()) > 1 or len(valid_emp['fk_secao'].unique()) > 1 or len(valid_emp['fk_tipo_posto'].unique()) == 0:
                    self.logger.error("More than one fk_secao or fk_unidade associated with the process.")
                    raise ValueError

                # Get colab_ids list
                colabs_id_list = valid_emp['fk_colaborador'].unique().tolist()
                self.logger.info(f"colabs_id_list: {colabs_id_list} stored in variables")
                main_year = count_dates_per_year(start_date_str=self.external_call_data.get('start_date', ''), end_date_str=self.external_call_data.get('end_date', ''))
                self.logger.info(f"main_year: {main_year} stored in variables")
            except Exception as e:
                self.logger.error(f"Error loading important info into memory(unit_id, secao_id, posto_id_list, colabs_id_list, main_year): {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # TODO: semanas_restantes logic to add to auxiliary_data

            # Load params_lq
            try:
                self.logger.info(f"Loading params_lq from data manager")
                # Logic needed because query cant run against dfs
                if isinstance(data_manager, CSVDataManager):
                    self.logger.info(f"Loading params_lq from csv")
                    params_lq = data_manager.load_data('params_lq')
                elif isinstance(data_manager, DBDataManager):
                    self.logger.info(f"Loading params_lq from database")
                    query_path = entities_dict['params_lq']
                    params_lq = data_manager.load_data('params_lq', query_file=query_path)
                else:
                    self.logger.error(f"No instance found for data_manager: {data_manager.__name__}")
                self.logger.info(f"params_lq shape (rows {params_lq.shape[0]}, columns {params_lq.shape[1]}): {params_lq.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading params_lq: {e}", exc_info=True)
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

            # Load start_date2 and end_date2
            try:
                self.logger.info(f"Treating start_date2 and end_date2")
                start_date2 = pd.to_datetime(f"{main_year}-01-01")
                end_date2 = pd.to_datetime(f"{main_year}-12-31")
            except Exception as e:
                self.logger.error(f"Error treating start_date2 and end_date2: {e}", exc_info=True)
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

            # Treat df_closed_days
            try:
                self.logger.info(f"Treating df_closed_days")
                if len(df_closed_days) > 0:
                    df_closed_days = (df_closed_days
                            .assign(data=pd.to_datetime(df_closed_days['data'].dt.strftime('%Y-%m-%d')))
                            .query('(data >= @start_date2 and data <= @end_date2) or data < "2000-12-31"')
                            .assign(data=lambda x: x['data'].apply(lambda d: d.replace(year=start_date2.year)))
                            [['data']]
                            .drop_duplicates())
            except Exception as e:
                self.logger.error(f"Error treating df_closed_days: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Load global parameters - Very important!! This could be done with params_lq query most probably
            try:
                self.logger.info(f"Loading parameters from data manager")
                query_path = entities_dict['params_df']
                unit_id_str = "'" + str(unit_id) + "'"
                params_df = data_manager.load_data('params_df', query_file=query_path, unit_id=unit_id_str)
                self.logger.info(f"params_df shape (rows {params_df.shape[0]}, columns {params_df.shape[1]}): {params_df.columns.tolist()}")
                self.logger.info(f"DEBUG: params_df {params_df}")
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
                if parameters_cfg.empty:
                    self.logger.error(f"parameters_cfg is empty")
                    return False, "errSubproc", "parameters_cfg is empty"
                # Store the value to then validate it
                parameters_cfg = str(parameters_cfg["WFM.S_PCK_CORE_PARAMETER.GETCHARATTR('ADMISSAO_PROPORCIONAL')"].iloc[0]).lower()
                self.logger.info(f"parameters_cfg: {parameters_cfg}")
            except Exception as e:
                self.logger.error(f"Error loading algorithm treatment params: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            if parameters_cfg not in ['floor', 'ceil']:
                self.logger.error(f"admissao_proporcional is not a valid value: {parameters_cfg}")
                return False, "errSubproc", "admissao_proporcional is not a valid value"

            # Copy the dataframes into the apropriate dict
            try:
                self.logger.info(f"Copying dataframes into the apropriate dict")
                # Copy the dataframes into the apropriate dict
                # AUX DATA
                self.auxiliary_data['valid_emp'] = valid_emp.copy()
                self.auxiliary_data['params_lq'] = params_lq.copy()
                self.auxiliary_data['params_df'] = params_df.copy()
                self.auxiliary_data['df_festivos'] = df_festivos.copy()
                self.auxiliary_data['df_closed_days'] = df_closed_days.copy()
                self.auxiliary_data['unit_id'] = unit_id 
                self.auxiliary_data['secao_id'] = secao_id
                self.auxiliary_data['posto_id_list'] = posto_id_list
                self.auxiliary_data['main_year'] = main_year
                self.auxiliary_data['start_date2'] = start_date2
                self.auxiliary_data['end_date2'] = end_date2
                self.auxiliary_data['colabs_id_list'] = colabs_id_list
                self.auxiliary_data['messages_df'] = messages_df
                
                # ALGORITHM TREATMENT PARAMS
                # TODO: remove comment from query line
                self.algorithm_treatment_params['admissao_proporcional'] = parameters_cfg
                #self.algorithm_treatment_params['admissao_proporcional'] = 'floor'
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
            params_df = self.auxiliary_data['params_df'].copy()
            params_names_list = CONFIG.get('parameters_names', [])
            params_defaults = CONFIG.get('parameters_defaults', {})
            self.logger.info(f"params_df before treatment:\n{params_df}")

            # Get all parameters in one call
            retrieved_params = get_param_for_posto(
                df=params_df, 
                posto_id=self.external_call_data['current_process_id'], 
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
            try:
                self.logger.info(f"Loading colaborador info from data_model. Creating colabs_id_list")
                valid_emp = self.auxiliary_data['valid_emp'].copy()
                valid_emp = valid_emp[valid_emp['fk_tipo_posto'] == posto_id]
                # TODO: check if this is need since it also is loaded in load_process_data
                colabs_id_list = valid_emp['fk_colaborador'].tolist()
                self.logger.info(f"Loaded information from valid_emp into colabs_id_list: {colabs_id_list}")
            except Exception as e:
                self.logger.error(f"Error loading colaborador info: {e}", exc_info=True)
                return False

            try:
                self.logger.info(f"Creating colabs_str to be used in df_colaborador query.")
                if len(colabs_id_list) == 0:
                    self.logger.error(f"Error in load_colaborador_info method: colabs_id_list provided is empty (invalid): {colabs_id_list}")
                    return False
                elif len(colabs_id_list) == 1:
                    self.logger.info(f"colabs_id_list has only one value: {colabs_id_list[0]}")
                    colabs_str = str(colabs_id_list[0])
                elif len(colabs_id_list) > 1:
                    # Fix: Create a proper comma-separated list of numbers without any quotes
                    self.logger.info(f"colabs_id_list has more than one value: {colabs_id_list}")
                    colabs_str = ','.join(str(x) for x in colabs_id_list)
                
                self.logger.info(f"colabs_str: {colabs_str}, type: {type(colabs_str)}")
            except Exception as e:
                self.logger.error(f"Error creating colabs_str to be used in query: {e}", exc_info=True)
                return False
            
            try:
                # colaborador info
                self.logger.info(f"Loading df_colaborador info from data manager")
                query_path = CONFIG.get('available_entities_raw', {}).get('df_colaborador')
                df_colaborador = data_manager.load_data('df_colaborador', query_file=query_path, colabs_id=colabs_str)
                df_colaborador = df_colaborador.rename(columns={'ec.codigo': 'fk_colaborador', 'codigo': 'fk_colaborador'})
                self.logger.info(f"df_colaborador shape (rows {df_colaborador.shape[0]}, columns {df_colaborador.shape[1]}): {df_colaborador.columns.tolist()}")
                
                # Saving values into memory
                self.logger.info(f"Saving df_colaborador in raw_data")
                self.raw_data['df_colaborador'] = df_colaborador.copy()
                self.auxiliary_data['num_fer_doms'] = 0
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
            
            # Validate input parameters
            if posto_id == 0:
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
                if not query_path:
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
                if not query_path:
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
                if not query_path:
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
                if not query_path:
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
                if not query_path:
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

    
    def load_calendario_info(self, data_manager: BaseDataManager, process_id: int = 0, posto_id: int = 0, start_date: str = '', end_date: str = '', colabs_passado: List[int] = []):
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
        
            try:
                self.logger.info("Loading tipo_contrato info from df_colaborador")
                # Tipo_contrato info
                df_colaborador = self.raw_data['df_colaborador'].copy()
                self.logger.info(f"df_colaborador shape (rows {df_colaborador.shape[0]}, columns {df_colaborador.shape[1]}): {df_colaborador.columns.tolist()}")
                
                colaborador_list = df_colaborador['emp'].tolist()
                self.logger.info(f"Extracted {len(colaborador_list)} collaborators from emp column")
                #df_tipo_contrato = df_colaborador[['emp', 'tipo_contrato']] # TODO: ensure it is needed
            except Exception as e:
                self.logger.error(f"Error processing colaborador info: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Identifying employees with 90-day cycles")
                # Get the colab_id for ciclos 90 - FIXED: Added str.upper() and proper column selection
                colaborador_90_list = df_colaborador[df_colaborador['seq_turno'].str.upper() == 'CICLO']['fk_colaborador'].tolist()
                self.logger.info(f"Found {len(colaborador_90_list)} employees with 90-day cycles: {colaborador_90_list}")
            except KeyError as e:
                self.logger.error(f"Column not found for 90-day cycles: {e}", exc_info=True)
                self.logger.info(f"Available columns: {df_colaborador.columns.tolist()}")
                colaborador_90_list = []
            except Exception as e:
                self.logger.error(f"Error processing 90-day cycles: {e}", exc_info=True)
                colaborador_90_list = []

            try:
                self.logger.info("Initializing empty df_calendario")
                # Get sql file path, if the cvs is being used, it gets the the path defined on dummy_data_filepaths
                # calendario information
                #query_path = CONFIG.get('available_entities_raw', {}).get('df_calendario', '')
                #df_calendario = data_manager.load_data('df_calendario', custom_query=query_path)
                df_calendario = pd.DataFrame()
                self.logger.info("df_calendario initialized as empty DataFrame")
            except Exception as e:
                self.logger.error(f"Error initializing df_calendario: {e}", exc_info=True)
                return False

            try:
                # Set up calendario passado dates
                self.logger.info("Setting up calendario passado dates")
                # Calendario passado - FIXED: Added proper type conversion
                main_year = str(self.auxiliary_data.get('main_year', ''))
                if not main_year:
                    self.logger.warning("main_year not found in auxiliary_data")
                    main_year = pd.to_datetime(start_date).year
                
                first_date_passado = f"{main_year}-01-01"
                self.logger.info(f"first_date_passado: {first_date_passado}")
                
                last_date_passado = pd.to_datetime(self.external_call_data.get('end_date', end_date))
                last_date_passado = last_date_passado + pd.Timedelta(days=7)
                last_date_passado = last_date_passado.strftime('%Y-%m-%d')
                self.logger.info(f"last_date_passado: {last_date_passado}")
            except Exception as e:
                self.logger.error(f"Error setting up dates: {e}", exc_info=True)
                return False

            try:
                # Filter employees by admission date
                self.logger.info("Filtering employees by admission date")
                df_colaborador = self.raw_data['df_colaborador']
                start_date_dt = pd.to_datetime(start_date)

                # Fixed: Added proper error handling
                colabs_passado = df_colaborador[
                    pd.to_datetime(df_colaborador['data_admissao']) < start_date_dt
                ]['fk_colaborador'].tolist()
                self.logger.info(f"Found {len(colabs_passado)} employees with past admission dates: {colabs_passado}")
            except Exception as e:
                self.logger.error(f"Error filtering employees by admission date: {e}", exc_info=True)
                colabs_passado = []

            # Treat colabs_passado str
            try:
                self.logger.info("Treating colabs_passado str")
                self.logger.info(f"Creating colabs_str to be used in df_colaborador query.")
                if len(colabs_passado) == 0:
                    self.logger.error(f"Error in load_colaborador_info method: colabs_passado provided is empty (invalid): {colabs_passado}")
                    return False
                elif len(colabs_passado) == 1:
                    self.logger.info(f"colabs_passado has only one value: {colabs_passado[0]}")
                    colabs_str = str(colabs_passado[0])
                elif len(colabs_passado) > 1:
                    # Fix: Create a proper comma-separated list of numbers without any quotes
                    self.logger.info(f"colabs_passado has more than one value: {colabs_passado}")
                    colabs_str = ','.join(str(x) for x in colabs_passado)
                self.logger.info(f"colabs_str: {colabs_str}")
            except Exception as e:
                self.logger.error(f"Error treating colabs_passado str: {e}", exc_info=True)
                colabs_str = ''

            try:
                # Only query if we have employees and the date range makes sense
                if len(colabs_passado) > 0 and start_date_dt != pd.to_datetime(first_date_passado):
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
                            colabs=colabs_str
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
                self.logger.info("Processing historical calendar data")
                # Process calendar data if available
                if len(df_calendario_passado) == 0:
                    self.logger.info(f"No historical calendar data for employees: {colabs_passado}")
                    reshaped_final_3 = pd.DataFrame()
                    emp_pre_ger = []
                    df_count = pd.DataFrame()
                else:
                    reshaped_final_3, emp_pre_ger, df_count = load_wfm_scheds(
                        df_calendario_passado,  # Your DataFrame with historical schedule data
                        df_calendario_passado['employee_id'].unique().tolist()  # List of employee IDs
                    )
                    self.logger.info(f"Successfully processed historical calendar data - reshaped_final_3: {reshaped_final_3.shape}, emp_pre_ger: {len(emp_pre_ger)}, df_count: {df_count.shape}")
            except Exception as e:
                self.logger.error(f"Error in load_wfm_scheds: {e}", exc_info=True)
                reshaped_final_3 = pd.DataFrame()
                emp_pre_ger = []
                df_count = pd.DataFrame()

            try:
                self.logger.info("Loading df_ausencias_ferias from data manager")
                # Ausencias ferias information
                query_path = CONFIG.get('available_entities_aux', {}).get('df_ausencias_ferias', '')
                if query_path:
                    colabs_id="'" + "','".join([str(x) for x in colaborador_list]) + "'"
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
                if query_path:
                    df_core_pro_emp_horario_det = data_manager.load_data(
                        'df_core_pro_emp_horario_det', 
                        query_file=query_path, 
                        process_id=process_id, 
                        start_date=start_date, 
                        end_date=end_date
                    )
                    self.logger.info(f"df_core_pro_emp_horario_det shape (rows {df_core_pro_emp_horario_det.shape[0]}, columns {df_core_pro_emp_horario_det.shape[1]}): {df_core_pro_emp_horario_det.columns.tolist()}")
                    #self.logger.info(f"DEBUG: df_core_pro_emp_horario_det: {df_core_pro_emp_horario_det}")
            except Exception as e:
                self.logger.error(f"Error loading df_core_pro_emp_horario_det: {e}", exc_info=True)
                df_core_pro_emp_horario_det = pd.DataFrame()

            try:
                self.logger.info("Loading df_ciclos_90 from data manager")
                # Ciclos de 90
                if len(colaborador_90_list) > 0:
                    query_path = CONFIG.get('available_entities_aux', {}).get('df_ciclos_90', '')
                    if query_path:
                        df_ciclos_90 = data_manager.load_data(
                            'df_ciclos_90', 
                            query_file=query_path, 
                            process_id=process_id, 
                            start_date=start_date, 
                            end_date=end_date, 
                            colab90ciclo=','.join(map(str, colaborador_90_list))
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
                if query_path:
                    colabs_id="'" + "','".join([str(x) for x in colaborador_list]) + "'"
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
                self.auxiliary_data['df_calendario_passado'] = reshaped_final_3.copy()
                self.auxiliary_data['emp_pre_ger'] = emp_pre_ger
                self.auxiliary_data['df_count'] = df_count.copy()
                self.auxiliary_data['df_core_pro_emp_horario_det'] = df_core_pro_emp_horario_det.copy()
                self.raw_data['df_calendario'] = df_calendario.copy()

                # TODO: remove this
                self.logger.info(f"df_calendario shape: {df_calendario.shape}")
                
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