"""File containing the data model for Salsa"""

# Dependencies
from typing import Dict, Optional, Any, List, Tuple
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
    get_first_and_last_day_passado_arguments,
    get_past_employee_id_list,
    create_employee_query_string,
    count_holidays_in_period,
    count_sundays_in_period,
    count_open_holidays
)
from src.data_models.functions.data_treatment_functions import (
    treat_df_valid_emp, 
    treat_df_closed_days, 
    treat_df_calendario_passado,
    treat_df_ausencias_ferias,
    treat_df_ciclos_90,
    treat_df_colaborador,
    add_lqs_to_df_colaborador,
    set_tipo_contrato_to_df_colaborador,
    add_prioridade_folgas_to_df_colaborador,
    admission_date_adjustments_to_df_colaborador,
    add_l_d_to_df_colaborador,
    add_l_dom_to_df_colaborador,
    add_l_q_to_df_colaborador, 
    add_l_total_to_df_colaborador,
    set_c2d_to_df_colaborador,
    set_c3d_to_df_colaborador,
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
    validate_past_employee_id_list,
    validate_date_passado,
    validate_df_ausencias_ferias,
    validate_df_core_pro_emp_horario_det
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

    def load_process_data(self, data_manager: BaseDataManager, entities_dict: Dict[str, str]) -> Tuple[bool, str, str]:
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

                success, df_valid_emp, error_msg = treat_df_valid_emp(df_valid_emp)
                if not success:
                    self.logger.error(f"Employee treatment failed: {error_msg}")
                    return False, "errNoColab", error_msg

                if not validate_df_valid_emp(df_valid_emp):
                    self.logger.error("df_valid_emp is invalid")
                    # TODO: Add set process errors
                    return False, "errNoColab", "df_valid_emp is invalid"

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
                wfm_proc_colab = self.external_call_data.get('wfm_proc_colab', None)
                self.logger.info(f"start_date: {start_date}, end_date: {end_date} stored in variables")

                main_year = count_dates_per_year(start_date_str=self.external_call_data.get('start_date', ''), end_date_str=self.external_call_data.get('end_date', ''))
                self.logger.info(f"main_year: {main_year} stored in variables")

                
                # Load first_year_date and last_year_date
                self.logger.info(f"Treating first_year_date and last_year_date")
                first_year_date = pd.to_datetime(f"{main_year}-01-01")
                last_year_date = pd.to_datetime(f"{main_year}-12-31")
                
                first_day_passado, last_day_passado, case_type = get_first_and_last_day_passado_arguments(
                    start_date_str=start_date, 
                    end_date_str=end_date, 
                    main_year=main_year, 
                    wfm_proc_colab=wfm_proc_colab, 
                )
                self.logger.info(f"first_day_passado: {first_day_passado}, last_day_passado: {last_day_passado}")

                # Calculate domingos and festivos amount
                num_sundays_year = count_sundays_in_period(
                    first_day_year_str=first_year_date,
                    last_day_year_str=last_day_passado,
                    start_date_str=start_date,
                    end_date_str=end_date
                )

                if not isinstance(num_sundays_year, int) or num_sundays_year < 0:
                    self.logger.error(f"num_sundays_year is invalid: {num_sundays_year}")
                    return False, "", ""

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

            if not validate_date_passado(first_day_passado):
                self.logger.error(f"first_day_passado is empty: {first_day_passado}")
                return False, "errSubproc", "first_day_passado is empty"

            if not validate_date_passado(last_day_passado):
                self.logger.error(f"last_day_passado is empty: {last_day_passado}")
                return False, "errSubproc", "last_day_passado is empty"

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

            num_feriados_abertos, num_feriados_fechados = count_holidays_in_period(
                start_date_str=first_year_date,
                end_date_str=last_year_date,
                df_festivos=df_festivos,
                use_case=0 # TODO: this should be defined in params
            )

            if not isinstance(num_feriados_abertos, int) or num_feriados_abertos < 0:
                self.logger.error(f"num_feriados is invalid: {num_feriados_abertos}")
                return False, "", ""

            if not isinstance(num_feriados_fechados, int) or num_feriados_fechados < 0:
                self.logger.error(f"num_feriados is invalid: {num_feriados_fechados}")
                return False, "", ""

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

            self.logger.info(f"DEBUG: df_closed_days: {df_closed_days}")

            success, df_closed_days, error_msg = treat_df_closed_days(df_closed_days, first_year_date, last_year_date)
            if not success:
                self.logger.error(f"Closed days treatment failed: {error_msg}")
                return False, "errSubproc", error_msg

            if df_closed_days.empty:
                self.logger.info(f"No closed days found in the specified date range - proceeding with empty DataFrame")
                # Empty df_closed_days is valid - it means no closed days in the period

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
                
                # AUX DATA - Copy the dataframes into the apropriate dict
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
                self.auxiliary_data['first_date_passado'] = first_day_passado
                self.auxiliary_data['last_date_passado'] = last_day_passado
                self.auxiliary_data['employees_id_list'] = employees_id_list
                self.auxiliary_data['case_type'] = case_type
                self.auxiliary_data['num_sundays_year'] = num_sundays_year
                self.auxiliary_data['num_feriados_abertos'] = num_feriados_abertos
                self.auxiliary_data['num_feriados_fechados'] = num_feriados_fechados
                
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

    def validate_process_data(self) -> bool:
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

    def treat_params(self) -> Tuple[bool, str, str]:
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
                posto_id=self.auxiliary_data['current_posto_id'], 
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
            # Store algorithm_name in auxiliary_data for later use
            self.auxiliary_data['algorithm_name'] = algorithm_name
            return True, "", ""
        except Exception as e:
            self.logger.error(f"Error treating parameters: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_params(self):
        """
        Validate parameters
        """
        self.logger.info(f"Validating parameters in validate_params method. Not implemented yet.")
        return True

    def load_colaborador_info(self, data_manager: BaseDataManager, posto_id: int = 0) -> Tuple[bool, str, str]:
        """
        transform database data into data raw
        """
        try:
            self.logger.info(f"Starting load_colaborador_info method.")

            # Get needed data
            try:
                # Store values in variables
                employees_id_list = self.auxiliary_data['employees_id_list']
                case_type = self.auxiliary_data['case_type']
                # External call data values
                start_date = self.external_call_data['start_date']
                wfm_proc_colab = self.external_call_data['wfm_proc_colab']
                self.logger.info(f"Loaded information from df_valid_emp into employees_id_list: {employees_id_list}")
            except Exception as e:
                self.logger.error(f"Error loading colaborador info: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Create colabs_str to be used in df_colaborador query
            colabs_str = create_employee_query_string(employee_id_list=employees_id_list)
            
            # Load df_colaborador info from data manager
            try:
                # colaborador info
                self.logger.info(f"Loading df_colaborador info from data manager")
                query_path = CONFIG.get('available_entities_raw', {}).get('df_colaborador')
                df_colaborador = data_manager.load_data(
                    entity='df_colaborador', 
                    query_file=query_path, 
                    colabs_id=colabs_str
                )
                self.logger.info(f"df_colaborador shape (rows {df_colaborador.shape[0]}, columns {df_colaborador.shape[1]}): {df_colaborador.columns.tolist()}")
                
            except Exception as e:
                self.logger.error(f"Error loading df_colaborador info from data source: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            success, df_colaborador, error_msg = treat_df_colaborador(df_colaborador)
            if not success:
                self.logger.error(f"Colaborador treatment failed: {error_msg}")
                return False, "errSubproc", error_msg

            # Note: validate_df_colaborador function doesn't exist, so commenting out for now
            # if not validate_df_colaborador(df_colaborador):
            #     self.logger.error(f"")
            #     return False

            # Get matricula from df_colaborador
            try:
                self.logger.info(f"Getting matriculas_list from df_colaborador")
                matriculas_list = df_colaborador['matricula'].tolist()
                employee_id_matriculas_map = df_colaborador[['fk_colaborador', 'matricula']].set_index('fk_colaborador').to_dict()['matricula']
                self.logger.info(f"Loaded information from df_colaborador into matriculas_list: {matriculas_list}")
            except Exception as e:
                self.logger.error(f"Error getting matriculas_list from df_colaborador: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Create employees_id_90_list from df_colaborador
            try:
                self.logger.info(f"Creating employees_id_90_list from df_colaborador")
                employees_id_90_list = df_colaborador[df_colaborador['seq_turno'].str.upper() == 'CICLO']['fk_colaborador'].tolist()
                self.logger.info(f"Loaded information from df_colaborador into employees_id_90_list: {employees_id_90_list}")
            except Exception as e:
                self.logger.error(f"Error creating employees_id_90_list from df_colaborador: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info(f"Creating past_employee_id_list from df_colaborador")
                start_date_dt = pd.to_datetime(start_date)
                past_employee_id_list = df_colaborador[pd.to_datetime(df_colaborador['data_admissao']) < start_date_dt]['fk_colaborador'].tolist()
                self.logger.info(f"Loaded information from df_colaborador into past_employee_id_list: {past_employee_id_list}")
            except Exception as e:
                self.logger.error(f"Error creating past_employee_id_list from df_colaborador: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            past_employee_id_list = get_past_employee_id_list(past_employee_id_list, case_type, wfm_proc_colab)

            # Quick validations
            if not validate_past_employee_id_list(past_employee_id_list):
                self.logger.error(f"past_employee_id_list is empty: {past_employee_id_list}")
                return False, "errSubproc", "past_employee_id_list is empty"

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
                return True, "", ""
            except KeyError as e:
                self.logger.error(f"KeyError: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except ValueError as e:
                self.logger.error(f"ValueError: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except Exception as e:
                self.logger.error(f"Error loading colaborador info using data manager. Error: {e}")
                return False, "errSubproc", str(e)
        except Exception as e:
            self.logger.error(f"Error loading colaborador info method: {e}", exc_info=True)
            return False, "errSubproc", str(e)
        
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
    
    def load_calendario_info(self, data_manager: BaseDataManager, process_id: int = 0, posto_id: int = 0, start_date: str = '', end_date: str = '', past_employee_id_list: List[int] = []) -> Tuple[bool, str, str]:
        """
        Load calendario from data manager and treat the data
        """
        try:
            self.logger.info(f"Starting load_calendario_info method.")
            
            # Validate input parameters
            # TODO: posto_id should come from auxiliary_data
            if posto_id == 0 or posto_id == None:
                self.logger.error("posto_id is 0, returning False. Check load_calendario_info call method.")
                return False, "errSubproc", "posto_id is 0 or None"
        
            # Get needed data
            try:
                self.logger.info("Loading tipo_contrato info from df_colaborador")
                employees_id_list = self.auxiliary_data['employees_id_list']
                matriculas_list = self.auxiliary_data['matriculas_list']
                employees_id_90_list = self.auxiliary_data['employees_id_90_list']
                employee_id_matriculas_map = self.auxiliary_data['employee_id_matriculas_map']
                first_date_passado = self.auxiliary_data['first_date_passado']
                last_date_passado = self.auxiliary_data['last_date_passado']
                past_employee_id_list = self.auxiliary_data['past_employee_id_list']
                case_type = self.auxiliary_data['case_type']

                # External call data values
                wfm_proc_colab = self.external_call_data['wfm_proc_colab']
                start_date_str = self.external_call_data['start_date']
                end_date_str = self.external_call_data['end_date']

                #df_tipo_contrato = df_colaborador[['emp', 'tipo_contrato']] # TODO: ensure it is needed
            except Exception as e:
                self.logger.error(f"Error processing colaborador info: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Treat past_employee_id_list str

            self.logger.info("Treating past_employee_id_list str. Creating colabs_str to be used in df_colaborador query.")
            colabs_passado_str = create_employee_query_string(past_employee_id_list)

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

            # Treatment df_calendario_passado
            try:
                self.logger.info("Treating df_calendario_passado")
                success, df_calendario_passado, error_msg = treat_df_calendario_passado(
                    df_calendario_passado=df_calendario_passado,
                    employees_id_list=employees_id_list,
                    past_employee_id_list=past_employee_id_list,
                    case_type=case_type,
                    wfm_proc_colab=wfm_proc_colab,
                    start_date=start_date_str,
                    end_date=end_date_str,
                )
                if not success:
                    self.logger.error(f"Calendario passado treatment failed: {error_msg}")
                    return False, "errSubproc", error_msg
            except Exception as e:
                self.logger.error(f"Error treating df_calendario_passado: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            
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

            try:
                self.logger.info("Treating df_ausencias_ferias")
                success, df_ausencias_ferias, error_msg = treat_df_ausencias_ferias(df_ausencias_ferias)
                if not success:
                    self.logger.error(f"Ausencias ferias treatment failed: {error_msg}")
                    return False, "errSubproc", error_msg
            except Exception as e:
                self.logger.error(f"Error treating ausencias_ferias: {e}", exc_info=True)
                return False, "errSubproc", "Error treating ausencias_ferias"

            if not df_ausencias_ferias.empty and not validate_df_ausencias_ferias(df_ausencias_ferias):
                self.logger.error(f"df_ausencias_ferias not valid: {df_ausencias_ferias}")
                return False, "errSubproc", "df_ausencias_ferias validation failed"

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

            if not validate_df_core_pro_emp_horario_det(df_core_pro_emp_horario_det):
                self.logger.error("df_core_pro_emp_horario_det validation failed")
                return False, "errSubproc", "df_core_pro_emp_horario_det validation failed"

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
                return True, "", ""
            except KeyError as e:
                self.logger.error(f"KeyError when saving calendario data: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except ValueError as e:
                self.logger.error(f"ValueError when saving calendario data: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except Exception as e:
                self.logger.error(f"Error saving calendario results to memory: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            
        except Exception as e:
            self.logger.error(f"Error in load_calendario_info method: {e}", exc_info=True)
            return False, "errSubproc", str(e)

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

    def load_calendario_transformations(self) -> Tuple[bool, str, str]:
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
                return False, "errSubproc", str(e)
            except Exception as e:
                self.logger.error(f"Error loading DataFrame in load_calendario_transformations: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                # Create df_calendario
                success, df_calendario, error_msg = create_df_calendario(
                    start_date=start_date, 
                    end_date=end_date,
                    employee_id_matriculas_map=employee_id_matriculas_map
                )
                if not success:
                    self.logger.error(f"Calendar creation failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add df_ausencias_ferias to df_calendario
                success, df_calendario, error_msg = add_ausencias_ferias(df_calendario, df_ausencias_ferias)
                if not success:
                    self.logger.error(f"Adding ausencias ferias failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add df_ciclos_90 to df_calendario
                success, df_calendario, error_msg = add_ciclos_90(df_calendario, df_ciclos_90)
                if not success:
                    self.logger.error(f"Adding ciclos 90 failed: {error_msg}")
                    return False, "errSubproc", error_msg
                
                # Add df_days_off to df_calendario
                success, df_calendario, error_msg = add_days_off(df_calendario, df_days_off)
                if not success:
                    self.logger.error(f"Adding days off failed: {error_msg}")
                    return False, "errSubproc", error_msg
                
                # Add df_calendario_passado to df_calendario
                success, df_calendario, error_msg = add_calendario_passado(df_calendario, df_calendario_passado)
                if not success:
                    self.logger.error(f"Adding calendario passado failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add df_core_pro_emp_horario_det to df_calendario
                success, df_calendario, error_msg = add_folgas_ciclos(df_calendario, df_core_pro_emp_horario_det)
                if not success:
                    self.logger.error(f"Adding folgas ciclos failed: {error_msg}")
                    return False, "errSubproc", error_msg
        
                # TODO: Save df_calendario to appropriate location and return success
                self.logger.info("Calendar transformations completed successfully")

                # Save df_calendario to raw_data
                try:
                    self.raw_data['df_calendario'] = df_calendario.copy()
                except KeyError as e:
                    self.logger.error(f"KeyError when saving df_calendario to raw_data: {e}", exc_info=True)
                    return False, "errSubproc", str(e)
                except ValueError as e:
                    self.logger.error(f"ValueError when saving df_calendario to raw_data: {e}", exc_info=True)
                    return False, "errSubproc", str(e)
                except Exception as e:
                    self.logger.error(f"Error saving df_calendario to raw_data: {e}", exc_info=True)
                    return False, "errSubproc", str(e)


                return True, "", ""
                
            except Exception as e:
                self.logger.error(f"Error in load_calendario_transformations method: {e}", exc_info=True)
                return False, "errSubproc", str(e)

        except Exception as e:
            self.logger.error(f"Error in load_calendario_transformations method: {e}", exc_info=True)
            return False, "errSubproc", str(e)


    def load_colaborador_transformations(self) -> Tuple[bool, str, str]:
        """
        """
        # Main try/except block, for general purposes
        try:
            # Get important information from memory
            try:
                # Variables
                employee_id_list = self.auxiliary_data['employee_id_list']
                unit_id = self.auxiliary_data['unit_id']
                convenio_bd  = self.auxiliary_data['GD_convenioBD']
                num_sundays_year = self.auxiliary_data['num_sundays_year']
                num_feriados_abertos = self.auxiliary_data['num_feriados_abertos']
                num_feriados_fechados = self.auxiliary_data['num_feriados_fechados']
                # Dataframes
                df_params_lq = self.auxiliary_data['df_params_lq']
                df_valid_emp = self.auxiliary_data['df_valid_emp']
                df_festivos = self.auxiliary_data['df_festivos']
                df_colaborador = self.raw_data['df_colaborador']
                # External call data
                start_date_str = self.external_call_data['start_date']
                end_date_str = self.external_call_data['end_date']
            except KeyError as e:
                self.logger.error(f"Missing required parameter in colaborador_transformations: {e}", exc_info=True)
                return False, "", ""
            except Exception as e:
                self.logger.error(f"", exc_info=True)
                return False, "", ""

            # Define which columns are going to be added:
            try:
                add_lq_column = False
                add_l_dom_salsa_column = True

                # Add lq column
                success, df_colaborador, error_msg = add_lqs_to_df_colaborador(
                    df_colaborador=df_colaborador, 
                    df_params_lq=df_params_lq, 
                    use_case=0
                )
                if not success:
                    self.logger.error(f"Adding lq column failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # TODO: Add params contrato column information
                success, df_colaborador, error_msg = set_tipo_contrato_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Adding tipo_contrato column failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # TODO: Add coluna prioridade folgas
                success, df_colaborador, error_msg = add_prioridade_folgas_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    df_valid_emp=df_valid_emp,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Adding prioridade_folgas column failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Set C2D values based on use case
                success, df_colaborador, error_msg = set_c2d_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Setting c2d failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Set C3D values based on convenio and use case
                success, df_colaborador, error_msg = set_c3d_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    convenio_bd=convenio_bd,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Setting c3d failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add L_D (working days) calculations
                success, df_colaborador, error_msg = add_l_d_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    convenio_bd=convenio_bd,
                    num_fer_dom=num_sundays_year,
                    fer_fechados=num_feriados_fechados,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Adding l_d failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add L_DOM (weekend/holiday days) calculations
                success, df_colaborador, error_msg = add_l_dom_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    df_festivos=df_festivos,
                    convenio_bd=convenio_bd,
                    num_fer_dom=num_sundays_year,
                    fer_fechados=num_feriados,
                    num_sundays=num_sundays_year,
                    start_date=start_date_str,
                    end_date=end_date_str,
                    count_open_holidays_func=count_open_holidays,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Adding l_dom failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add L_Q (quality leave) calculations
                success, df_colaborador, error_msg = add_l_q_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    convenio_bd=convenio_bd,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Adding l_q failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add L_TOTAL (total leave) calculations
                success, df_colaborador, error_msg = add_l_total_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    df_festivos=df_festivos,
                    convenio_bd=convenio_bd,
                    num_sundays=num_sundays_year,
                    num_fer_dom=num_sundays_year,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Adding l_total failed: {error_msg}")
                    return False, "errSubproc", error_msg


                # TODO: Add totals adjustments for admission date
                success, df_colaborador, error_msg = admission_date_adjustments_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    start_date=start_date_str,
                    end_date=end_date_str
                )
                if not success:
                    self.logger.error(f"Adding admission date adjustments failed: {error_msg}")
                    return False, "errSubproc", error_msg

                try:
                    self.raw_data['df_colaborador'] = df_colaborador.copy()
                except KeyError as e:
                    self.logger.error(f"KeyError when saving df_calendario to raw_data: {e}", exc_info=True)
                    return False, "errSubproc", str(e)
                except ValueError as e:
                    self.logger.error(f"ValueError when saving df_calendario to raw_data: {e}", exc_info=True)
                    return False, "errSubproc", str(e)
                except Exception as e:
                    self.logger.error(f"Error saving df_calendario to raw_data: {e}", exc_info=True)
                    return False, "errSubproc", str(e)

                return True, "", ""
            except Exception as e:
                self.logger.error(f"Error in load_colaborador_transformations method: {e}", exc_info=True)
                return False, "", ""
            pass
        except Exception as e:
            self.logger.error(f"Error in load_colaborador_transformations method: {e}", exc_info=True)
            return False, "", ""