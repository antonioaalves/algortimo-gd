"""File containing the data model for Salsa"""

# Dependencies
from tracemalloc import start
from typing import Dict, Optional, Any, List, Tuple
import pandas as pd
import os

# Local stuff
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.data_manager.managers.managers import CSVDataManager, DBDataManager
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.log_config import get_logger
from src.data_models.base import BaseDescansosDataModel
from src.configuration_manager.base import BaseConfig
from src.configuration_manager.instance import get_config
from src.data_models.functions.helper_functions import (
    count_dates_per_year, 
    get_param_for_posto, 
    load_wfm_scheds, 
    get_valid_emp_info,
    get_first_and_last_day_passado_arguments,
    get_past_employees_id_list,
    get_employees_id_90_list,
    get_matriculas_for_employee_id,
    get_employee_id_matriculas_map_dict,
    create_employee_query_string,
    count_holidays_in_period,
    count_sundays_in_period,
    count_open_holidays,
    convert_fields_to_int
)
from src.data_models.functions.data_treatment_functions import (
    separate_df_ciclos_completos_folgas_ciclos,
    treat_df_valid_emp, 
    treat_df_closed_days, 
    treat_df_feriados,
    treat_df_contratos,
    treat_df_calendario_passado,
    treat_df_ausencias_ferias,
    treat_df_ciclos_completos,
    treat_df_folgas_ciclos,
    treat_df_colaborador,
    add_lqs_to_df_colaborador,
    set_tipo_contrato_to_df_colaborador,
    add_prioridade_folgas_to_df_colaborador,
    date_adjustments_to_df_colaborador,
    add_l_d_to_df_colaborador,
    add_l_dom_to_df_colaborador,
    add_l_q_to_df_colaborador, 
    add_l_total_to_df_colaborador,
    set_c2d_to_df_colaborador,
    set_c3d_to_df_colaborador,
    create_df_calendario,
    add_seq_turno,
    add_calendario_passado,
    add_ausencias_ferias,
    add_folgas_ciclos,
    add_ciclos_completos,
    add_days_off,
    adjust_estimativas_special_days,
    filter_df_dates,
    extract_tipos_turno,
    process_special_shift_types,
    add_date_related_columns,
    define_dia_tipo,
    merge_contract_data,
    adjust_counters_for_contract_types,
    handle_employee_edge_cases,
    adjust_horario_for_admission_date,
    calculate_and_merge_allocated_employees,
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
    validate_df_feriados,
    validate_df_ausencias_ferias,
    validate_df_ciclos_completos,
    validate_df_folgas_ciclos,
    validate_valid_emp_info,
    validate_num_sundays_year,
)
from src.data_models.validations.func_inicializa_validations import (
    validate_df_calendario_structure,
    validate_df_estimativas_structure,
    validate_df_colaborador_structure,
    validate_all_core_dataframes
)
#from src.config import PROJECT_NAME, CONFIG, ROOT_DIR

class SalsaDataModel(BaseDescansosDataModel):
    """"""

    def __init__(self, data_container: BaseDataContainer, project_name: str = 'algoritmo_GD', config_manager: BaseConfig = None, external_data: Dict[str, Any] = None):
        """Initialize the DescansosDataModel with data dictionaries for storing dataframes.
        
        Args:
            data_container: Container for storing intermediate data
            project_name: Name of the project
            config_manager: Configuration manager instance (uses singleton if None)
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
                - df_feriados: Holiday information
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

        # Use provided config_manager or get singleton instance
        self.config_manager = config_manager if config_manager is not None else get_config()
        
        super().__init__(data_container=data_container, project_name=project_name)
        # Static data, doesn't change during the process run but are essential for data model treatments - See data lifecycle to understand what this data is
        self.auxiliary_data = {
            'df_messages': pd.DataFrame(), # df containing messages to set process errors
            'df_valid_emp': None, # valid employees filtered for processing
            'df_params_lq': None, # LQ parameters
            'df_feriados': None, # holiday information dataframe
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
        # Use runtime external_data if provided, otherwise fall back to JSON defaults
        self.logger.info(f"DEBUGGING: config_manager: {self.config_manager}")
        if external_data:
            self.external_call_data = external_data
            self.logger.info(f"Using runtime external_data: current_process_id={external_data.get('current_process_id')}")
        else:
            self.external_call_data = self.config_manager.parameters.external_call_data if self.config_manager else {}
            self.logger.info(f"Using JSON defaults: current_process_id={self.external_call_data.get('current_process_id')}")
        
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
            df_messages = pd.read_csv(os.path.join(self.config_manager.system.project_root_dir, 'data', 'csvs', 'df_messages.csv'))
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
                    df_valid_emp = data_manager.load_data(
                        'valid_emp', 
                        query_file=self.config_manager.paths.sql_processing_paths['valid_emp'], 
                        process_id="'" + str(self.external_call_data['current_process_id']) + "'"
                    )
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


            self.logger.info(f"Loading important info into memory(unit_id, secao_id, posto_id_list, colabs_id_list, main_year)")
            # Save important this important info to be able to use it on querys
            unit_id, secao_id, posto_id_list, employees_id_by_posto_dict, employees_id_total_list = get_valid_emp_info(df_valid_emp)
            self.logger.info(f"unit_id: {unit_id}, secao_id: {secao_id}, posto_id_list: {posto_id_list} stored in variables")

            # TODO: check if this is needed or how can we adjust this to work with the other ones next
            if not validate_valid_emp_info(unit_id, secao_id, posto_id_list, employees_id_total_list):
                self.logger.error("Invalid valid_emp info")
                return False, "errSubproc", "Invalid valid_emp info"

            # Get start and end date
            start_date = self.external_call_data.get('start_date', '')
            end_date = self.external_call_data.get('end_date', '')
            wfm_proc_colab = self.external_call_data.get('wfm_proc_colab', None)
            self.logger.info(f"start_date: {start_date}, end_date: {end_date} stored in variables")

            first_year_date, last_year_date, main_year = count_dates_per_year(
                start_date_str=start_date, 
                end_date_str=end_date
            )
            
            # Get important arguments for calendario querys
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
                last_day_year_str=last_year_date,
                start_date_str=start_date,
                end_date_str=end_date
            )

            if not validate_num_sundays_year(num_sundays_year):
                self.logger.error(f"num_sundays_year is invalid: {num_sundays_year}")
                return False, "", ""

            # Load fk_colaborador-matricula mapping:
            try:
                self.logger.info(f"Loading fk_colaborador-matricula mapping from data manager")

                df_fk_colaborador_matricula = data_manager.load_data(
                    'df_fk_colaborador_matricula', 
                    query_file=self.config_manager.paths.sql_processing_paths['df_fk_colaborador_matricula'], 
                    colabs_id=create_employee_query_string(employees_id_total_list),
                )
                self.logger.info(f"df_fk_colaborador_matricula shape (rows {df_fk_colaborador_matricula.shape[0]}, columns {df_fk_colaborador_matricula.shape[1]}): {df_fk_colaborador_matricula.columns.tolist()}")

                success, employee_id_matriculas_map, error_msg = get_employee_id_matriculas_map_dict(df_fk_colaborador_matricula)
                if not success:
                    self.logger.error(f"fk_colaborador-matricula mapping treatment failed: {error_msg}")
                    return False, "errSubproc", error_msg
                
            except Exception as e:
                self.logger.error(f"Error loading fk_colaborador-matricula mapping: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Validate values
            if not validate_employees_id_list(employees_id_total_list):
                self.logger.error(f"employees_id_total_list is empty: {employees_id_total_list}")
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
                    df_params_lq = data_manager.load_data(
                        'params_lq', 
                        query_file=self.config_manager.paths.sql_processing_paths['params_lq'])
                else:
                    self.logger.error(f"No instance found for data_manager: {data_manager.__name__}")
                self.logger.info(f"df_params_lq shape (rows {df_params_lq.shape[0]}, columns {df_params_lq.shape[1]}): {df_params_lq.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_params_lq: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Load festivos information
            try:
                self.logger.info(f"Loading df_feriados from data manager")
                # TODO: join the other query and make only one df

                df_feriados = data_manager.load_data(
                    'df_feriados', 
                    query_file=self.config_manager.paths.sql_processing_paths['df_feriados'], 
                    unit_id="'" + str(unit_id) + "'", 
                    start_date=first_day_passado, 
                    end_date=last_day_passado)
                self.logger.info(f"df_feriados shape (rows {df_feriados.shape[0]}, columns {df_feriados.shape[1]}): {df_feriados.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_feriados: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            success, df_feriados, error_msg = treat_df_feriados(df_feriados)
            if not success:
                self.logger.error(f"Feriados treatment failed: {error_msg}")
                return False, "errSubproc", error_msg

            if not validate_df_feriados(df_feriados):
                self.logger.error(f"df_feriados is invalid: {df_feriados}")
                return False, "errSubproc", "df_feriados is invalid"

            num_feriados_abertos, num_feriados_fechados = count_holidays_in_period(
                start_date_str=first_year_date,
                end_date_str=last_year_date,
                df_feriados=df_feriados,
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
                df_closed_days = data_manager.load_data(
                    'df_closed_days', 
                    query_file=self.config_manager.paths.sql_processing_paths['df_closed_days'], 
                    unit_id="'" + str(unit_id) + "'"
                )
                self.logger.info(f"df_closed_days shape (rows {df_closed_days.shape[0]}, columns {df_closed_days.shape[1]}): {df_closed_days.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_closed_days: {e}", exc_info=True)
                return False, "errSubproc", str(e)

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

                df_params = data_manager.load_data(
                    'df_params', 
                    query_file=self.config_manager.paths.sql_processing_paths['params_df'], 
                    unit_id="'" + str(unit_id) + "'"
                )
                self.logger.info(f"df_params shape (rows {df_params.shape[0]}, columns {df_params.shape[1]}): {df_params.columns.tolist()}")

            except Exception as e:
                self.logger.error(f"Error loading parameters: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Load algorithm treatment params
            try:
                self.logger.info(f"Loading algorithm treatment params from data manager")
                parameters_cfg = data_manager.load_data(
                    'parameters_cfg', 
                    query_file=self.config_manager.paths.sql_processing_paths['parameters_cfg']
                )
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
                self.auxiliary_data['df_feriados'] = df_feriados.copy()
                self.auxiliary_data['df_closed_days'] = df_closed_days.copy()
                self.auxiliary_data['df_fk_colaborador_matricula'] = df_fk_colaborador_matricula.copy()
                self.auxiliary_data['unit_id'] = unit_id 
                self.auxiliary_data['secao_id'] = secao_id
                self.auxiliary_data['posto_id_list'] = posto_id_list
                self.auxiliary_data['main_year'] = main_year
                self.auxiliary_data['first_year_date'] = first_year_date
                self.auxiliary_data['last_year_date'] = last_year_date
                self.auxiliary_data['first_date_passado'] = first_day_passado
                self.auxiliary_data['last_date_passado'] = last_day_passado
                self.auxiliary_data['employees_id_total_list'] = employees_id_total_list
                self.auxiliary_data['employees_id_by_posto_dict'] = employees_id_by_posto_dict
                self.auxiliary_data['employee_id_matriculas_map'] = employee_id_matriculas_map.copy()
                self.auxiliary_data['case_type'] = case_type
                self.auxiliary_data['num_sundays_year'] = num_sundays_year
                self.auxiliary_data['num_feriados_abertos'] = num_feriados_abertos
                self.auxiliary_data['num_feriados_fechados'] = num_feriados_fechados
                
                # ALGORITHM TREATMENT PARAMS
                self.algorithm_treatment_params['admissao_proporcional'] = parameters_cfg
                self.algorithm_treatment_params['wfm_proc_colab'] = wfm_proc_colab
                self.algorithm_treatment_params['df_feriados'] = df_feriados.copy()

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
            algorithm_treatment_params = self.algorithm_treatment_params
            params_names_list = self.config_manager.parameters.get_parameter_names()
            params_defaults = self.config_manager.parameters.get_parameter_defaults()
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

                if param_name == 'NUM_DIAS_CONS':
                    algorithm_treatment_params['NUM_DIAS_CONS'] = int(param_value)
            self.logger.info(f"Treating parameters completed successfully")
            # Store algorithm_name in auxiliary_data for later use
            self.auxiliary_data['algorithm_name'] = algorithm_name
            # Store algorithm_treatment_params in auxiliary_data
            self.algorithm_treatment_params = algorithm_treatment_params
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
                employees_id_list_for_posto = self.auxiliary_data['employees_id_by_posto_dict'].get(posto_id, [])
                case_type = self.auxiliary_data['case_type']
                # External call data values
                first_date_passado = self.auxiliary_data['first_date_passado']
                last_date_passado = self.auxiliary_data['last_date_passado']
                process_id = self.external_call_data['current_process_id']
                wfm_proc_colab = self.external_call_data['wfm_proc_colab']
                df_valid_emp = self.auxiliary_data['df_valid_emp']
            except Exception as e:
                self.logger.error(f"Error loading colaborador info: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Get employee lists
            try:
                success, past_employees_id_list, error_msg = get_past_employees_id_list(
                    wfm_proc_colab=wfm_proc_colab,
                    df_valid_emp=df_valid_emp,
                    fk_tipo_posto=posto_id,
                    employees_id_list_for_posto=employees_id_list_for_posto,
                )
                if not success:
                    self.logger.error(f"Error getting past employees id list: {error_msg}")
                    return False, "errSubproc", error_msg

            except Exception as e:
                self.logger.error(f"Error getting past employees id list: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Load df_colaborador info from data manager
            try:
                # colaborador info
                self.logger.info(f"Loading df_colaborador info from data manager")
                df_colaborador = data_manager.load_data(
                    entity='df_colaborador', 
                    query_file=self.config_manager.paths.sql_raw_paths.get('df_colaborador'), 
                    colabs_id=create_employee_query_string(employee_id_list=past_employees_id_list)
                )
                self.logger.info(f"df_colaborador shape (rows {df_colaborador.shape[0]}, columns {df_colaborador.shape[1]}): {df_colaborador.columns.tolist()}")
                
            except Exception as e:
                self.logger.error(f"Error loading df_colaborador info from data source: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            
            # Load df_contratos info from data manager
            try:                                
                self.logger.info(f"Loading df_contratos info from data manager")
                df_contratos = data_manager.load_data(
                    entity='df_contratos', 
                    query_file=self.config_manager.paths.sql_auxiliary_paths.get('df_contratos'), 
                    colabs_id=create_employee_query_string(past_employees_id_list), 
                    start_date=first_date_passado, 
                    end_date=last_date_passado, 
                    process_id=process_id
                )
                self.logger.info(f"df_contratos shape (rows {df_contratos.shape[0]}, columns {df_contratos.shape[1]}): {df_contratos.columns.tolist()}")
                
                
            except Exception as e:
                self.logger.error(f"Error loading df_contratos: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            success, df_colaborador, error_msg = treat_df_colaborador(df_colaborador=df_colaborador, employees_id_list=past_employees_id_list)
            if not success:
                self.logger.error(f"Colaborador treatment failed: {error_msg}")
                return False, "errSubproc", error_msg

            # Get employees_id_90_list
            try:
                success, employees_id_90_list, error_msg = get_employees_id_90_list(
                    employees_id_list_for_posto=employees_id_list_for_posto, 
                    df_colaborador=df_colaborador,
                )
                if not success:
                    self.logger.error(f"Error getting employees id 90 list: {error_msg}")
                    return False, "errSubproc", error_msg

            except Exception as e:
                self.logger.error(f"Error creating employees_id_90_list from df_colaborador: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Quick validations
            if not validate_past_employee_id_list(past_employees_id_list, case_type):
                self.logger.error(f"past_employees_id_list is empty: {past_employees_id_list}")
                return False, "errSubproc", "past_employees_id_list is empty"

            # Saving values into memory
            try:
                self.logger.info(f"Saving df_colaborador in raw_data")

                self.raw_data['df_colaborador'] = df_colaborador.copy()
                self.auxiliary_data['df_contratos'] = df_contratos.copy()
                # TODO: Remove this, not used
                self.auxiliary_data['num_fer_doms'] = 0
                # Note: employee_id_matriculas_map and matriculas_list already in auxiliary_data from load_process_data
                self.auxiliary_data['employees_id_list_for_posto'] = employees_id_list_for_posto
                self.auxiliary_data['employees_id_90_list'] = employees_id_90_list
                self.auxiliary_data['past_employees_id_list'] = past_employees_id_list

                # Save important information in algorithm_treatment_params
                self.algorithm_treatment_params['employees_id_list_for_posto'] = employees_id_list_for_posto
                self.algorithm_treatment_params['employees_id_90_list'] = employees_id_90_list

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
    
    def load_calendario_info(self, data_manager: BaseDataManager, process_id: int = 0, posto_id: int = 0) -> Tuple[bool, str, str]:
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
                # Employee list lookup
                employees_id_list_for_posto = self.auxiliary_data['employees_id_list_for_posto']
                employees_id_90_list = self.auxiliary_data['employees_id_90_list']
                past_employees_id_list = self.auxiliary_data['past_employees_id_list']
                employee_id_matriculas_map = self.auxiliary_data['employee_id_matriculas_map']
                # Dates lookup
                first_date_passado = self.auxiliary_data['first_date_passado']
                last_date_passado = self.auxiliary_data['last_date_passado']
                # Case type lookup
                case_type = self.auxiliary_data['case_type']

                # External call data values
                process_id = self.external_call_data['current_process_id']
                wfm_proc_colab = self.external_call_data['wfm_proc_colab']
                start_date_str = self.external_call_data['start_date']
                end_date_str = self.external_call_data['end_date']

                #df_tipo_contrato = df_colaborador[['emp', 'tipo_contrato']] # TODO: ensure it is needed
            except Exception as e:
                self.logger.error(f"Error processing colaborador info: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info("Loading df_calendario_passado")
                df_calendario_passado = data_manager.load_data(
                    'df_calendario_passado', 
                    query_file=self.config_manager.paths.sql_auxiliary_paths['df_calendario_passado'], 
                    start_date=first_date_passado, 
                    end_date=last_date_passado, 
                    colabs=create_employee_query_string(past_employees_id_list)
                )
                self.logger.info(f"df_calendario_passado shape (rows {df_calendario_passado.shape[0]}, columns {df_calendario_passado.shape[1]}): {df_calendario_passado.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_calendario_passado: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Treatment df_calendario_passado
            try:
                self.logger.info("Treating df_calendario_passado")
                success, df_calendario_passado, error_msg = treat_df_calendario_passado(
                    df_calendario_passado=df_calendario_passado,
                    case_type=case_type,
                    wfm_proc_colab=wfm_proc_colab,
                    first_date_passado=first_date_passado,
                    last_date_passado=last_date_passado,
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
                
                # Convert employees_id_list_for_posto from List[str] to List[int]
                employees_id_for_posto_int = [int(emp_id) for emp_id in employees_id_list_for_posto]
                
                # Get filtered matriculas for employees_id_list_for_posto
                success, matriculas_for_posto, error_msg = get_matriculas_for_employee_id(
                    employee_id_list=employees_id_for_posto_int,
                    employee_id_matriculas_map=employee_id_matriculas_map
                )
                if not success:
                    self.logger.error(f"Error getting matriculas for posto: {error_msg}")
                    return False, "errSubproc", error_msg
                
                self.logger.info(f"Retrieved {len(matriculas_for_posto)} matriculas for employees_id_list_for_posto")
                
                # Ausencias ferias information
                df_ausencias_ferias = data_manager.load_data(
                    'df_ausencias_ferias', 
                    query_file=self.config_manager.paths.sql_auxiliary_paths['df_ausencias_ferias'], 
                    colabs_id=create_employee_query_string(matriculas_for_posto),
                    start_date=first_date_passado,
                    end_date=last_date_passado
                )
                self.logger.info(f"df_ausencias_ferias shape (rows {df_ausencias_ferias.shape[0]}, columns {df_ausencias_ferias.shape[1]}): {df_ausencias_ferias.columns.tolist()}")

            except Exception as e:
                self.logger.error(f"Error loading ausencias_ferias: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info("Treating df_ausencias_ferias")
                success, df_ausencias_ferias, error_msg = treat_df_ausencias_ferias(
                    df_ausencias_ferias=df_ausencias_ferias,
                    start_date=start_date_str,
                    end_date=end_date_str
                )
                if not success:
                    self.logger.error(f"Ausencias ferias treatment failed: {error_msg}")
                    return False, "errSubproc", error_msg
            except Exception as e:
                self.logger.error(f"Error treating ausencias_ferias: {e}", exc_info=True)
                return False, "errSubproc", "Error treating ausencias_ferias"

            if not df_ausencias_ferias.empty and not validate_df_ausencias_ferias(df_ausencias_ferias):
                self.logger.error(f"df_ausencias_ferias not valid: {df_ausencias_ferias}")
                return False, "errSubproc", "df_ausencias_ferias validation failed"

            try: 
                self.logger.info("Loading df_ciclos_completos_folgas_ciclos from data manager")
                df_ciclos_completos_folgas_ciclos = data_manager.load_data(
                    'df_ciclos_completos_folgas_ciclos', 
                    query_file=self.config_manager.paths.sql_auxiliary_paths['df_ciclos_completos_folgas_ciclos'], 
                    process_id=process_id, 
                    start_date=start_date_str, 
                    end_date=end_date_str,
                    colab90ciclo=create_employee_query_string(employees_id_90_list)
                )
                self.logger.info(f"df_ciclos_completos_folgas_ciclos shape (rows {df_ciclos_completos_folgas_ciclos.shape[0]}, columns {df_ciclos_completos_folgas_ciclos.shape[1]}): {df_ciclos_completos_folgas_ciclos.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_ciclos_completos_folgas_ciclos: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Separating initial df query into both dfs
            try:
                success, df_ciclos_completos, df_folgas_ciclos, error_msg = separate_df_ciclos_completos_folgas_ciclos(
                    df_ciclos_completos_folgas_ciclos=df_ciclos_completos_folgas_ciclos,
                    employees_id_90_list=employees_id_90_list
                )
                if not success:
                    self.logger.error(f"Error separating initial df query into both dfs: {error_msg}")
                    return False, "errSubproc", error_msg
            except Exception as e:
                self.logger.error(f"Error separating initial df query into both dfs: {e}", exc_info=True)
                return False, "errSubproc", str(e)


            try:
                self.logger.info("Treating ciclos completos")
                
                # Extract employee limits from df_colaborador
                df_colaborador = self.raw_data.get('df_colaborador')
                df_colaborador_limits = None
                
                if df_colaborador is not None and not df_colaborador.empty:
                    limit_columns = ['matricula', 'limite_superior_manha', 'limite_inferior_tarde']
                    available_columns = [col for col in limit_columns if col in df_colaborador.columns]
                    
                    if 'matricula' in available_columns:
                        df_colaborador_limits = df_colaborador[available_columns].copy()
                        self.logger.info(f"Extracted employee limits for {len(df_colaborador_limits)} employees")
                    else:
                        self.logger.warning("matricula column not found in df_colaborador - proceeding without limits")
                else:
                    self.logger.warning("df_colaborador not available - proceeding without employee limits")
                
                success, df_ciclos_completos, error_msg = treat_df_ciclos_completos(
                    df_ciclos_completos=df_ciclos_completos,
                )
                if not success:
                    self.logger.error(f"Ciclos completos treatment failed: {error_msg}")
                    return False, "errSubproc", error_msg
            except Exception as e:
                self.logger.error(f"Error treating ciclos 90: {e}", exc_info=True)
                return False, "errSubproc", "Error treating ciclos 90"

            if not validate_df_ciclos_completos(df_ciclos_completos):
                self.logger.error("df_folgas_ciclos validation failed")
                return False, "errSubproc", "df_folgas_ciclos validation failed"

            try:
                self.logger.info("Treating folgas ciclos")
                success, df_folgas_ciclos, error_msg = treat_df_folgas_ciclos(
                    df_folgas_ciclos=df_folgas_ciclos,
                )
                if not success:
                    self.logger.error(f"Folgas ciclos treatment failed: {error_msg}")
                    return False, "errSubproc", error_msg
            except Exception as e:
                self.logger.error(f"Error treating folgas ciclos: {e}", exc_info=True)
                return False, "errSubproc", "Error treating folgas ciclos"

            if not validate_df_folgas_ciclos(df_folgas_ciclos):
                self.logger.error("df_folgas_ciclos validation failed")
                return False, "errSubproc", "df_folgas_ciclos validation failed"

            try:
                self.logger.info("Loading df_days_off from data manager")
                
                # Convert employees_id_list_for_posto from List[str] to List[int] and get matriculas
                employees_id_for_posto_int = [int(emp_id) for emp_id in employees_id_list_for_posto]
                success, matriculas_for_posto, error_msg = get_matriculas_for_employee_id(
                    employee_id_list=employees_id_for_posto_int,
                    employee_id_matriculas_map=employee_id_matriculas_map
                )
                if not success:
                    self.logger.error(f"Error getting matriculas for posto (df_days_off): {error_msg}")
                    return False, "errSubproc", error_msg
                
                self.logger.info(f"Retrieved {len(matriculas_for_posto)} matriculas for employees_id_list_for_posto (df_days_off)")
                
                df_days_off = data_manager.load_data(
                    'df_days_off', 
                    query_file=self.config_manager.paths.sql_auxiliary_paths['df_days_off'], 
                    colabs_id=create_employee_query_string(matriculas_for_posto)
                )
                if df_days_off.empty:
                    df_days_off = pd.DataFrame(columns=pd.Index(['employee_id', 'schedule_dt', 'sched_type']))
                    self.logger.info("df_days_off was empty, created with default columns")
                else:
                    self.logger.info(f"df_days_off shape (rows {df_days_off.shape[0]}, columns {df_days_off.shape[1]}): {df_days_off.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_days_off: {e}", exc_info=True)
                df_days_off = pd.DataFrame()

            try:
                self.logger.info("Saving results to auxiliary_data and raw_data")
                # Saving results in memory
                self.auxiliary_data['df_calendario_passado'] = df_calendario_passado.copy() if not df_calendario_passado.empty else pd.DataFrame()
                self.auxiliary_data['df_ausencias_ferias'] = df_ausencias_ferias.copy()
                self.auxiliary_data['df_days_off'] = df_days_off.copy()
                self.auxiliary_data['df_ciclos_completos'] = df_ciclos_completos.copy()
                self.auxiliary_data['df_folgas_ciclos'] = df_folgas_ciclos.copy()
                
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
                employees_id_list_for_posto = self.auxiliary_data['employees_id_list_for_posto']
                past_employees_id_list = self.auxiliary_data['past_employees_id_list']
                # Dict
                employee_id_matriculas_map = self.auxiliary_data['employee_id_matriculas_map']
                # Strings
                start_date = self.external_call_data['start_date']
                end_date = self.external_call_data['end_date']
                main_year = self.auxiliary_data['main_year']
                # Dataframe
                df_calendario_passado = self.auxiliary_data['df_calendario_passado'].copy()
                df_ausencias_ferias = self.auxiliary_data['df_ausencias_ferias'].copy()
                df_folgas_ciclos = self.auxiliary_data['df_folgas_ciclos'].copy()
                df_ciclos_completos = self.auxiliary_data['df_ciclos_completos'].copy()
                df_days_off = self.auxiliary_data['df_days_off'].copy()
                df_feriados = self.auxiliary_data['df_feriados'].copy()
                df_colaborador = self.raw_data['df_colaborador'].copy()
                
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
                    main_year=main_year,
                    employee_id_matriculas_map=employee_id_matriculas_map,
                    past_employees_id_list=past_employees_id_list,
                    df_feriados=df_feriados,
                )
                if not success:
                    self.logger.error(f"Calendar creation failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add date-related columns (Step 3G from func_inicializa guide)
                success, df_calendario, error_msg = add_date_related_columns(
                    df=df_calendario,
                    date_col='schedule_day',
                    add_id_col=True,
                    use_case=0,
                    main_year=main_year
                )
                if not success:
                    self.logger.error(f"Failed to add date-related columns: {error_msg}")
                    return False, "errSubproc", error_msg

                success, df_calendario, error_msg = add_seq_turno(df_calendario, df_colaborador)
                if not success:
                    self.logger.error(f"Adding seq turno failed: {error_msg}")
                    return False, "", error_msg

                # Add df_ausencias_ferias to df_calendario
                success, df_calendario, error_msg = add_ausencias_ferias(df_calendario, df_ausencias_ferias)
                if not success:
                    self.logger.error(f"Adding ausencias ferias failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add df_ciclos_90 to df_calendario
                success, df_calendario, error_msg = add_ciclos_completos(df_calendario, df_ciclos_completos)
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
                success, df_calendario, error_msg = add_folgas_ciclos(df_calendario, df_folgas_ciclos)
                if not success:
                    self.logger.error(f"Adding folgas ciclos failed: {error_msg}")
                    return False, "errSubproc", error_msg
                
                # Filter by date range (Step 3B from func_inicializa guide)
                success, df_calendario, error_msg = filter_df_dates(
                    df=df_calendario,
                    first_date_str=start_date,
                    last_date_str=end_date,
                    date_col_name='schedule_day'  # Calendario uses schedule_day for schedule dates
                )
                if not success:
                    self.logger.error(f"Failed to filter calendario by dates: {error_msg}")
                    return False, "errSubproc", error_msg
                
                # Extract unique shift types (Step 3E from func_inicializa guide)
                success, tipos_turno_list, error_msg = extract_tipos_turno(
                    df_calendario=df_calendario,
                    tipo_turno_col='tipo_turno'
                )
                if not success:
                    self.logger.error(f"Failed to extract shift types: {error_msg}")
                    return False, "errSubproc", error_msg
                
                # Store tipos_turno in auxiliary_data for later use
                self.auxiliary_data['tipos_de_turno'] = tipos_turno_list
                self.logger.info(f"Stored tipos_de_turno in auxiliary_data: {tipos_turno_list}")
                
                # Define dia_tipo (Step 3H from func_inicializa guide)
                success, df_calendario, error_msg = define_dia_tipo(
                    df=df_calendario,
                    df_feriados=df_feriados,
                    date_col='schedule_day',
                    tipo_turno_col='tipo_turno',
                    horario_col='horario',
                    wd_col='wd'
                )
                if not success:
                    self.logger.error(f"Failed to define dia_tipo: {error_msg}")
                    return False, "errSubproc", error_msg
        
                # Note: Admission date adjustment (Step 6A) moved to func_inicializa
                # as it requires df_colaborador (cross-dataframe operation)
                
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
    
    def validate_matrices_loading(self) -> bool:
        """
        Validate that the required data is present, conforming, and valid.
        
        Returns:
            True if validation passes, False otherwise
        """
        # TODO: Implement actual validation logic
        return True


    def load_colaborador_transformations(self) -> Tuple[bool, str, str]:
        """
        """
        # Main try/except block, for general purposes
        try:
            # Get important information from memory
            try:
                # Variables
                unit_id = self.auxiliary_data['unit_id']
                convenio_bd  = self.auxiliary_data['GD_convenioBD']
                num_sundays_year = self.auxiliary_data['num_sundays_year']
                num_feriados_abertos = self.auxiliary_data['num_feriados_abertos']
                num_feriados_fechados = self.auxiliary_data['num_feriados_fechados']
                # Dataframes
                df_params_lq = self.auxiliary_data['df_params_lq']
                df_valid_emp = self.auxiliary_data['df_valid_emp']
                df_feriados = self.auxiliary_data['df_feriados']
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
                # Treat df_contratos if it exists
                if 'df_contratos' in self.auxiliary_data:
                    df_contratos = self.auxiliary_data['df_contratos'].copy()
                    success, df_contratos, error_msg = treat_df_contratos(df_contratos=df_contratos)
                    if not success:
                        self.logger.error(f"Treating df_contratos failed: {error_msg}")
                        return False, "errSubproc", error_msg
                    self.auxiliary_data['df_contratos'] = df_contratos.copy()
                
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
                    use_case=0
                )
                if not success:
                    self.logger.error(f"Adding l_d failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Add L_DOM (weekend/holiday days) calculations
                success, df_colaborador, error_msg = add_l_dom_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    df_feriados=df_feriados,
                    convenio_bd=convenio_bd,
                    num_fer_dom=num_sundays_year,
                    num_feriados=num_feriados_abertos+num_feriados_fechados,
                    num_feriados_fechados=num_feriados_fechados,
                    num_sundays=num_sundays_year,
                    start_date_str=start_date_str,
                    end_date_str=end_date_str,
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
                    df_feriados=df_feriados,
                    convenio_bd=convenio_bd,
                    num_sundays=num_sundays_year,
                    num_fer_dom=num_sundays_year,
                    use_case=1
                )
                if not success:
                    self.logger.error(f"Adding l_total failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # TODO: Add totals adjustments for admission date
                success, df_colaborador, error_msg = date_adjustments_to_df_colaborador(
                    df_colaborador=df_colaborador,
                    main_year=self.auxiliary_data['main_year']
                )
                if not success:
                    self.logger.error(f"Adding admission date adjustments failed: {error_msg}")
                    return False, "errSubproc", error_msg

                # Adjust contract type 3 employees (Step 5B from func_inicializa guide)
                success, df_colaborador, error_msg = adjust_counters_for_contract_types(
                    df_colaborador=df_colaborador,
                    tipo_contrato_col='tipo_contrato'
                )
                if not success:
                    self.logger.error(f"Contract type 3 adjustment failed: {error_msg}")
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

    def func_inicializa(self) -> Tuple[bool, str, str]:
        """
        """

        try: 
            self.logger.info("Starting func_inicializa processing.")

            try:
                # Load the 3 transformed dataframes
                df_calendario = self.raw_data['df_calendario'].copy()
                df_colaborador = self.raw_data['df_colaborador'].copy()
                df_estimativas = self.raw_data['df_estimativas'].copy()
                
                # Load df_contratos for merging (needed for Step 3I)
                df_contratos = self.auxiliary_data['df_contratos'].copy()

                main_year = self.auxiliary_data['main_year']
                start_date = self.external_call_data.get('start_date')
                end_date = self.external_call_data.get('end_date')

            except KeyError as e:
                self.logger.error(f"Missing required DataFrame in func_inicializa: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            except Exception as e:
                self.logger.error(f"Error loading DataFrame in func_inicializa: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Validate core dataframes structure (Step 3C - replaces old metadata row checks)
            try:
                self.logger.info("Validating core dataframes structure before cross-dataframe operations")
                valid, error_msg = validate_all_core_dataframes(
                    df_calendario=df_calendario,
                    df_estimativas=df_estimativas,
                    df_colaborador=df_colaborador,
                    start_date=start_date,
                    end_date=end_date
                )
                if not valid:
                    self.logger.error(f"Core dataframes validation failed: {error_msg}")
                    return False, "errValidation", error_msg
                    
                self.logger.info("Core dataframes validation passed ✓")
            except Exception as e:
                self.logger.error(f"Error validating core dataframes: {e}", exc_info=True)
                return False, "errValidation", str(e)

            # Merge contract data into calendario (Step 3I - cross-dataframe operation)
            success, df_calendario, error_msg = merge_contract_data(
                df_calendario=df_calendario,
                df_contratos=df_contratos,
                employee_col='employee_id',
                date_col='schedule_day'
            )
            if not success:
                self.logger.error(f"Failed to merge contract data: {error_msg}")
                return False, "errSubproc", error_msg

            # Handle employee edge cases (Step 5H - cross-dataframe operation)
            success, df_colaborador, df_calendario, error_msg = handle_employee_edge_cases(
                df_colaborador=df_colaborador,
                df_calendario=df_calendario,
                employee_col='employee_id',
                matricula_col='matricula'
            )
            if not success:
                self.logger.error(f"Failed to handle employee edge cases: {error_msg}")
                return False, "errSubproc", error_msg

            # Adjust HORARIO for admission dates (Step 6A - cross-dataframe operation)
            success, df_calendario, error_msg = adjust_horario_for_admission_date(
                df_calendario=df_calendario,
                df_colaborador=df_colaborador,
                employee_col='employee_id',
                date_col='schedule_day',
                horario_col='horario',
                dia_tipo_col='dia_tipo'
            )
            if not success:
                self.logger.error(f"Failed to adjust HORARIO for admission dates: {error_msg}")
                return False, "errSubproc", error_msg

            # Calculate +H and merge with estimativas (Step 6B/C/D - cross-dataframe operation)
            # TODO: Change this, param_pessoas_objetivo is not here!!!
            param_pess_obj = self.external_call_data.get('param_pessoas_objetivo', 0.5)
            success, df_estimativas, error_msg = calculate_and_merge_allocated_employees(
                df_estimativas=df_estimativas,
                df_calendario=df_calendario,
                date_col_est='data',
                date_col_cal='schedule_day',
                shift_col_est='turno',
                shift_col_cal='tipo_turno',
                employee_col='employee_id',
                horario_col='horario',
                param_pess_obj=param_pess_obj
            )
            if not success:
                self.logger.error(f"Failed to calculate and merge allocated employees: {error_msg}")
                return False, "errSubproc", error_msg


            # Final type coercion: ensure counters are stored as integers
            success, df_colaborador, error_msg = convert_fields_to_int(
                df=df_colaborador,
                fields=['ld', 'l_dom', 'lq', 'l_total', 'c2d', 'c3d', 'cxx']
            )
            if not success:
                self.logger.error(f"Final conversion of fields to int failed: {error_msg}")
                return False, "errSubproc", error_msg

            # Dateframe date filtering (3.b)
            try:
                pass
            except Exception as e:
                self.logger.error(f"Error in dataframe date filtering method: {e}", exc_info=True)
                return False, "", str(e)
            
            # Store processed dataframes in raw_data
            try:
                self.medium_data['df_colaborador'] = df_colaborador.copy()
                self.medium_data['df_calendario'] = df_calendario.copy()
                self.medium_data['df_estimativas'] = df_estimativas.copy()
                self.logger.info("Stored processed dataframes in raw_data")
            except Exception as e:
                self.logger.error(f"Error storing processed dataframes: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            
            # Save CSV files for debugging (similar to models.py)
            try:
                output_dir = self.config_manager.paths.get_output_dir()
                process_id = self.external_call_data.get("current_process_id", "")
                posto_id = self.auxiliary_data.get("current_posto_id", "")
                
                df_colaborador.to_csv(
                    os.path.join(output_dir, f'df_colaborador-{process_id}-{posto_id}.csv'),
                    index=False,
                    encoding='utf-8'
                )
                df_calendario.to_csv(
                    os.path.join(output_dir, f'df_calendario-{process_id}-{posto_id}.csv'),
                    index=False,
                    encoding='utf-8'
                )
                df_estimativas.to_csv(
                    os.path.join(output_dir, f'df_estimativas-{process_id}-{posto_id}.csv'),
                    index=False,
                    encoding='utf-8'
                )
                self.logger.info("CSV debug files saved successfully")
            except Exception as csv_error:
                self.logger.warning(f"Failed to save CSV debug files: {csv_error}")
            
            self.logger.info("func_inicializa completed successfully")
            return True, "", ""
            
        except Exception as e:
            self.logger.error(f"Error in func_inicializa method: {e}", exc_info=True)
            return False, "", ""

    def validate_func_inicializa(self) -> bool:
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