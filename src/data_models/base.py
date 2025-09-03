"""
File containing the base data model class.
"""

# Dependencies
from typing import Dict, Any, Optional
import pandas as pd
import os
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.log_config import get_logger
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.data_manager.managers.managers import CSVDataManager, DBDataManager

# Local stuff
from src.config import PROJECT_NAME, CONFIG, ROOT_DIR
from src.data_models.functions.loading_functions import load_valid_emp_csv
from src.data_models.functions.helper_functions import count_dates_per_year

# Set up logger
logger = get_logger(PROJECT_NAME)


class BaseDescansosDataModel:
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