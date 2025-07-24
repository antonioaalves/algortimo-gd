"""Data models and operations for the my_new_project project.

This module contains data models, transformation functions, and utility methods
for working with the project's data structures.
"""

import logging
import pandas as pd
import numpy as np
import os
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from isoweek import Week
import json

# Import project-specific components
from src.config import PROJECT_NAME, CONFIG, ROOT_DIR
from src.helpers import (
    calcular_max, count_open_holidays, 
    insert_feriados, insert_closed_days, insert_holidays_absences,
    create_m0_0t, create_mt_mtt_cycles, assign_days_off, assign_empty_days,
    add_trads_code, assign_90_cycles, load_pre_ger_scheds, get_limit_mt,
    count_dates_per_year, load_wfm_scheds, func_turnos, adjusted_isoweek,
    custom_round, calcular_folgas2, calcular_folgas3, insert_holidays_absences, insert_closed_days,
    get_param_for_posto, convert_types_out, bulk_insert_with_query
)
from src.load_csv_functions.load_valid_emp import load_valid_emp_csv
from src.algorithms.factory import AlgorithmFactory
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.data_manager.managers.managers import CSVDataManager, DBDataManager
from base_data_project.storage.models import BaseDataModel
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.log_config import get_logger

#from src.services.example_service import AlgoritmoGDService

class DescansosDataModel(BaseDataModel):
    """
    Container for managing and transforming project data.
    
    This class provides a central place for data operations including:
    - Loading and validating data entities
    - Transforming data for analysis
    - Tracking data lineage and operations
    """
    
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
        # Data first stage
        self.raw_data: Dict[str, Any] = {
            'df_calendario': None,
            'df_colaborador': None,
            'df_estimativas': None
        }
        # Data second stage
        self.medium_data: Dict[str, Any] = {
            'df_calendario': None,
            'df_colaborador': None,
            'df_estimativas': None
        }
        # Data third stage
        self.rare_data: Dict[str, Any] = {
            'df_results': None,
            'stage1_schedule': None,
            'stage2_schedule': None,
        }
        # Data final stage
        self.formatted_data: Dict[str, Any] = {
            'df_final': None,  # Final output DataFrame
            'stage1_schedule': None,
            'stage2_schedule': None,
        }
        # External call data coming from the product
        self.external_call_data = external_data
        
        self.logger.info("DescansosDataModel initialized")
    
    def load_process_data(self, data_manager: BaseDataManager, entities_dict: Dict[str, str]) -> bool:
        """
        Load data from the data manager.
        
        Args:
            data_manager: The data manager instance
            enities: list of entities names to load
            
        Returns:
            True if successful, False otherwise
        """
        # Load messages df, from where?
        try:
            self.logger.info(f"Loading messages_df from data manager. Not implemented yet")
            messages_path = os.path.join(ROOT_DIR, 'data', 'csvs', '') # TODO: add the file path

            pass
        except Exception as e:
            self.logger.error(f"Error loading messages_df: {e}")
            return False
        

        try:
            self.logger.info("Loading process data from data manager")

            # Get entities to load from configuration
            if not entities_dict:
                self.logger.warning("No entities passed as argument")
                return False

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

                self.logger.info(f"valid_emp shape (rows {valid_emp.shape[0]}, columns {valid_emp.shape[1]}): {valid_emp.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading valid_emp: {e}", exc_info=True)
                return False

            # Load important info into memory
            try:
                self.logger.info(f"Loading important info into memory(unit_id, secao_id, posto_id_list, colabs_id_list, main_year)")
                # Save important this important info to be able to use it on querys
                unit_id = valid_emp['fk_unidade'].unique()[0]  # Get first (and only) unique value
                secao_id = valid_emp['fk_secao'].unique()[0]   # Get first (and only) unique value
                posto_id_list = valid_emp['fk_perfil'].unique().tolist()  # Get list of unique values
                self.logger.info(f"unit_id: {unit_id}, secao_id: {secao_id}, posto_id_list: {posto_id_list} stored in variables")

                if len(valid_emp['fk_unidade'].unique()) > 1 or len(valid_emp['fk_secao'].unique()) > 1:
                    self.logger.error("More than one fk_secao or fk_unidade associated with the process.")
                    raise ValueError

                # Get colab_ids list
                colabs_id_list = valid_emp['fk_colaborador'].unique().tolist()
                self.logger.info(f"colabs_id_list: {colabs_id_list} stored in variables")
                main_year = count_dates_per_year(start_date_str=self.external_call_data.get('start_date', ''), end_date_str=self.external_call_data.get('end_date', ''))
                self.logger.info(f"main_year: {main_year} stored in variables")
            except Exception as e:
                self.logger.error(f"Error loading important info into memory(unit_id, secao_id, posto_id_list, colabs_id_list, main_year): {e}", exc_info=True)
                return False

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
                return False

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
                return False

            # Load start_date2 and end_date2
            try:
                self.logger.info(f"Treating start_date2 and end_date2")
                start_date2 = pd.to_datetime(f"{main_year}-01-01")
                end_date2 = pd.to_datetime(f"{main_year}-12-31")
            except Exception as e:
                self.logger.error(f"Error treating start_date2 and end_date2: {e}", exc_info=True)
                return False

            # Load closed days information
            try:
                self.logger.info(f"Loading df_closed_days from data manager")
                query_path = entities_dict['df_closed_days']
                unit_id_str = "'" + str(unit_id) + "'"
                df_closed_days = data_manager.load_data('df_closed_days', query_file=query_path, unit_id=unit_id_str)
                self.logger.info(f"df_closed_days shape (rows {df_closed_days.shape[0]}, columns {df_closed_days.shape[1]}): {df_closed_days.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error loading df_closed_days: {e}", exc_info=True)
                return False

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
                return False

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
                return False

            # Copy the dataframes into the apropriate dict
            try:
                self.logger.info(f"Copying dataframes into the apropriate dict")
                # Copy the dataframes into the apropriate dict
                # TODO: should we ensure unit, secao e posto are only one value?
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

                if not self.auxiliary_data:
                    self.logger.warning("No data was loaded into auxiliary_data")
                    return False 
            except KeyError as e:
                self.logger.error(f"KeyError: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error copying dataframes into the apropriate dict, saving them in auxiliary_data: {e}", exc_info=True)
                return False
                
            self.logger.info(f"Successfully loaded {len(self.raw_data)} entities")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading process data: {str(e)}", exc_info=True)

            return False

    def validate_process_data(self):
        """
        Implement a validation method for process data loading
        """
        return True

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
                valid_emp = valid_emp[valid_emp['fk_perfil'] == posto_id]
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
            if start_date == '' or end_date == '':
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
    
    def load_calendario_info(self, data_manager: BaseDataManager, process_id: int = 0, posto_id: int = 0, start_date: str = '', end_date: str = '', colabs_passado: List[int] = []):
        """
        Load calendario from data manager and treat the data
        """
        try:
            self.logger.info(f"Starting load_calendario_info method.")
            
            # Validate input parameters
            if posto_id == 0:
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
    
    def load_estimativas_transformations(self) -> bool:
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
                fk_unidade = self.auxiliary_data['unit_id']
                fk_secao = self.auxiliary_data['secao_id'] 
                fk_tipo_posto = self.auxiliary_data['current_posto_id']
                
                self.logger.info(f"Parameters extracted - start_date: {start_date}, end_date: {end_date}, fk_unidade: {fk_unidade}, fk_secao: {fk_secao}, fk_tipo_posto: {fk_tipo_posto}")
            except KeyError as e:
                self.logger.error(f"Missing required parameter: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error extracting parameters: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Loading DataFrames from existing data")
                # Get DataFrames from existing data
                df_turnos = self.auxiliary_data['df_turnos'].copy()
                df_estrutura_wfm = self.auxiliary_data['df_estrutura_wfm'].copy()
                df_faixa_horario = self.auxiliary_data['df_faixa_horario'].copy()
                df_feriados = self.auxiliary_data['df_feriados'].copy()
                df_orcamento = self.auxiliary_data['df_orcamento'].copy()  # This is dfGranularidade equivalent - TODO: check if this is needed
                
                self.logger.info(f"DataFrames loaded - df_turnos: {df_turnos.shape}, df_estrutura_wfm: {df_estrutura_wfm.shape}, df_faixa_horario: {df_faixa_horario.shape}, df_feriados: {df_feriados.shape}, df_orcamento: {df_orcamento.shape}")
            except KeyError as e:
                self.logger.error(f"Missing required DataFrame: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error loading DataFrames: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Processing df_turnos data")
                # Filter df_turnos by fk_tipo_posto
                df_turnos = df_turnos[df_turnos['fk_tipo_posto'] == fk_tipo_posto].copy()
                self.logger.info(f"Filtered df_turnos by fk_tipo_posto {fk_tipo_posto}: {df_turnos.shape}")
                
                # Define time columns for min/max calculations
                columns_in = ["h_tm_in", "h_seg_in", "h_ter_in", "h_qua_in", "h_qui_in", "h_sex_in", "h_sab_in", "h_dom_in", "h_fer_in"]
                columns_out = ["h_tt_out", "h_seg_out", "h_ter_out", "h_qua_out", "h_qui_out", "h_sex_out", "h_sab_out", "h_dom_out", "h_fer_out"]
                self.logger.info(f"Time columns defined - in: {len(columns_in)}, out: {len(columns_out)}")
                
                # Calculate MinIN1 and MaxOUT2
                df_turnos['min_in1'] = df_turnos[columns_in].min(axis=1, skipna=True)
                df_turnos['max_out2'] = df_turnos[columns_out].max(axis=1, skipna=True)
                
                # Fill missing values for h_tm_out and h_tt_in
                df_turnos['h_tm_out'] = df_turnos['h_tm_out'].fillna(df_turnos['h_tt_out'])
                df_turnos['h_tt_in'] = df_turnos['h_tt_in'].fillna(df_turnos[columns_in].min(axis=1, skipna=True))
                
                # Select relevant columns
                df_turnos = df_turnos[['emp', 'fk_tipo_posto', 'min_in1', 'h_tm_out', 'h_tt_in', 'max_out2']].copy()
                
                # Fill remaining missing values
                df_turnos['min_in1'] = df_turnos['min_in1'].fillna(df_turnos['h_tt_in'])
                df_turnos['max_out2'] = df_turnos['max_out2'].fillna(df_turnos['h_tm_out'])
                
                self.logger.info("Basic time calculations completed")
            except Exception as e:
                self.logger.error(f"Error processing df_turnos data: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Converting time columns to datetime")
                # Convert time columns to datetime (using 2000-01-01 as base date)
                time_cols = ['min_in1', 'h_tm_out', 'h_tt_in', 'max_out2']
                for col in time_cols:
                    df_turnos[col] = pd.to_datetime('2000-01-01 ' + df_turnos[col].astype(str), format='%Y-%m-%d %H:%M:%S', errors='coerce')
                
                # Handle overnight shifts (add 24 hours if end time is before start time)
                mask_tm = df_turnos['h_tm_out'] < df_turnos['min_in1']
                df_turnos.loc[mask_tm, 'h_tm_out'] += timedelta(days=1)
                
                mask_max = df_turnos['max_out2'] < df_turnos['h_tt_in']
                df_turnos.loc[mask_max, 'max_out2'] += timedelta(days=1)
                
                self.logger.info("Time conversion and overnight shift handling completed")
            except Exception as e:
                self.logger.error(f"Error converting time columns: {e}", exc_info=True)
                return False
            
            # Group by fk_tipo_posto and calculate aggregated times
            df_turnos_grouped = df_turnos.groupby('fk_tipo_posto').agg({
                'min_in1': 'min',
                'h_tm_out': 'max', 
                'h_tt_in': 'min',
                'max_out2': 'max'
            }).reset_index()
            
            # Calculate MED1 and MED2
            df_turnos_grouped['med1'] = np.where(
                df_turnos_grouped['h_tm_out'] < df_turnos_grouped['h_tt_in'],
                df_turnos_grouped['h_tm_out'],
                df_turnos_grouped[['h_tm_out', 'h_tt_in']].min(axis=1)
            )
            
            df_turnos_grouped['med2'] = np.where(
                df_turnos_grouped['h_tm_out'] < df_turnos_grouped['h_tt_in'],
                df_turnos_grouped['h_tt_in'], 
                df_turnos_grouped[['h_tm_out', 'h_tt_in']].min(axis=1)
            )
            
            # Select and rename columns
            df_turnos = df_turnos_grouped[['fk_tipo_posto', 'min_in1', 'med1', 'med2', 'max_out2']].copy()
            
            # Calculate MED3
            df_turnos['med3'] = df_turnos['med1'].copy()
            df_turnos['med3'] = np.where(df_turnos['med3'] < df_turnos['med2'], df_turnos['med2'], df_turnos['med3'])
            df_turnos = df_turnos.drop('med2', axis=1)
            
            # Fill missing values
            df_turnos['med3'] = df_turnos['med3'].fillna(df_turnos['med1'])
            df_turnos['max_out2'] = df_turnos['max_out2'].fillna(df_turnos['med3'])
            df_turnos['med1'] = df_turnos['med1'].fillna(df_turnos['min_in1'])
            df_turnos['med3'] = df_turnos['med3'].fillna(df_turnos['max_out2'])
            
            # Merge with estrutura_wfm
            df_estrutura_wfm_filtered = df_estrutura_wfm[df_estrutura_wfm['fk_tipo_posto'] == fk_tipo_posto].copy()
            df_turnos = pd.merge(df_estrutura_wfm_filtered, df_turnos, on='fk_tipo_posto', how='left')
            
            # Create date sequence
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            df_data = pd.DataFrame({'data': date_range})
            df_data['wd'] = df_data['data'].dt.day_name().str.lower()
            
            # Add unit information
            df_unidade = pd.DataFrame({'fk_unidade': [fk_unidade]})
            df_data = df_data.assign(key=1).merge(df_unidade.assign(key=1), on='key').drop('key', axis=1)
            
            # Process holidays
            df_feriados_filtered = df_feriados[df_feriados['fk_unidade'] == fk_unidade].copy()
            
            if len(df_feriados_filtered) > 0:
                # df_feriados_filtered['data'] = pd.to_datetime(df_feriados_filtered['data'])

                if len(df_feriados_filtered) > 0:
                    # Check what columns are available
                    self.logger.info(f"df_feriados_filtered columns: {df_feriados_filtered.columns.tolist()}")
                    
                    # Check if 'data' column exists, otherwise try 'database'
                    if 'data' in df_feriados_filtered.columns:
                        df_feriados_filtered['data'] = pd.to_datetime(df_feriados_filtered['data'])
                    elif 'database' in df_feriados_filtered.columns:
                        df_feriados_filtered['data'] = pd.to_datetime(df_feriados_filtered['database'])
                    else:
                        self.logger.error(f"No date column found in df_feriados_filtered. Available columns: {df_feriados_filtered.columns.tolist()}")
                        # Set to empty DataFrame if no date column
                        df_feriados_filtered = pd.DataFrame()
                else:
                    self.logger.info("df_feriados_filtered is empty, skipping date conversion")                

                df_feriados_filtered['tipo_dia'] = 'feriado'
                
                # Filter holidays by date range
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                year = start_dt.year
                
                mask = ((df_feriados_filtered['data'] >= start_dt) & (df_feriados_filtered['data'] <= end_dt)) | \
                    (df_feriados_filtered['data'] < pd.to_datetime('2000-12-31'))
                df_feriados_filtered = df_feriados_filtered[mask].copy()
                df_feriados_filtered['data'] = pd.Series(df_feriados_filtered['data']).apply(lambda x: x.replace(year=year))
                
                # Merge with data
                df_data = pd.merge(df_data, pd.DataFrame(df_feriados_filtered[['fk_unidade', 'data', 'tipo_dia']]), 
                                on=['fk_unidade', 'data'], how='left')
                df_data['wd'] = df_data['tipo_dia'].fillna(df_data['wd'])
                df_data = df_data.drop('tipo_dia', axis=1)
            
            # Process faixa_horario
            self.logger.info(f"DEBUG: df_faixa_horario before filter:\n {df_faixa_horario}")
            df_faixa_horario_filtered = df_faixa_horario[df_faixa_horario['fk_secao'] == fk_secao].copy()
            
            # Expand date ranges in faixa_horario
            expanded_rows = []
            for _, row in df_faixa_horario_filtered.iterrows():
                date_range_fh = pd.date_range(start=row['data_ini'], end=row['data_fim'], freq='D')
                for date in date_range_fh:
                    new_row = row.copy()
                    new_row['data'] = date
                    expanded_rows.append(new_row)

            #self.logger.info(f"DEBUG: expanded_rows:\n {expanded_rows}")
            
            if expanded_rows:
                df_faixa_horario_expanded = pd.DataFrame(expanded_rows)
                
                # Reshape from wide to long format for time columns
                time_columns = ["aber_seg", "fech_seg", "aber_ter", "fech_ter", "aber_qua", "fech_qua", 
                            "aber_qui", "fech_qui", "aber_sex", "fech_sex", "aber_sab", "fech_sab", 
                            "aber_dom", "fech_dom", "aber_fer", "fech_fer"]

                self.logger.info(f"DEBUG: df_faixa_horario_expanded before melt:\n {df_faixa_horario_expanded}")
                
                df_faixa_long = pd.melt(df_faixa_horario_expanded, 
                                    id_vars=['fk_secao', 'data', 'data_ini', 'data_fim'],
                                    value_vars=time_columns,
                                    var_name='wd_ab', value_name='value')

                self.logger.info(f"DEBUG: df_faixa_long after melt:\n {df_faixa_long}")
                
                # Split wd_ab into action (aber/fech) and weekday
                df_faixa_long[['a_f', 'wd']] = df_faixa_long['wd_ab'].str.split('_', expand=True)

                self.logger.info(f"DEBUG: df_faixa_long after split:\n {df_faixa_long}")
                
                # Pivot back to get aber and fech columns
                df_faixa_wide = df_faixa_long.pivot_table(
                    index=['fk_secao', 'data', 'wd'], 
                    columns='a_f', 
                    values='value', 
                    aggfunc='first'
                ).reset_index()

                self.logger.info(f"DEBUG: df_faixa_wide after pivot:\n {df_faixa_wide}")
                
                # Clean column names
                df_faixa_wide.columns.name = None
                
                # Convert weekday names and match with actual dates
                df_faixa_wide['wd'] = df_faixa_wide['wd'].str.replace('sab', 'sáb')
                df_faixa_wide['wd_date'] = df_faixa_wide['data'].dt.day_name().str.lower()
                df_faixa_wide['wd_date'] = df_faixa_wide['wd_date'].str.replace('saturday', 'sáb')
                df_faixa_wide['wd_date'] = df_faixa_wide['wd_date'].str.replace('sunday', 'dom')
                df_faixa_wide['wd_date'] = df_faixa_wide['wd_date'].str.replace('monday', 'seg')
                df_faixa_wide['wd_date'] = df_faixa_wide['wd_date'].str.replace('tuesday', 'ter')
                df_faixa_wide['wd_date'] = df_faixa_wide['wd_date'].str.replace('wednesday', 'qua')
                df_faixa_wide['wd_date'] = df_faixa_wide['wd_date'].str.replace('thursday', 'qui')
                df_faixa_wide['wd_date'] = df_faixa_wide['wd_date'].str.replace('friday', 'sex')

                self.logger.info(f"DEBUG: df_faixa_wide after weekday replacement:\n {df_faixa_wide}")
                
                # Filter matching weekdays
                df_faixa_horario_final = df_faixa_wide[df_faixa_wide['wd'] == df_faixa_wide['wd_date']].copy()

                self.logger.info(f"DEBUG: df_faixa_horario_final after filter:\n {df_faixa_horario_final}")
                
                # Convert time columns to datetime
                df_faixa_horario_final['aber'] = pd.to_datetime(df_faixa_horario_final['aber'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                df_faixa_horario_final['fech'] = pd.to_datetime(df_faixa_horario_final['fech'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                
                df_faixa_horario_final = df_faixa_horario_final[['fk_secao', 'data', 'aber', 'fech']]
            else:
                df_faixa_horario_final = pd.DataFrame({col: [] for col in ['fk_secao', 'data', 'aber', 'fech']})

            self.logger.info(f"DEBUG: df_faixa_horario_final:\n {df_faixa_horario_final}")
            
            # Merge all data together
            df_turnos = pd.merge(df_turnos, df_data, on=['fk_unidade'], how='left')
            df_turnos = pd.merge(df_turnos, pd.DataFrame(df_faixa_horario_final), on=['fk_secao', 'data'], how='left')
            
            # Fill missing times with faixa_horario values
            df_turnos['max_out2'] = df_turnos['max_out2'].fillna(df_turnos['fech'])
            df_turnos['min_in1'] = df_turnos['min_in1'].fillna(df_turnos['aber'])
            
            # Calculate middle time
            df_turnos['middle_time'] = pd.to_datetime(
                (df_turnos['min_in1'].astype('int64') + df_turnos['max_out2'].astype('int64')) / 2,
                unit='ns'
            )
            
            # Round to nearest hour
            df_turnos['hour'] = df_turnos['middle_time'].dt.hour
            df_turnos['middle_time'] = pd.to_datetime('2000-01-01 ' + df_turnos['hour'].astype(str) + ':00:00')
            
            # Select final columns and rename
            df_turnos = df_turnos[['fk_unidade', 'unidade', 'fk_secao', 'secao', 'fk_tipo_posto', 'tipo_posto',
                                'min_in1', 'med1', 'med3', 'max_out2', 'middle_time', 'aber', 'fech', 'data']].copy()
            
            # Update med1 and med3 with middle_time
            df_turnos['med1'] = df_turnos['middle_time']
            df_turnos['med3'] = df_turnos['middle_time']
            df_turnos = df_turnos.drop('middle_time', axis=1)
            
            # Rename columns to match R output
            df_turnos.columns = ["fk_unidade", "unidade", "fk_secao", "secao", "fk_tipo_posto", "tipo_posto", 
                                "m_ini", "m_out", "t_ini", "t_out", "aber", "fech", "data"]
            
            # Reshape to long format for turnos
            df_turnos_long1 = pd.melt(pd.DataFrame(df_turnos), 
                                    id_vars=[col for col in df_turnos.columns if col not in ["m_ini", "t_ini"]], 
                                    value_vars=["m_ini", "t_ini"],
                                    var_name='turno', value_name='h_ini_1')
            
            df_turnos_long2 = pd.melt(pd.DataFrame(df_turnos),
                                    id_vars=[col for col in df_turnos.columns if col not in ["m_out", "t_out"]],
                                    value_vars=["m_out", "t_out"], 
                                    var_name='turno2', value_name='h_out_1')
            
            # Map turno names
            df_turnos_long1['turno'] = df_turnos_long1['turno'].replace({"m_ini": "m", "t_ini": "t"})
            df_turnos_long2['turno2'] = df_turnos_long2['turno2'].replace({"m_out": "m", "t_out": "t"})
            
            # Merge the two long formats
            common_cols = [col for col in df_turnos_long1.columns 
                        if col in df_turnos_long2.columns 
                        and col not in ['turno', 'h_ini_1', 'turno2', 'h_out_1']]

            df_turnos_final = pd.merge(df_turnos_long1, df_turnos_long2, on=common_cols, how='inner')
            
            # Filter matching turnos
            df_turnos_final = df_turnos_final[df_turnos_final['turno'] == df_turnos_final['turno2']].copy()
            df_turnos_final = df_turnos_final.drop('turno2', axis=1)
            
            # Filter out where start and end times are equal
            df_turnos_final = df_turnos_final[df_turnos_final['h_ini_1'] != df_turnos_final['h_out_1']].copy()
            
            # Handle overnight shifts
            df_turnos_final = pd.DataFrame(df_turnos_final)
            mask_overnight = df_turnos_final['h_ini_1'] > df_turnos_final['h_out_1']
            df_turnos_final.loc[mask_overnight, 'h_out_1'] += timedelta(days=1)
            
            # Update fech and aber based on turno
            df_turnos_final.loc[df_turnos_final['turno'] == 'M', 'fech'] = df_turnos_final.loc[df_turnos_final['turno'] == 'M', 'h_out_1']
            df_turnos_final.loc[df_turnos_final['turno'] == 'T', 'aber'] = df_turnos_final.loc[df_turnos_final['turno'] == 'T', 'h_ini_1']
            
            # Adjust times based on aber/fech constraints
            mask_m = df_turnos_final['turno'] == 'M'
            df_turnos_final.loc[mask_m, 'h_ini_1'] = df_turnos_final.loc[mask_m, ['h_ini_1', 'aber']].min(axis=1)
            
            mask_t = df_turnos_final['turno'] == 'T'
            df_turnos_final.loc[mask_t, 'h_out_1'] = df_turnos_final.loc[mask_t, ['h_out_1', 'fech']].max(axis=1)
            
            # Process granularity data (df_orcamento equivalent)
            # TODO: shouldnt it be from a query
            df_granularidade = self.auxiliary_data.get('df_granularidade', pd.DataFrame())
            #df_granularidade = df_orcamento[['fk_unidade', 'unidade', 'fk_secao', 'secao', 'fk_tipo_posto', 'tipo_posto', 
            #                                'data', 'hora_ini', 'pessoas_min', 'pessoas_estimado', 'pessoas_final']].copy()
            
            # Select relevant columns from df_turnos_final
            df_turnos_processing = df_turnos_final[['fk_tipo_posto', 'h_ini_1', 'h_out_1', 'turno', 'data']].copy()
            df_turnos_processing['fk_posto_turno'] = df_turnos_processing['fk_tipo_posto'].astype(str) + '_' + df_turnos_processing['turno']
            
            # Convert dates to proper format
            df_granularidade['data'] = pd.to_datetime(df_granularidade['data'])
            df_turnos_processing['data'] = pd.to_datetime(df_turnos_processing['data'])
            df_granularidade['hora_ini'] = pd.to_datetime(df_granularidade['hora_ini'])
            df_turnos_processing['h_ini_1'] = pd.to_datetime(df_turnos_processing['h_ini_1'])
            df_turnos_processing['h_out_1'] = pd.to_datetime(df_turnos_processing['h_out_1'])
            
            # Filter by fk_tipo_posto
            df_turnos_processing = df_turnos_processing[df_turnos_processing['fk_tipo_posto'] == fk_tipo_posto].copy()
            
            # Handle case where no turnos exist
            if len(df_turnos_processing) == 0:
                min_time = df_granularidade['hora_ini'].min()
                max_time = df_granularidade['hora_ini'].max()
                
                # Calculate middle time
                middle_seconds = (min_time.hour * 3600 + min_time.minute * 60 + 
                                max_time.hour * 3600 + max_time.minute * 60) / 2
                middle_hour = int(middle_seconds // 3600)
                middle_time = pd.to_datetime(f'2000-01-01 {middle_hour:02d}:00:00')
                
                # Create default turnos
                new_rows = [
                    {
                        'fk_tipo_posto': fk_tipo_posto,
                        'h_ini_1': min_time,
                        'h_out_1': middle_time,
                        'turno': 'M',
                        'data': None,
                        'fk_posto_turno': f'{fk_tipo_posto}_M'
                    },
                    {
                        'fk_tipo_posto': fk_tipo_posto,
                        'h_ini_1': middle_time,
                        'h_out_1': max_time,
                        'turno': 'T', 
                        'data': None,
                        'fk_posto_turno': f'{fk_tipo_posto}_T'
                    }
                ]
                df_turnos_processing = pd.DataFrame(new_rows)
            
            # Filter granularity data
            df_granularidade = df_granularidade[df_granularidade['fk_tipo_posto'] == fk_tipo_posto].copy()
            
            # Process each unique turno
            output_final = pd.DataFrame()
            
            df_turnos_processing = pd.DataFrame(df_turnos_processing)
            for i, fk_posto_turno in enumerate(df_turnos_processing['fk_posto_turno'].unique()):
                self.logger.info(f"Processing turno {i+1}: {fk_posto_turno}")
                
                df_turnos_f = pd.DataFrame(df_turnos_processing[df_turnos_processing['fk_posto_turno'] == fk_posto_turno].copy())
                fk_posto = pd.DataFrame(df_turnos_f)['fk_tipo_posto'].iloc[0]
                turno = pd.DataFrame(df_turnos_f)['turno'].iloc[0]
                
                # Filter granularity data for this posto
                df_granularidade_f = pd.DataFrame(df_granularidade[df_granularidade['fk_tipo_posto'] == fk_posto].copy())
                
                # Merge with turno data
                df_granularidade_f = pd.merge(df_granularidade_f, df_turnos_f, 
                                            on=['fk_tipo_posto', 'data'], how='inner')
                
                # Filter by time range
                time_mask = (df_granularidade_f['hora_ini'] >= df_granularidade_f['h_ini_1']) & \
                        (df_granularidade_f['hora_ini'] < df_granularidade_f['h_out_1'])
                df_granularidade_f = pd.DataFrame(df_granularidade_f[time_mask].copy())
                
                df_granularidade_f = df_granularidade_f.sort_values(['data', 'hora_ini'], ascending=[True, True]).drop_duplicates()
                df_granularidade_f['pessoas_final'] = pd.to_numeric(df_granularidade_f['pessoas_final'], errors='coerce')
                
                # Calculate statistics
                if len(df_granularidade_f) == 0:
                    output = pd.DataFrame({
                        'data': [],
                        'media_turno': [],
                        'max_turno': [],
                        'min_turno': [],
                        'sd_turno': []
                    })
                else:
                    output = df_granularidade_f.groupby('data').agg({
                        'pessoas_final': [
                            ('media_turno', 'mean'),
                            ('max_turno', lambda x: calcular_max(x.tolist())),
                            ('min_turno', 'min'),
                            ('sd_turno', 'std')
                        ]
                    }).reset_index()
                    
                    # Flatten column names
                    output.columns = ['data', 'media_turno', 'max_turno', 'min_turno', 'sd_turno']
                
                # Create complete date range
                date_range_complete = pd.date_range(start=start_date, end=end_date, freq='D')
                df_data_complete = pd.DataFrame({'data': date_range_complete})
                
                # Merge with output
                output = pd.merge(df_data_complete, output, on='data', how='left')
                output = output.fillna(0)
                
                output['turno'] = turno
                output['fk_tipo_posto'] = fk_posto
                
                output_final = pd.concat([output_final, output], ignore_index=True)
                
                self.logger.info(f"Completed processing turno {fk_posto_turno}")
            
            # Final processing
            if len(output_final) > 0:
                output_final['data_turno'] = output_final['data'].astype(str) + '_' + output_final['turno']
            
            output_final = output_final.fillna(0)
            output_final = output_final.drop_duplicates()
            output_final['fk_tipo_posto'] = fk_tipo_posto
            
            # Remove duplicates based on key columns
            output_final = output_final.drop_duplicates(['fk_tipo_posto', 'data', 'data_turno', 'turno'])
            
            # Convert numeric columns
            numeric_cols = ['max_turno', 'min_turno', 'media_turno', 'sd_turno']
            for col in numeric_cols:
                output_final[col] = pd.to_numeric(output_final[col], errors='coerce')
            
            try:
                self.logger.info("Storing results in appropriate class attributes")
                # Store processed turnos data in auxiliary_data
                self.auxiliary_data['df_turnos'] = df_turnos_processing.copy()
                self.auxiliary_data['df_feriados_filtered'] = df_feriados_filtered.copy()
                
                # Store final output matrix (matrizB_og equivalent) in raw_data
                self.raw_data['df_estimativas'] = output_final.copy()
                
                if not self.auxiliary_data or not self.raw_data:
                    self.logger.warning("Data storage verification failed")
                    return False
                    
                self.logger.info("load_estimativas_transformations completed successfully")
                self.logger.info(f"Stored df_turnos with shape: {df_turnos_processing.shape}")
                self.logger.info(f"Stored df_estimativas (matrizB_og) with shape: {output_final.shape}")
                
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

    def load_colaborador_transformations(self) -> bool:
        """
        Convert R loadMA_BD function to Python.
        Process employee matrix data with contract types, holidays, and labor calculations.
        
        Uses data from:
        - raw_data['df_colaborador']: Employee data (matrizA_og equivalent)
        - auxiliary_data['params_lq']: LQ parameters
        - auxiliary_data['df_festivos']: Holiday data
        - auxiliary_data['colabs_id_list']: Employee IDs list
        
        Stores results in:
        - raw_data['df_colaborador']: Updated processed employee matrix
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            #logger = get_logger(PROJECT_NAME)
            self.logger.info("Starting load_colaborador_transformations processing")
            
            try:
                self.logger.info("Extracting parameters from auxiliary_data and external_data")
                # Get parameters from auxiliary_data and external_data
                colabs_id = self.auxiliary_data['colabs_id_list']
                start_date = self.external_call_data['start_date']
                end_date = self.external_call_data['end_date']
                unit_id = self.auxiliary_data['unit_id']
                
                self.logger.info(f"Parameters extracted - colabs_id: {len(colabs_id) if colabs_id else 0}, start_date: {start_date}, end_date: {end_date}, unit_id: {unit_id}")
            except KeyError as e:
                self.logger.error(f"Missing required parameter in colaborador_transformations: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error extracting parameters: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Loading DataFrames from existing data")
                # Get DataFrames from existing data
                matriz_ma = self.raw_data['df_colaborador'].copy()
                params_lq = self.auxiliary_data['params_lq'].copy()
                matriz_festivos = self.auxiliary_data['df_festivos'].copy()
                
                self.logger.info(f"DataFrames loaded - matriz_ma: {matriz_ma.shape}, params_lq: {params_lq.shape}, matriz_festivos: {matriz_festivos.shape}")
            except KeyError as e:
                self.logger.error(f"Missing required DataFrame in colaborador_transformations: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error loading DataFrames: {e}", exc_info=True)
                return False
                
            try:
                self.logger.info("Setting up global variables and configuration")
                # Global variables that would be passed from external context
                # TODO: These should be configured in your config or passed as parameters
                #convenio_bd = 'ALCAMPO'  # You may need to set this appropriately
                convenio_bd = self.auxiliary_data.get('GD_convenioBD', 'ALCAMPO')
                wfm_user = self.external_call_data.get('wfm_user', 'WFM')
                wfm_proc_id = self.external_call_data.get('wfm_proc_id', 0)
                path_ficheiros_global = ''  # Configure as needed
                
                self.logger.info(f"Global variables set - convenio_bd: {convenio_bd}, wfm_user: {wfm_user}, wfm_proc_id: {wfm_proc_id}")
            except Exception as e:
                self.logger.error(f"Error setting up global variables: {e}", exc_info=True)
                return False
            
            # Params mapping from db values to what the algorithm needs
            params_lq_aux = pd.DataFrame({
                'seq_turno': ['M', 'T', 'MT', 'MMT', 'MTT', 'CICLO'],
                'bd_values': ['SQ_TURNO_M', 'SQ_TURNO_T', 'SQ_TURNO_MT', 'SQ_TURNO_MMT', 'SQ_TURNO_MTT', 'CICLO']
            })
            
            # Process params_lq if available
            if len(params_lq) == 0:
                self.logger.error('No params stored in database')
                return False
            
            # Merge params_lq with auxiliary mapping
            params_lq = pd.merge(params_lq, params_lq_aux, 
                            left_on='sys_p_name', right_on='bd_values', how='left')
            params_lq = params_lq[['seq_turno', 'numbervalue']].copy()
            params_lq.columns = ['seq_turno', 'lq']
            
            # Calculate number of holidays
            if len(matriz_festivos) == 0:
                self.logger.warning('No festivos stored in database')
                nr_festivos = 0
            else:
                # Filter festivos that are not Sundays (weekday != 0 in pandas)
                matriz_festivos['data'] = pd.to_datetime(matriz_festivos['data'])
                non_sunday_festivos = matriz_festivos[matriz_festivos['data'].dt.weekday != 6]  # Sunday is 6 in pandas
                nr_festivos = len(non_sunday_festivos['data'].unique())
            
            # Create day sequence from start_date to end of year
            start_dt = pd.to_datetime(start_date)
            end_of_year = pd.to_datetime(f"{start_dt.year}-12-31")
            day_seq = pd.date_range(start=start_dt, end=end_of_year, freq='D')
            
            df1 = pd.DataFrame({
                'day_seq': day_seq,
                'wd': pd.Series(day_seq).dt.dayofweek + 1  # Convert to 1-7 where 1=Monday, 7=Sunday
            })
            
            # Params for contract types
            params_contrato = pd.DataFrame({
                'min': [2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 6],
                'max': [2, 3, 4, 3, 4, 5, 6, 4, 5, 6, 5, 6, 6],
                'tipo_contrato': [2, 3, 4, 3, 4, 5, 4, 4, 5, 6, 5, 6, 6]
            })
            
            # Check if matriz_ma is empty
            if len(matriz_ma) == 0:
                self.logger.error('None of the colabs ids are present at CORE_ALGORITHM_VARIABLES.')
                return False
            
            # Check for missing collaborators
            unique_colabs = matriz_ma['fk_colaborador'].unique()
            if not all(colab in unique_colabs for colab in colabs_id):
                missing_colabs = [colab for colab in colabs_id if colab not in unique_colabs]
                self.logger.warning(f'Colabs {missing_colabs} not present in CORE_ALGORITHM_VARIABLES, proceeding...')
            
            self.logger.info(f"DEBUG: matriz_ma columns: {matriz_ma.columns}")
            # Rename columns to ensure compatibility
            expected_columns = [
                "fk_colaborador", "unidade", "secao", "posto", "convenio", "nome", "matricula",
                "min_dia_trab", "max_dia_trab", "tipo_turno", "seq_turno", "t_total", "l_total",
                "dyf_max_t", "lqs", "q", "c2d", "c3d", "cxx", "semana_1", "out", "ciclo", 
                "data_admissao", "data_demissao", "dofhc"
            ]
            
            # Map current columns to expected columns if they differ
            if list(matriz_ma.columns) != expected_columns:
                # Create a mapping - you may need to adjust this based on your actual column names
                column_mapping = {
                    'fk_colaborador': 'fk_colaborador',
                    'loja': 'unidade', 
                    'secao': 'secao',
                    'puesto': 'posto',
                    'convenio': 'convenio',
                    'nome': 'nome',
                    'emp': 'matricula',
                    'min_dias_trabalhados': 'min_dia_trab',
                    'max_dias_trabalhados': 'max_dia_trab',
                    'tipo_de_turno': 'tipo_turno',
                    'seq_turno': 'seq_turno',
                    't_total': 't_total',
                    'l_total': 'l_total',
                    'dyf_max_t': 'dyf_max_t',
                    'lq': 'lqs',
                    'q': 'q',
                    'fds_cal_2d': 'c2d',
                    'fds_cal_3d': 'c3d',
                    'd_cal_xx': 'cxx',
                    'semana_1': 'semana_1',
                    'out': 'out',
                    'ciclo': 'ciclo',
                    'data_admissao': 'data_admissao',
                    'data_demissao': 'data_demissao',
                    'dofhc': 'dofhc'
                }
                
                # Rename columns that exist in the mapping
                for old_col, new_col in column_mapping.items():
                    if old_col in matriz_ma.columns:
                        matriz_ma = matriz_ma.rename(columns={old_col: new_col})
            
            # Transform data types
            matriz_ma['convenio'] = matriz_ma['convenio'].str.upper()
            matriz_ma['min_dia_trab'] = pd.to_numeric(matriz_ma['min_dia_trab'], errors='coerce')
            matriz_ma['max_dia_trab'] = pd.to_numeric(matriz_ma['max_dia_trab'], errors='coerce')
            matriz_ma['dyf_max_t'] = pd.to_numeric(matriz_ma['dyf_max_t'], errors='coerce')
            matriz_ma['c2d'] = pd.to_numeric(matriz_ma['c2d'], errors='coerce')
            matriz_ma['c3d'] = pd.to_numeric(matriz_ma['c3d'], errors='coerce')
            matriz_ma['cxx'] = pd.to_numeric(matriz_ma['cxx'], errors='coerce')
            matriz_ma['lqs'] = pd.to_numeric(matriz_ma['lqs'], errors='coerce')
            
            # Convert dates
            matriz_ma['data_admissao'] = pd.to_datetime(matriz_ma['data_admissao'], errors='coerce')
            matriz_ma['data_demissao'] = pd.to_datetime(matriz_ma['data_demissao'], errors='coerce')
            
            # Add EMP column (padded zeros) - implement pad_zeros equivalent
            matriz_ma['emp'] = matriz_ma['matricula'].astype(str)  # Adjust padding as needed
            # TODO: analyse matricula information for adding
            
            # Fill missing min/max working days
            matriz_ma['min_dia_trab'] = matriz_ma['min_dia_trab'].fillna(matriz_ma['max_dia_trab'])
            matriz_ma['max_dia_trab'] = matriz_ma['max_dia_trab'].fillna(matriz_ma['min_dia_trab'])
            
            # Merge with params_lq
            matriz_ma = pd.merge(matriz_ma, params_lq, on='seq_turno', how='left')
            
            # Merge with params_contrato
            matriz_ma = pd.merge(matriz_ma, params_contrato, 
                            left_on=['min_dia_trab', 'max_dia_trab'], 
                            right_on=['min', 'max'], how='left')
            
            # Fill missing values (except date columns)
            date_columns = ['data_admissao', 'data_demissao']
            non_date_columns = [col for col in matriz_ma.columns if col not in date_columns]
            matriz_ma[non_date_columns] = matriz_ma[non_date_columns].fillna(0)
            
            # Validate seq_turno
            seq_turno_zeros = bool((matriz_ma['seq_turno'] == 0).any())
            seq_turno_nulls = bool(matriz_ma['seq_turno'].isna().any())
            if seq_turno_zeros or seq_turno_nulls:
                self.logger.error("seq_turno=0 or null - columna SEQ_TURNO mal parametrizada")
                return False
            
            # Validate tipo_contrato
            contrato_zeros = bool((matriz_ma['tipo_contrato'] == 0).any())
            contrato_nulls = bool(matriz_ma['tipo_contrato'].isna().any())
            if contrato_zeros or contrato_nulls:
                self.logger.error("contrato=0 or null - columna TIPO_CONTRATO mal parametrizada")
                return False
            
            # Calculate numFerDom and ferFechados
            num_sundays = len(df1[df1['wd'] == 7])  # Sunday is 7 (current Python indexing)
            num_fer_dom = nr_festivos + num_sundays
            
            # Calculate closed holidays (tipo == 3 and not Sunday)
            if len(matriz_festivos) > 0:
                closed_festivos = matriz_festivos[
                    (matriz_festivos['tipo'] == 3) & 
                    (matriz_festivos['data'].dt.weekday != 6)  # Not Sunday
                ]
                fer_fechados = len(closed_festivos['data'].unique())
            else:
                fer_fechados = 0
            
            # Update LQ based on seq_turno
            special_turnos = ['CICLO', 'MOT', 'P']
            mask_special = matriz_ma['seq_turno'].str.upper().isin(special_turnos)
            matriz_ma.loc[mask_special, 'lq'] = matriz_ma.loc[mask_special, 'lqs']
            matriz_ma = matriz_ma.drop('lqs', axis=1)
            
            # Process each collaborator
            processed_rows = []
            
            for colab in matriz_ma['fk_colaborador'].unique():
                cc = matriz_ma[matriz_ma['fk_colaborador'] == colab].copy()
                
                if len(cc) == 0:
                    continue
                    
                cc = cc.iloc[0:1].copy()  # Take first row if multiple
                
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                
                # Adjust for admission date
                if pd.notna(cc['data_admissao'].iloc[0]) and start_dt < cc['data_admissao'].iloc[0]:
                    days_from_admission = (end_dt - cc['data_admissao'].iloc[0]).days + 1
                    total_days = (end_dt - start_dt).days + 1
                    div = days_from_admission / total_days
                    
                    cc['dyf_max_t'] = np.ceil(cc['dyf_max_t'] * div)
                    cc['lq'] = np.ceil(cc['lq'] * div)
                    cc['c2d'] = np.ceil(cc['c2d'] * div)
                    cc['c3d'] = np.ceil(cc['c3d'] * div)

                    if (cc['c3d'] + cc['c2d']).iloc[0] > cc['lq'].iloc[0]:
                        cc['lq'] = cc['c2d'].iloc[0] + cc['c3d'].iloc[0]
                        self.logger.error(f"Empleado {cc['matricula'].iloc[0]} sin suficiente LQ para fines de semana de calidad. Recalculated l_total: {cc['l_total'].iloc[0]}")
                
                # Apply business logic: C2D = C2D + C3D
                cc['c2d'] = cc['c2d'] + cc['c3d']
                
                tipo_contrato = cc['tipo_contrato'].iloc[0]
                convenio = cc['convenio'].iloc[0]
                
                # Process based on contract type and convention
                if tipo_contrato == 6 and convenio == convenio_bd:
                    cc['ld'] = cc['dyf_max_t']
                    cc['l_dom'] = num_fer_dom - cc['dyf_max_t'] - fer_fechados
                    cc['lq_og'] = cc['lq'].copy()
                    cc['lq'] = cc['lq'] - (cc['c2d'] + cc['c3d'])
                    cc['l_total'] = num_fer_dom + cc['lq'] + cc['c2d'] + cc['c3d']
                    
                    if cc['lq'].iloc[0] < 0:
                        cc['c3d'] = cc['c3d'] + cc['lq']
                        cc['lq'] = 0
                        # Recalculate l_total after LQ adjustment
                        #cc['l_total'] = cc['lq'] + cc['c2d'] + cc['c3d']
                        self.logger.warning(f"Empleado {cc['matricula'].iloc[0]} sin suficiente LQ para fines de semana de calidad. Recalculated l_total: {cc['l_total'].iloc[0]}")
                
                elif tipo_contrato in [5, 4] and convenio == convenio_bd:
                    self.logger.info("teste entre novo convenio")
                    cc['ld'] = cc['dyf_max_t']
                    cc['l_dom'] = num_fer_dom - cc['dyf_max_t'] - fer_fechados
                    cc['lq_og'] = 0
                    cc['lq'] = 0
                    cc['l_total'] = num_sundays * (7 - tipo_contrato)
                
                elif tipo_contrato in [3, 2] and convenio == convenio_bd:
                    if len(matriz_festivos) > 0:
                        coh = count_open_holidays(matriz_festivos, tipo_contrato)
                        cc['dyf_max_t'] = 0
                        cc['q'] = 0
                        cc['lq_og'] = 0
                        cc['lq'] = 0
                        cc['c2d'] = 0
                        cc['c3d'] = 0
                        cc['cxx'] = 0
                        cc['ld'] = 0
                        cc['l_dom'] = coh[0]
                        cc['l_total'] = coh[1] - coh[0]
                        # Log potential negative l_total from holiday calculation
                        if cc['l_total'].iloc[0] < 0:
                            self.logger.warning(f"Employee {cc['matricula'].iloc[0]}: negative l_total ({cc['l_total'].iloc[0]}) from holidays calc (coh[1]:{coh[1]} - coh[0]:{coh[0]})")
                    else:
                        cc['dyf_max_t'] = 0
                        cc['q'] = 0
                        cc['lq_og'] = 0
                        cc['lq'] = 0
                        cc['c2d'] = 0
                        cc['c3d'] = 0
                        cc['cxx'] = 0
                        cc['ld'] = 0
                        cc['l_dom'] = 0
                        cc['l_total'] = 0
                
                # Nota: não está a atualizar l_total para lq's negativos
                elif tipo_contrato == 6 and convenio == 'SABECO':
                    cc['ld'] = cc['dyf_max_t']
                    cc['l_dom'] = num_fer_dom - cc['dyf_max_t'] - fer_fechados
                    cc['c3d'] = 0
                    cc['lq'] = 0
                    cc['lq_og'] = 0
                    cc['l_total'] = num_fer_dom + cc['c2d']
                
                elif tipo_contrato in [5, 4] and convenio == 'SABECO':
                    cc['ld'] = cc['dyf_max_t']
                    cc['l_dom'] = num_fer_dom - cc['dyf_max_t'] - fer_fechados
                    cc['c3d'] = 0
                    cc['lq'] = 0
                    cc['lq_og'] = 0
                    cc['l_total'] = num_sundays * (7 - tipo_contrato) + 8  # 8 is hardcoded per business rule
                
                elif tipo_contrato in [3, 2] and convenio == 'SABECO':
                    coh = count_open_holidays(matriz_festivos, tipo_contrato)
                    cc['dyf_max_t'] = 0
                    cc['q'] = 0
                    cc['lq'] = 0
                    cc['lq_og'] = 0
                    cc['c2d'] = 0
                    cc['c3d'] = 0
                    cc['cxx'] = 0
                    cc['ld'] = 0
                    cc['l_dom'] = coh[0]
                    cc['l_total'] = coh[1] - coh[0]
                    # Log potential negative l_total from holiday calculation
                    if cc['l_total'].iloc[0] < 0:
                        self.logger.warning(f"Employee {cc['matricula'].iloc[0]}: negative l_total ({cc['l_total'].iloc[0]}) from SABECO holidays calc (coh[1]:{coh[1]} - coh[0]:{coh[0]})")
                    # Preserve dofhc value
                    cc['dofhc'] = cc['dofhc']
                
                processed_rows.append(cc)

            self.logger.info(f"columnes matriz a: {matriz_ma.columns.tolist()}")
            
            # Combine all processed rows
            if processed_rows:
                matriz_ma_final = pd.concat(processed_rows, ignore_index=True)
            else:
                matriz_ma_final = pd.DataFrame()
                
            # Final validation - check for negative L_DOM
            if len(matriz_ma_final) > 0 and 'l_dom' in matriz_ma_final.columns and (matriz_ma_final['l_dom'] < 0).any():
                self.logger.error("l_dom < 0 - columna DyF_MAX_T mal parametrizada")
                return False
            
            try:
                self.logger.info("Storing final results")
                # Store result in raw_data
                self.raw_data['df_colaborador'] = matriz_ma_final.copy()
                self.auxiliary_data['num_fer_doms'] = num_fer_dom
                self.logger.info(f"DEBUG: matriz_ma_final columns: {matriz_ma_final.columns.tolist()}")
                self.logger.info(f"DEBUG: matriz_ma_final dtypes:\n{matriz_ma_final.dtypes}")
                
                # Log data with better formatting - using CSV format for readability
                self.logger.info("DEBUG: matriz_ma_final data (CSV format):")
                self.logger.info(matriz_ma_final.to_csv(sep='\t', index=False))
                
                if not self.raw_data or not self.auxiliary_data:
                    self.logger.warning("Data storage verification failed")
                    return False
                
                self.logger.info(f"load_colaborador_transformations completed successfully. Processed {len(matriz_ma_final)} employees.")
                return True
            except KeyError as e:
                self.logger.error(f"KeyError when storing colaborador results: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError when storing colaborador results: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error storing final results: {e}", exc_info=True)
                return False
            
        except Exception as e:
            self.logger.error(f"Error in load_colaborador_transformations method: {e}", exc_info=True)
            return False

    def load_calendario_transformations(self) -> bool:
        try:
            # TODO: associate to self
            #logger = get_logger(PROJECT_NAME)
            self.logger.info("Starting load_calendario_transformations processing")
            
            try:
                self.logger.info("Extracting input parameters")
                # DEBUG: Check inputs
                start_date = self.external_call_data['start_date']
                end_date = self.external_call_data['end_date']
                self.logger.info(f"Date parameters - start_date: {start_date}, end_date: {end_date}")
            except KeyError as e:
                self.logger.error(f"Missing required parameter in calendario_transformations: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error extracting input parameters: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Loading required DataFrames")
                matriz_ma = self.raw_data['df_colaborador'].copy()
                df_ciclos_90 = self.auxiliary_data['df_ciclos_90'].copy()
                #df_feriados_filtered = self.auxiliary_data['df_feriados_filtered'].copy()
                df_festivos = self.auxiliary_data['df_festivos'].copy()
                df_closed_days = self.auxiliary_data['df_closed_days'].copy()
                df_ausencias_ferias = self.auxiliary_data['df_ausencias_ferias'].copy()
                df_days_off = self.auxiliary_data['df_days_off'].copy()
                start_date2 = self.auxiliary_data['start_date2']
                end_date2 = self.auxiliary_data['end_date2']
                current_year = pd.to_datetime(end_date2, format="%Y-%m-%d").year
                
                self.logger.info(f"DataFrames loaded - matriz_ma: {matriz_ma.shape}, df_ciclos_90: {df_ciclos_90.shape}, df_festivos: {df_festivos.shape}, df_closed_days: {df_closed_days.shape}, df_ausencias_ferias: {df_ausencias_ferias.shape}, df_days_off: {df_days_off.shape}")
                self.logger.info(f"Date parameters - start_date2: {start_date2}, end_date2: {end_date2}, current_year: {current_year}")
            except KeyError as e:
                self.logger.error(f"Missing required DataFrame in calendario_transformations: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error loading DataFrames: {e}", exc_info=True)
                return False

            # Select columns 
            df_tipo_contrato = (matriz_ma[['matricula', 'tipo_contrato']]
                            .dropna(subset=['tipo_contrato']))
            df_tipo_contrato.columns = ['emp', 'tipo_contrato']

            #self.logger.info(f"DEBUG: matriz_ma shape={matriz_ma.shape}")
            
            # This is probably where it's failing - check if 'emp' column exists
            if 'emp' not in matriz_ma.columns:
                self.logger.error(f"'emp' column not found. Available columns: {matriz_ma.columns.tolist()}")
                # Try 'matricula' instead
                if 'matricula' in matriz_ma.columns:
                    matriz_ma['emp'] = matriz_ma['matricula']
                    #self.logger.info("DEBUG: Used 'matricula' as 'emp'")
                else:
                    #self.logger.error("DEBUG: Neither 'emp' nor 'matricula' found")
                    return False
            
            all_colab_pad = matriz_ma['emp'].tolist()
            self.logger.info(f"all_colab_pad length={len(all_colab_pad)}")
            
            # Create the basic matrix structure
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            date_range = pd.date_range(start=start_dt, end=end_dt, freq='D')
            self.logger.info(f"DEBUG: date_range length={len(date_range)}")
            
            # Create header rows
            dia_row = ['Dia']
            for date in date_range:
                date_str = date.strftime('%Y-%m-%d')
                dia_row.extend([date_str, date_str])
            
            turno_row = ['TURNO']
            for _ in date_range:
                turno_row.extend(['M', 'T'])
            
            self.logger.info(f"dia_row length=\n{len(dia_row)}, turno_row length={len(turno_row)}")
            
            # Create DataFrame
            reshaped_final_3 = pd.DataFrame([dia_row, turno_row])
            reshaped_final_3.columns = range(len(reshaped_final_3.columns))

            self.logger.info(f"DEBUG: Initial reshaped_final_3={reshaped_final_3}")
            
            # Add TIPO_DIA row
            # tipo_dia_row = ['-'] * reshaped_final_3.shape[1]
            # tipo_dia_row[0] = "TIPO_DIA"
            # new_row_df = pd.DataFrame([tipo_dia_row], columns=reshaped_final_3.columns)
            # reshaped_final_3 = pd.concat([reshaped_final_3, new_row_df], ignore_index=True)
            
            #self.logger.info(f"DEBUG: After headers, matrix shape={reshaped_final_3.shape}")
            #self.logger.info(f"DEBUG: reshaped_final_3 first few rows:\n{reshaped_final_3.head()}")

            matriculas_90_cycles = []
            
            if len(df_ciclos_90) > 0:
                
                # Convert day number according to BD standard to R standard
                df_ciclos_90['dia_semana'] = df_ciclos_90['dia_semana'].apply(
                    lambda x: x - 6 if x == 7 else (x if x == 8 else x + 1)
                )
                
                # Get unique employees with 90-day cycles
                colabs_90_cycle = df_ciclos_90['employee_id'].unique().tolist()
                
                # Convert schedule_day to datetime
                df_ciclos_90['schedule_day'] = pd.to_datetime(df_ciclos_90['schedule_day'].astype(str))
                
                # Process each employee with 90-day cycles
                for colab in colabs_90_cycle:
                    # Filter cycle info for this employee and date range
                    df_cycle90_info_filtered = df_ciclos_90[
                        (df_ciclos_90['employee_id'] == colab) &
                        (df_ciclos_90['schedule_day'] >= pd.to_datetime(start_date)) &
                        (df_ciclos_90['schedule_day'] <= pd.to_datetime(end_date))
                    ].copy()
                    
                    if len(df_cycle90_info_filtered) == 0:
                        continue
                        
                    # Get matricula for this employee
                    matricula = df_cycle90_info_filtered['matricula'].iloc[0]
                    matriculas_90_cycles.append(matricula)
                    
                    # Get MT limits using helper function
                    lim_sup_manha, lim_inf_tarde = get_limit_mt(matricula, matriz_ma)
                    
                    # Create new row for this matricula
                    row_filling = ['-'] * (reshaped_final_3.shape[1] - 1)
                    new_row = [str(matricula)] + row_filling
                    
                    # Add new row to matrix
                    new_row_df = pd.DataFrame([new_row], columns=reshaped_final_3.columns)
                    reshaped_final_3 = pd.concat([reshaped_final_3, new_row_df], ignore_index=True)
                    
                    # Get the row index for this matricula
                    reshaped_row_index = None
                    for idx, row in reshaped_final_3.iterrows():
                        if str(matricula) in row.values:
                            reshaped_row_index = idx
                            break
                    
                    if reshaped_row_index is None:
                        continue
                        
                    # Process each day in the filtered cycle info
                    for schedule_day in df_cycle90_info_filtered['schedule_day']:
                        day = schedule_day.strftime('%Y-%m-%d')
                        
                        # Find column index for this day
                        reshaped_col_index = []
                        first_row = reshaped_final_3.iloc[0]
                        matching_cols = first_row[first_row == day].index.tolist()
                        if matching_cols:
                            reshaped_col_index = matching_cols
                        
                        if len(reshaped_col_index) == 0:
                            continue
                        
                        #self.logger.info(f"DEBUG: reshaped_col_index: {reshaped_col_index}")

                        # Assign 90-day cycles using helper function
                        reshaped_final_3 = assign_90_cycles(
                            reshaped_final_3, 
                            df_cycle90_info_filtered,
                            colab, 
                            df_festivos,
                            lim_sup_manha, 
                            lim_inf_tarde,
                            day, 
                            reshaped_col_index, 
                            [reshaped_row_index], 
                            matricula
                        )

                    #self.logger.info(f"DEBUG: reshaped final after 90 cycles {reshaped_final_3}")
            
            # TODO: add rest of the logic here

            # Add employee rows: TODO: maybe is going away
            #for emp in all_colab_pad:
            #    emp_row = ['-'] * reshaped_final_3.shape[1]
            #    emp_row[0] = emp
            #    emp_row_df = pd.DataFrame([emp_row], columns=reshaped_final_3.columns)
            #    reshaped_final_3 = pd.concat([reshaped_final_3, emp_row_df], ignore_index=True)

            df_alg_variables_DB = matriz_ma.copy()
            df_alg_variables_filtered = df_alg_variables_DB[
                ~df_alg_variables_DB['emp'].isin(matriculas_90_cycles)
            ][['emp', 'seq_turno', 'semana_1']].copy()

            #self.logger.info(f"DEBUG: reshaped_final_3 after 90 cycles: {reshaped_final_3}")
            #self.logger.info(f"DEBUG: df_alg_variables_filtered pre mt_mtt_cycles: {df_alg_variables_filtered}")
            if len(df_alg_variables_filtered) > 0:
                reshaped_final_3 = create_mt_mtt_cycles(df_alg_variables_filtered, reshaped_final_3)

            #self.logger.info(f"DEBUG: reshaped_final_3 after mt_mtt_cycles: {reshaped_final_3}")

            # TODO: introduce actual substitution logic functions from helpers

            # This piece of code places 0 when the shift is either M or T
            reshaped_final_3 = create_m0_0t(reshaped_final_3)

            #self.logger.info(f"DEBUG: reshaped_final_3 after create_m0_0t: {reshaped_final_3}")

            # TODO: add comments explaining this part
            start_date = pd.to_datetime(f"{current_year}-01-01")
            end_date = pd.to_datetime(f"{current_year}-12-31")

            # TODO: Remove this or test the logic
            # TODO: add empty df_ausencias_ferias

            ausencias_total = pd.DataFrame(df_ausencias_ferias[(df_ausencias_ferias['data_ini'] >= start_date) & 
                                    (df_ausencias_ferias['data_ini'] <= end_date)])

            # feriados
            df_festivos['data'] = pd.to_datetime(df_festivos['data'])

            mask = ((df_festivos['data'] >= start_date) & (df_festivos['data'] <= end_date)) | \
                    (df_festivos['data'] < pd.to_datetime('2000-12-31'))
            df_festivos_filtered = df_festivos[mask].copy()
            df_festivos_filtered['data'] = pd.Series(df_festivos_filtered['data']).apply(lambda x: x.replace(year=current_year))

            ause_colab = ausencias_total[ausencias_total['matricula'] == '0156020']
            #self.logger.info(f"DEBUG: ause_colab {ause_colab}")
            reshaped_final_3 = insert_holidays_absences(all_colab_pad, ausencias_total, reshaped_final_3)

            #reshaped_final_3.to_csv(os.path.join('data', 'output', 'reshaped_final_3_holydays.csv'), index=False, encoding='utf-8')            

            #self.logger.info(f"DEBUG: reshaped_final_3 after holy: {reshaped_final_3}")

            #self.logger.info(f"DEBUG: df_feriados_filtered {df_festivos_filtered}")

            if len(df_festivos_filtered) > 0:
                reshaped_final_3 = insert_feriados(df_festivos_filtered, reshaped_final_3)
            else:
                default_names = [f"Column{i}" for i in range(len(reshaped_final_3.columns))]
                new_row = pd.DataFrame(['-'] * len(reshaped_final_3.columns)).T
                
                new_row.iloc[0, 0] = "TIPO_DIA"
                upper_bind = reshaped_final_3.iloc[[0]].copy()
                lower_bind = reshaped_final_3.iloc[1:].copy()
                
                upper_bind.columns = default_names
                lower_bind.columns = default_names
                new_row.columns = default_names
                
                reshaped_final_3 = pd.concat([upper_bind, new_row, lower_bind], ignore_index=True)
                reshaped_final_3.columns = range(len(reshaped_final_3.columns))

            self.logger.info(f"DEBUG: reshaped_final_3 after insert_feriados: {reshaped_final_3}")
            #reshaped_final_3.to_csv(os.path.join('data', 'output', 'reshaped_final_3_feriados.csv'), index=False, encoding='utf-8')

            if len(df_closed_days) > 0:
                reshaped_final_3 = insert_closed_days(df_closed_days, reshaped_final_3)

            self.logger.info(f"DEBUG: reshaped_final_3 after insert_closed_days: {reshaped_final_3}")

            if len(df_tipo_contrato) > 0 and len(df_tipo_contrato.columns) > 0:
                # TODO: check this not in pre_ger logic
                reshaped_final_3 = assign_empty_days(df_tipo_contrato, reshaped_final_3, all_colab_pad, df_festivos_filtered)

            self.logger.info(f"DEBUG: reshaped_final_3 after assign_empty_days: {reshaped_final_3}")

            reshaped_final_3 = insert_holidays_absences(all_colab_pad, ausencias_total, reshaped_final_3)

            self.logger.info(f"DEBUG: reshaped_final_3 after insert_holidays_absences: {reshaped_final_3}")

            if len(reshaped_final_3) > 0:
                
                self.logger.info(f"df_days_off {df_days_off}")
                df_days_off_filtered = pd.DataFrame(df_days_off[(df_days_off['schedule_dt'] >= start_date) & 
                                    (df_days_off['schedule_dt'] <= end_date)])
                if len(df_days_off_filtered) > 0:
                    reshaped_final_3 = assign_days_off(reshaped_final_3, df_days_off_filtered)
            
            self.logger.info(f"Final matrix={reshaped_final_3}")
            
            # Simple validation
            if reshaped_final_3.iloc[0, 0] != 'Dia':
                self.logger.error(f"Header validation failed. [0,0]={reshaped_final_3.iloc[0, 0]}")
                return False
            
            if reshaped_final_3.iloc[1, 0] != 'TIPO_DIA':
                self.logger.error(f"Header validation failed. [1,0]={reshaped_final_3.iloc[1, 0]}")
                return False

            if reshaped_final_3.iloc[2, 0] != 'TURNO':
                self.logger.error(f"Header validation failed. [1,0]={reshaped_final_3.iloc[1, 0]}")
                return False

            try:
                self.logger.info("Storing final results")
                # Store result
                self.raw_data['df_calendario'] = reshaped_final_3.copy()
                
                if not self.raw_data:
                    self.logger.warning("Data storage verification failed")
                    return False
                    
                self.logger.info(f"df_calendario shape stored after transformations: {reshaped_final_3.shape}")
                #self.logger.info(f"DEBUG: Stored df_calendario=\n{reshaped_final_3}")
                self.logger.info("load_calendario_transformations: Successfully stored df_calendario")
                
                return True
            except KeyError as e:
                self.logger.error(f"KeyError when storing calendario results: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError when storing calendario results: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error storing calendario transformations results: {e}", exc_info=True)
                return False
            
        except Exception as e:
            self.logger.error(f"Error in load_calendario_transformations method: {e}", exc_info=True)
            return False
    
    def _log_and_clip_l_total(self, df, operation_name, clip_negative=True):
        """Log negative l_total values and optionally clip them"""
        if 'l_total' in df.columns:
            neg_count = (df['l_total'] < 0).sum()
            if neg_count > 0:
                neg_employees = df[df['l_total'] < 0]['matricula'].tolist() if 'matricula' in df.columns else []
                min_val = df['l_total'].min()
                self.logger.warning(f"After {operation_name}: {neg_count} employees with negative l_total (min: {min_val}). Employees: {neg_employees[:5]}{'...' if len(neg_employees) > 5 else ''}")
                
                if clip_negative:
                    df['l_total'] = df['l_total'].clip(lower=0)
                    self.logger.info(f"Clipped negative l_total values to 0 after {operation_name}")
        return df

    def validate_matrices_loading(self) -> bool:
        """
        Validate that the required data is present, conforming, and valid.
        
        Returns:
            True if validation passes, False otherwise
        """
        return True

    def func_inicializa(self, start_date: str, end_date: str, fer) -> bool:
        """
        Python translation of R funcInicializa function.
        Initializes matrices and performs data transformations.
        
        Args:
            start_date: Start date string
            end_date: End date string  
            fer: Holiday data
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info("Starting func_inicializa processing")
            
            try:
                self.logger.info("Importing required libraries")
                import pandas as pd
                import numpy as np
                from datetime import datetime
                self.logger.info("Libraries imported successfully")
            except ImportError as e:
                self.logger.error(f"Failed to import required libraries: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error during library import: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Validating input parameters")
                if not start_date or not end_date:
                    self.logger.error(f"Invalid date parameters - start_date: {start_date}, end_date: {end_date}")
                    return False
                if fer is None:
                    self.logger.warning("fer parameter is None, proceeding with empty holiday data")
                    fer = pd.DataFrame()
                    
                self.logger.info(f"Input parameters validated - start_date: {start_date}, end_date: {end_date}, fer: {type(fer)}")
            except Exception as e:
                self.logger.error(f"Error validating input parameters: {e}", exc_info=True)
                return False
            
            try:
                self.logger.info("Loading matrices from existing data")
                # Get matrices from existing data
                matriz2_og = self.raw_data['df_calendario'].copy()
                matrizB_og = self.raw_data['df_estimativas'].copy() 
                matrizA_og = self.raw_data['df_colaborador'].copy()

                #DEBUG - TODO: remove
                matrizB_og.to_csv(os.path.join('data', 'output', f'df_estimativas_debug_inicializa-{self.external_call_data.get("current_process_id", "")}-{self.auxiliary_data.get("current_posto_id", "")}.csv'), index=False, encoding='utf-8')

                self.logger.info(f"Matrices loaded - matriz2_og (columns: {matriz2_og.shape[0]}, rows: {matriz2_og.shape[1]}), matrizB_og ( columns: {matrizB_og.shape[0]}, rows: {matrizB_og.shape[1]}), matrizA_og ( columns: {matrizA_og.shape[0]}, rows: {matrizA_og.shape[1]})")
            except KeyError as e:
                self.logger.error(f"Missing required data matrix in func_inicializa: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error loading matrices from existing data: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Debugging matrizB_og (df_estimativas)")
                # Debug: Check matrizB_og (df_estimativas)
                self.logger.info("=== Debug matrizB_og (df_estimativas) ===")
                self.logger.info(f"Shape: {matrizB_og.shape}")
                self.logger.info(f"Columns: {matrizB_og.columns.tolist()}")
                self.logger.info(f"First few rows:\n{matrizB_og.head()}")
                self.logger.info("=====================================")
            except Exception as e:
                self.logger.error(f"Error during matrizB_og debugging: {e}", exc_info=True)
                return False

            try:
                self.logger.info("Validating matriz2_og structure")
                # TODO: Remove this debug code
                # Debug: Check the structure of matriz2_og
                self.logger.info(f"matriz2_og shape: {matriz2_og.shape}")
                self.logger.info(f"matriz2_og first few rows:\n{matriz2_og.head()}")
                self.logger.info(f"matriz2_og first column unique values: {matriz2_og.iloc[:, 0].unique()}")
                
                # Check if TURNO and Dia rows exist
                turno_exists = (matriz2_og.iloc[:, 0] == 'TURNO').any()
                dia_exists = (matriz2_og.iloc[:, 0] == 'Dia').any()
                
                self.logger.info(f"TURNO row exists: {turno_exists}")
                self.logger.info(f"Dia row exists: {dia_exists}")
                
                if not turno_exists or not dia_exists:
                    self.logger.error("Required header rows (TURNO/Dia) not found in matriz2_og")
                    return False
                
                # Find TURNO and Dia rows
                turno_row_idx = matriz2_og[matriz2_og.iloc[:, 0] == 'TURNO'].index[0]
                dia_row_idx = matriz2_og[matriz2_og.iloc[:, 0] == 'Dia'].index[0]
                
                self.logger.info(f"Header rows located - TURNO: index {turno_row_idx}, Dia: index {dia_row_idx}")
            except Exception as e:
                self.logger.error(f"Error validating matriz2_og structure: {e}", exc_info=True)
                return False

            # Semanas restantes calculo
            date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            semana_inicial = date_obj.isocalendar().week
            semanas_restantes = 52 - semana_inicial
            
            # Get year from matrizB_og
            ano = pd.to_datetime(matrizB_og['data'].min()).year
            
            # Adjust minTurno for specific dates
            special_dates = [f'{ano}-12-23', f'{ano}-12-24', f'{ano}-12-30', f'{ano}-12-31']
            friday_dates = [f'{ano}-12-22', f'{ano}-12-29']
            
            matrizB_ini = matrizB_og.copy()
            matrizB_ini.loc[matrizB_ini['data'].isin(special_dates), 'min_turno'] = matrizB_ini['max_turno']
            mask_friday = (matrizB_ini['data'].isin(friday_dates)) & (matrizB_ini['turno'] == 'M')
            matrizB_ini.loc[mask_friday, 'min_turno'] = matrizB_ini.loc[mask_friday, 'max_turno']
            self.logger.info(f"DEBUG: matrizB_ini before matriz2: {matrizB_ini}")
            
            #CRIAR MATRIZ_2--------------------------------------------------
            
            # Reshape matriz2_og (equivalent to R melt)
            # Find TURNO and Dia rows
            turno_row_idx = matriz2_og[matriz2_og.iloc[:, 0] == 'TURNO'].index[0]
            dia_row_idx = matriz2_og[matriz2_og.iloc[:, 0] == 'Dia'].index[0]
            
            # Create column names
            dia_values = matriz2_og.iloc[dia_row_idx, 1:].values
            turno_values = matriz2_og.iloc[turno_row_idx, 1:].values
            new_columns = [f"{dia}_{turno}" for dia, turno in zip(dia_values, turno_values)]
            new_columns.insert(0, 'DIA_TURNO')
            
            # Rename columns
            #self.logger.info(f"DEBUG: new_columns func_inicilaiza 2095: {new_columns}")
            matriz2_og.columns = new_columns
            #self.logger.info(f"DEBUG: matriz2_og: {matriz2_og}")
            # Melt the dataframe
            matriz2_ini = pd.melt(matriz2_og, id_vars='DIA_TURNO', 
                                var_name='DATA', value_name='TIPO_TURNO')
            matriz2_ini.columns = ['COLABORADOR', 'DATA', 'TIPO_TURNO']

            #self.logger.info(f"DEBUG: matriz2_ini tail after melt: {matriz2_ini.tail(40)}")
            
            # Clean DATA column (remove everything after underscore)
            matriz2_ini['DATA'] = matriz2_ini['DATA'].str.replace(r'_.*$', '', regex=True)
            
            # Filter by date range
            matriz2_ini['DATA'] = matriz2_ini['DATA'].astype(str)
            matriz2_ini = matriz2_ini[
                (matriz2_ini['DATA'] >= start_date) & 
                (matriz2_ini['DATA'] <= end_date)
            ]
            #self.logger.info(f"end date: {end_date}")
            #self.logger.info(f"DEBUG: matriz2_ini tail after filter by date range: {matriz2_ini.tail(40)}")
            
            # Filter out unwanted rows
            unwanted_colaboradors = ['Dia', 'maxTurno', 'mediaTurno', 'minTurno', 'sdTurno', 'TURNO']
            matriz2_ini = pd.DataFrame(matriz2_ini)
            #self.logger.info(f"DEBUG: matriz2_ini last: {matriz2_ini.head(30)}")
            matriz2 = pd.DataFrame(matriz2_ini[~matriz2_ini['COLABORADOR'].isin(unwanted_colaboradors)].copy())
            
            # Previously, we had a filter by date range, but it was not working as expected. it since moved to before melt function

            # Add HORARIO column
            #self.logger.info(f"DEBUG: matriz2 before adding HORARIO column: {matriz2.head(30)}")
            matriz2 = pd.DataFrame(matriz2)
            matriz2['HORARIO'] = np.where(
                matriz2['TIPO_TURNO'].isin(['M', 'T', 'MoT', 'P']), 'H', matriz2['TIPO_TURNO']
            )
            
            # Get unique shift types
            tipos_de_turno = matriz2['TIPO_TURNO'].unique().tolist()
            
            # Process MoT and P shifts if they exist
            if 'MoT' in tipos_de_turno:
                matriz2 = func_turnos(matriz2, 'MoT')
            if 'P' in tipos_de_turno:
                matriz2 = func_turnos(matriz2, 'P')
            
            # Add date-related columns
            matriz2['DATA'] = pd.to_datetime(matriz2['DATA'])
            matriz2['WDAY'] = matriz2['DATA'].dt.dayofweek + 1  # Convert to 1-7 scale
            #self.logger.info(f"DEBUG: matriz2 max data: {matriz2['DATA'].max()}")
            matriz2['ID'] = range(len(matriz2))
            matriz2['WW'] = matriz2['DATA'].apply(adjusted_isoweek)
            matriz2['WD'] = matriz2['DATA'].dt.day_name().str[:3]
            
            #self.logger.info(f"DEBUG: matriz2 before assign_dia_tipo: {matriz2}")
            # Calculate DIA_TIPO
            # TODO: pass this function to helpers
            def calculate_dia_tipo(group):
                has_feriado = (group['TIPO_TURNO'] == 'F').any()
                is_sunday = group['WD'].iloc[0] == 'Sun'
                not_feriado_horario = group['HORARIO'] != 'F'
                
                if (has_feriado or is_sunday) and not_feriado_horario.any():
                    # PROBLEM here
                    return 'domYf'
                else:
                    return group['WD'].iloc[0]

            def assign_dia_tipo(group):
                has_F = (group['TIPO_TURNO'] == 'F').any()  # grupo tem pelo menos 1 F
                # aplica a lógica linha a linha
                group['DIA_TIPO'] = group.apply(
                    lambda row: 'domYf' if ((has_F or row['WD'] == 'Sun') and row['HORARIO'] != 'F') else row['WD'],
                    axis=1
                )
                return group
            
            # self.logger.info(f"DEBUG: matriz2 pre calculate_dia_tipo:\n {matriz2.head(30)}")
            # matriz2_temp = matriz2
            # matriz2['DIA_TIPO'] = matriz2.groupby('DATA').apply(calculate_dia_tipo).reset_index(drop=True)
            # self.logger.info(f"DEBUG: matriz2 post calculate_dia_tipo:\n {matriz2.head(30)}")

            matriz2 = matriz2.groupby('DATA', group_keys=False).apply(assign_dia_tipo)
            #self.logger.info(f"DEBUG: matriz2 post assign_dia_tipo:\n {matriz2.tail(30)}")
            
            # Merge with employee admission/dismissal dates
            emp_dates = matrizA_og[['emp', 'data_admissao', 'data_demissao']].copy()
            emp_dates = pd.concat([
                emp_dates,
                pd.DataFrame({'emp': ['TIPO_DIA'], 'data_admissao': [pd.NaT], 'data_demissao': [pd.NaT]})
            ])
            
            matriz2 = matriz2.merge(emp_dates, left_on='COLABORADOR', right_on='emp', how='left')
            #self.logger.info(f"DEBUG: matriz2 after merge with employee admission/dismissal dates:\n {matriz2.tail(30)}")
            
            # Filter by dismissal date and adjust HORARIO based on admission date
            matriz2 = matriz2[
                matriz2['DATA'] <= matriz2['data_demissao'].fillna(pd.Timestamp('2100-01-01'))
            ]
            self.logger.info(f"DEBUG: matriz2 after filter by dismissal date:\n {matriz2.tail(30)}")
            
            # TODO: pass this function to helpers
            def adjust_horario(row):
                admission_date = row['data_admissao'] if pd.notna(row['data_admissao']) else pd.Timestamp('1900-01-01')
                if row['DATA'] >= admission_date:
                    return row['HORARIO']
                elif row['DATA'] < admission_date and row['DIA_TIPO'] == 'domYf':
                    return 'L_'
                else:
                    return 'NL'
            
            matriz2['HORARIO'] = matriz2.apply(adjust_horario, axis=1)
            self.logger.info(f"DEBUG: matriz2 after adjust_horario:\n {matriz2.tail(30)}")
            
            #CRIAR MATRIZ_A--------------------------------------------------
            
            # Extract festivos (holidays)
            dom_e_fes = pd.Series(matriz2[
                (matriz2['HORARIO'] == 'F') & 
                (matriz2['COLABORADOR'] == 'TIPO_DIA')
            ]['DATA']).unique()
            
            # Filter matrizA_og
            matrizA_og = matrizA_og[matrizA_og['matricula'] != ''].copy()
            
            # Initialize counting dataframes
            df_merge_count_dom_fes = []
            df_merge_count_fes = []
            df_merge_count_holidays = []
            
            # Get convenio from external data or config
            convenio_bd = self.external_call_data.get('convenio_bd', 'BD')  # You may need to adjust this
            
            # Process each employee
            for matricula in matrizA_og['matricula'].unique():
                if matricula == '':
                    continue
                    
                matriz_temp = pd.DataFrame(matriz2[matriz2['COLABORADOR'] == matricula].copy())
                #matriz_temp['DATA'] = matriz_temp['DATA'].astype(str)
                tipo_contrato = matrizA_og[matrizA_og['matricula'] == matricula]['tipo_contrato'].iloc[0]
                matriz_temp = pd.DataFrame(matriz_temp)
                
                if convenio_bd == 'BD':  # Assuming BD convention
                    if tipo_contrato in [4, 5]:
                        # Count occurrences for 4/5 day contracts
                        count_occurrences = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            (matriz_temp['WDAY'] == 6)
                        ])
                        
                        # Subtract closed days that fell on holidays
                        fer_tipo3 = fer[fer['tipo'] == 3]['data'].tolist() if 'tipo' in fer.columns else []
                        count_occurrences_extra2 = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            matriz_temp['DATA'].isin(fer_tipo3) & 
                            (matriz_temp['WDAY'] == 6)
                        ])
                        count_occurrences -= count_occurrences_extra2
                        
                        # Count holidays
                        count_occurrences_fes = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            matriz_temp['DATA'].isin(pd.Series(dom_e_fes))
                        ])
                        count_occurrences_fes -= len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            matriz_temp['DATA'].isin(fer_tipo3) & 
                            (matriz_temp['WDAY'] != 6)
                        ])
                        
                        count_holidays = len(matriz_temp[matriz_temp['HORARIO'] == 'V'])
                        
                    else:
                        # For other contract types
                        count_occurrences = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            ((matriz_temp['WDAY'] == 6) | matriz_temp['DATA'].isin(pd.Series(dom_e_fes)))
                        ])
                        
                        fer_tipo3 = fer[fer['tipo'] == 3]['data'].tolist() if 'tipo' in fer.columns else []
                        count_occurrences_extra2 = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            matriz_temp['DATA'].isin(fer_tipo3)
                        ])
                        count_occurrences -= count_occurrences_extra2
                        
                        count_occurrences_fes = 0
                        count_holidays = 0
                
                else:
                    # SABECO convention
                    count_occurrences = len(matriz_temp[
                        (matriz_temp['HORARIO'] != 'NL') & 
                        matriz_temp['HORARIO'].str.contains('^L|DFS', case=False, na=False) & 
                        ((matriz_temp['WDAY'] == 6) | matriz_temp['DATA'].isin(pd.Series(dom_e_fes)))
                    ])
                    count_occurrences_fes = 0
                    count_holidays = 0
                
                # Store results
                df_merge_count_dom_fes.append({
                    'matricula': matricula, 
                    'total_dom_fes': count_occurrences
                })
                df_merge_count_fes.append({
                    'matricula': matricula, 
                    'total_fes': count_occurrences_fes
                })
                df_merge_count_holidays.append({
                    'matricula': matricula, 
                    'total_holidays': count_holidays
                })
            
            # Convert to DataFrames and merge
            df_merge_count_dom_fes = pd.DataFrame(df_merge_count_dom_fes).drop_duplicates()
            df_merge_count_fes = pd.DataFrame(df_merge_count_fes).drop_duplicates()
            df_merge_count_holidays = pd.DataFrame(df_merge_count_holidays).drop_duplicates()
            
            matrizA_og = matrizA_og.merge(df_merge_count_dom_fes, on='matricula', how='left')
            matrizA_og = matrizA_og.merge(df_merge_count_fes, on='matricula', how='left')
            matrizA_og = matrizA_og.merge(df_merge_count_holidays, on='matricula', how='left')
            
            # Divide by 2 (as in R code)
            matrizA_og['total_dom_fes'] = matrizA_og['total_dom_fes'] / 2
            matrizA_og['total_fes'] = matrizA_og['total_fes'] / 2
            matrizA_og['total_holidays'] = matrizA_og['total_holidays'] / 2
            
            # Adjust for contract type 3
            matrizA_og.loc[matrizA_og['tipo_contrato'] == 3, 'total_dom_fes'] = 0
            matrizA_og.loc[matrizA_og['tipo_contrato'] == 3, 'dyf_max_t'] = 0
            
            # Add DESCANSOS_ATRB column
            matrizA_og['descansos_atrb'] = 0
            
            # CALCULA LIBRANÇAS ------------------------------------------------------------
            
            # Create df_CD for consecutive day calculations
            matriz2 = pd.DataFrame(matriz2)
            matrizA_og = pd.DataFrame(matrizA_og)
            df_cd = pd.DataFrame(matriz2[pd.Series(matriz2['COLABORADOR']).isin(matrizA_og['matricula'])].copy())
            df_cd['WD'] = pd.Series(df_cd['DATA']).dt.dayofweek + 1  # Convert to 1-7 scale
            df_cd = df_cd.sort_values(['COLABORADOR', 'DATA'], ascending=[True, True])
            
            # Remove duplicates by keeping first occurrence per COLABORADOR/DATA
            df_cd = df_cd.groupby(['COLABORADOR', 'DATA']).first().reset_index()
            
            # Add next/previous day shift information
            df_cd = df_cd.sort_values(['COLABORADOR', 'DATA'])
            df_cd['TIPO_TURNO_NEXT'] = df_cd.groupby('COLABORADOR')['HORARIO'].shift(-1).fillna('H')
            df_cd['TIPO_TURNO_PREV'] = df_cd.groupby('COLABORADOR')['HORARIO'].shift(1).fillna('H')
            df_cd['WDAY_NEXT'] = df_cd.groupby('COLABORADOR')['WDAY'].shift(-1)
            df_cd['TIPO_TURNO_NEXT2'] = df_cd.groupby('COLABORADOR')['HORARIO'].shift(-2).fillna('H')
            df_cd['WDAY_NEXT2'] = df_cd.groupby('COLABORADOR')['WDAY'].shift(-2)
            df_cd['WDAY_PREV'] = df_cd.groupby('COLABORADOR')['WDAY'].shift(1)
            
            # Process 4/5 day contracts - count different types of rest days
            colabs_45d = matrizA_og[matrizA_og['tipo_contrato'].isin([4, 5])]['matricula'].tolist()
            
            count_ldt_45d_data = []
            for colab in colabs_45d:
                colab_data = df_cd[df_cd['COLABORADOR'] == colab].copy()
                
                # Calculate LD_at, LQ_at, LRES_at, CXX_at
                ld_at = 0
                
                # LQ_at calculation
                lq_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'] != 'LD') &
                    (pd.Series(colab_data['HORARIO']).str.contains('^L', case=False, na=False)) &
                    (pd.Series(colab_data['WD']).isin([1, 2, 3, 4, 5, 6])) &
                    (~pd.Series(colab_data['TIPO_TURNO_NEXT']).str.contains('^L', case=False, na=False)) &
                    (colab_data['TIPO_TURNO_PREV'] != 'L')
                )
                lq_at = lq_condition.sum()
                
                # LRES_at calculation  
                lres_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (pd.Series(colab_data['HORARIO']).str.contains('^L', case=False, na=False)) &
                    (pd.Series(colab_data['WD']).isin([1, 2, 3, 4, 5])) &
                    (~pd.Series(colab_data['TIPO_TURNO_PREV']).str.contains('^L', case=False, na=False)) &
                    (~pd.Series(colab_data['TIPO_TURNO_NEXT']).str.contains('^L', case=False, na=False))
                )
                lres_at = lres_condition.sum()
                
                # CXX_at calculation
                cxx_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (pd.Series(colab_data['HORARIO']).str.contains('^L', case=False, na=False)) &
                    (pd.Series(colab_data['WD']).isin([1, 2, 3, 4, 5])) &
                    (pd.Series(colab_data['TIPO_TURNO_PREV']).str.contains('^L', case=False, na=False))
                )
                cxx_at = cxx_condition.sum()
                
                count_ldt_45d_data.append({
                    'COLABORADOR': colab,
                    'LD_at': ld_at,
                    'LQ_at': lq_at, 
                    'LRES_at': lres_at,
                    'CXX_at': cxx_at
                })
            
            # Process 6 day contracts
            colabs_6d = matrizA_og[matrizA_og['tipo_contrato'] == 6]['matricula'].tolist()
            
            count_ldt_6d_data = []
            for colab in colabs_6d:
                colab_data = df_cd[df_cd['COLABORADOR'] == colab].copy()
                
                # LD_at calculation
                ld_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'] != 'LQ') &
                    (pd.Series(colab_data['HORARIO']).str.contains('^L', case=False, na=False)) &
                    (pd.Series(colab_data['WD']).isin([1, 2, 3, 4, 5, 6])) &
                    (~pd.Series(colab_data['TIPO_TURNO_NEXT']).str.contains('^L', case=False, na=False)) &
                    (colab_data['TIPO_TURNO_PREV'] != 'L')
                )
                ld_at = ld_condition.sum()
                
                # LQ_at calculation  
                lq_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'] != 'LD') &
                    (pd.Series(colab_data['HORARIO']).str.contains('^L', case=False, na=False)) &
                    (pd.Series(colab_data['WD']).isin([1, 2, 3, 4, 5, 6])) &
                    (~pd.Series(colab_data['TIPO_TURNO_NEXT']).str.contains('^L', case=False, na=False)) &
                    (colab_data['TIPO_TURNO_PREV'] != 'L')
                )
                lq_at = lq_condition.sum()
                
                # CXX_at calculation
                cxx_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (pd.Series(colab_data['HORARIO']).str.contains('^L', case=False, na=False)) &
                    (pd.Series(colab_data['WD']).isin([1, 2, 3, 4, 5])) &
                    (pd.Series(colab_data['TIPO_TURNO_PREV']).str.contains('^L', case=False, na=False))
                )
                cxx_at = cxx_condition.sum()
                
                count_ldt_6d_data.append({
                    'COLABORADOR': colab,
                    'LD_at': ld_at,
                    'LQ_at': lq_at,
                    'LRES_at': 0,
                    'CXX_at': cxx_at
                })
            
            # Combine all collaborators
            count_ldt = pd.DataFrame(count_ldt_45d_data + count_ldt_6d_data)
            
            # Calculate consecutive day patterns (C2D and C3D)
            
            # Add pattern identification columns
            df_cd['FRI_SAT_SUN'] = (
                (df_cd['WDAY_PREV'] == 5) & 
                (df_cd['TIPO_TURNO_PREV'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY'] == 6) & 
                (df_cd['HORARIO'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT'] == 7) & 
                (df_cd['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False))
            )
            
            df_cd['SAT_SUN_MON'] = (
                (df_cd['WDAY'] == 6) & 
                (df_cd['HORARIO'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT'] == 7) & 
                (df_cd['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT2'] == 1) & 
                (df_cd['TIPO_TURNO_NEXT2'].str.contains('^L', case=False, na=False))
            )
            
            df_cd['SAT_SUN_ONLY'] = (
                (df_cd['WDAY'] == 6) & 
                (df_cd['HORARIO'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT'] == 7) & 
                (df_cd['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT2'] == 1) & 
                (~df_cd['TIPO_TURNO_NEXT2'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_PREV'] == 5) & 
                (~df_cd['TIPO_TURNO_PREV'].str.contains('^L', case=False, na=False))
            )
            
            df_cd['SUN_MON_ONLY'] = (
                (df_cd['WDAY'] == 6) & 
                (~df_cd['HORARIO'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT'] == 7) & 
                (df_cd['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT2'] == 1) & 
                (df_cd['TIPO_TURNO_NEXT2'].str.contains('^L', case=False, na=False))
            )
            
            # Count C3D patterns
            c3d_fri_sat_sun = df_cd[df_cd['FRI_SAT_SUN']].groupby('COLABORADOR')['DATA'].nunique().reset_index()
            c3d_fri_sat_sun.columns = ['COLABORADOR', 'C3D_at_FSS']
            
            c3d_sat_sun_mon = df_cd[df_cd['SAT_SUN_MON']].groupby('COLABORADOR')['DATA'].nunique().reset_index()
            c3d_sat_sun_mon.columns = ['COLABORADOR', 'C3D_at_SSM']
            
            # Merge C3D counts
            c3d_at = pd.merge(c3d_sat_sun_mon, c3d_fri_sat_sun, on='COLABORADOR', how='outer').fillna(0)
            c3d_at['C3D_at'] = c3d_at['C3D_at_SSM'] + c3d_at['C3D_at_FSS']
            c3d_at = c3d_at[['COLABORADOR', 'C3D_at']]
            
            # Count C2D patterns  
            c2d_sat_sun_only = df_cd[df_cd['SAT_SUN_ONLY']].groupby('COLABORADOR')['DATA'].nunique().reset_index()
            c2d_sat_sun_only.columns = ['COLABORADOR', 'C2D_at_SSO']
            
            c2d_sun_mon_only = df_cd[df_cd['SUN_MON_ONLY']].groupby('COLABORADOR')['DATA'].nunique().reset_index()
            c2d_sun_mon_only.columns = ['COLABORADOR', 'C2D_at_SMO']
            
            # Merge C2D counts
            c2d_at = pd.merge(c2d_sat_sun_only, c2d_sun_mon_only, on='COLABORADOR', how='outer').fillna(0)
            c2d_at['C2D_at'] = c2d_at['C2D_at_SSO'] + c2d_at['C2D_at_SMO']
            c2d_at = c2d_at[['COLABORADOR', 'C2D_at']]
            
            # Merge C2D and C3D
            c_at = pd.merge(c2d_at, c3d_at, on='COLABORADOR', how='outer').fillna(0)
            
            # Merge with count_ldt
            count_ldt = pd.merge(count_ldt, c_at, on='COLABORADOR', how='left').fillna(0)
            
            # Merge with matrizA_og
            matrizA_og = matrizA_og.merge(count_ldt, left_on='matricula', right_on='COLABORADOR', how='left').fillna(0)
            
            # Store original matrizA_og
            matrizA_og_ed = matrizA_og.copy() # TODO: check if this is needed
            
            # Adjust C2D logic (C2D = C2D + C3D)
            matrizA_og['C2D_at'] = matrizA_og['C2D_at'] + matrizA_og['C3D_at']

            # Log the column names of matrizA_og
            self.logger.info(f"Columns in matrizA_og after processing: {matrizA_og.columns.tolist()}")
            
            # Calculate adjusted values
            matrizA_og = self._log_and_clip_l_total(matrizA_og, "before LD_at subtraction", clip_negative=False)
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['LD_at']
            matrizA_og = self._log_and_clip_l_total(matrizA_og, "after LD_at subtraction", clip_negative=False)
            
            # Log all columns and their data
            #self.logger.info(f"DEBUG: matrizA_og shape: {matrizA_og.shape}")
            #self.logger.info(f"DEBUG: matrizA_og columns: {matrizA_og.columns.tolist()}")
            #self.logger.info(f"DEBUG: matrizA_og dtypes:\n{matrizA_og.dtypes}")
            
            # Log data with better formatting - using CSV format for readability
            #self.logger.info("DEBUG: matrizA_og data (CSV format):")
            #self.logger.info(matrizA_og.to_csv(sep='\t', index=False))
            
            # Alternative: Log each column separately for very detailed view
            #self.logger.info("DEBUG: matrizA_og column-by-column data:")
            #for col in matrizA_og.columns:
            #    self.logger.info(f"  {col}: {matrizA_og[col].tolist()}")
            
            # Log specific column for backward compatibility
            self.logger.info(f"DEBUG: matrizA_og ld: {matrizA_og['ld']}")
            matrizA_og['ld'] = np.maximum(matrizA_og['ld'] - matrizA_og['LD_at'], 0)
            matrizA_og['l_dom'] = matrizA_og['l_dom'] - matrizA_og['total_dom_fes'] - matrizA_og['total_fes']
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['total_dom_fes']
            matrizA_og = self._log_and_clip_l_total(matrizA_og, "after total_dom_fes subtraction", clip_negative=False)
            
            # Handle holidays based on contract type
            holiday_adjustment = np.where(
                matrizA_og['tipo_contrato'] == 4,
                2 * matrizA_og['total_holidays'].apply(custom_round) / 7,
                matrizA_og['total_holidays'].apply(custom_round) / 7
            )
            matrizA_og['l_total'] = matrizA_og['l_total'] - holiday_adjustment
            matrizA_og = self._log_and_clip_l_total(matrizA_og, "after holiday adjustment", clip_negative=False)
            
            # Handle C2D and C3D
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['C2D_at']
            matrizA_og = self._log_and_clip_l_total(matrizA_og, "after C2D_at subtraction", clip_negative=False)
            matrizA_og['c2d'] = np.maximum(matrizA_og['c2d'] - matrizA_og['C2D_at'], 0)
            
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['C3D_at']
            matrizA_og = self._log_and_clip_l_total(matrizA_og, "after C3D_at subtraction", clip_negative=False)
            matrizA_og['c3d'] = np.maximum(matrizA_og['c3d'] - matrizA_og['C3D_at'], 0)
            
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['LQ_at']
            matrizA_og = self._log_and_clip_l_total(matrizA_og, "after LQ_at subtraction", clip_negative=False)
            matrizA_og['lq'] = np.maximum(matrizA_og['lq'] - matrizA_og['LQ_at'], 0)
            
            # Handle CXX
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['CXX_at']
            matrizA_og = self._log_and_clip_l_total(matrizA_og, "after CXX_at subtraction", clip_negative=False)
            matrizA_og['cxx'] = np.maximum(matrizA_og['cxx'] - matrizA_og['CXX_at'], 0)

            # Handle VZ
            if 'vz' not in matrizA_og.columns:
                matrizA_og['vz'] = 0
            self.logger.info(f"DEBUG: matrizA_og columns: {matrizA_og.columns}")
            
            # Create matrizA with selected columns
            matrizA = matrizA_og[[
                'unidade', 'secao', 'posto', 'fk_colaborador', 'matricula', 'out',
                'tipo_contrato', 'ciclo', 'l_total', 'l_dom', 'ld', 'lq', 'q', 
                'c2d', 'c3d', 'cxx', 'descansos_atrb', 'dyf_max_t', 'LRES_at', 'lq_og', 'dofhc', 'vz', 'data_admissao'
            ]].copy()
            
            # Rename columns to match R output
            matrizA = pd.DataFrame(matrizA).rename(columns={
                'ld': 'l_d',
                'lq': 'l_q', 
                'q': 'l_qs'
            })
            
            # Create backup matrices after renaming
            matrizA_bk_og = matrizA.copy()
            matrizA_bk_og['l_res'] = (matrizA_bk_og['l_total'] - matrizA_bk_og['l_dom'] - 
                                    matrizA_bk_og['l_d'] - matrizA_bk_og['l_q'] - 
                                    matrizA_bk_og['l_qs'] - matrizA_bk_og['c2d'] - 
                                    matrizA_bk_og['c3d'] - matrizA_bk_og['cxx'] - 
                                    matrizA_bk_og['vz'] - matrizA_bk_og['LRES_at'])
            matrizA_bk_og['l_total'] = matrizA_bk_og['l_total'] - matrizA_bk_og['LRES_at']
            matrizA_bk_og = matrizA_bk_og.drop('LRES_at', axis=1)
            matrizA_bk_og.loc[matrizA_bk_og['l_res'] < 0, 'vz'] = matrizA_bk_og['vz'] + matrizA_bk_og['l_res']
            
            # Preserve dofhc column in matrizA_bk_og
            if 'dofhc' in matrizA.columns:
                matrizA_bk_og['dofhc'] = matrizA['dofhc']
            
            matrizA_bk = matrizA.copy()           
            matrizA_bk['l_res'] = (matrizA_bk['l_total'] - matrizA_bk['l_dom'] - 
                                matrizA_bk['l_d'] - matrizA_bk['l_q'] - 
                                matrizA_bk['l_qs'] - matrizA_bk['c2d'] - 
                                matrizA_bk['c3d'] - matrizA_bk['cxx'] - 
                                matrizA_bk['vz'] - matrizA_bk['LRES_at'])
            matrizA_bk['l_total'] = matrizA_bk['l_total'] - matrizA_bk['LRES_at']
            matrizA_bk = matrizA_bk.drop('LRES_at', axis=1)
            matrizA_bk.loc[matrizA_bk['l_res'] < 0, 'vz'] = matrizA_bk['vz'] + matrizA_bk['l_res']
            
            # Preserve dofhc column in matrizA_bk
            if 'dofhc' in matrizA.columns:
                matrizA_bk['dofhc'] = matrizA['dofhc']
            
            # CONTRATOS 2/3DIAS Processing -----------------------------------------------
            
            # Create matriz2_3D for 2/3 day contracts
            contract_23_employees = pd.Series(matrizA[pd.Series(matrizA['tipo_contrato']).isin([2, 3])]['matricula']).tolist()
            
            if len(contract_23_employees) > 0:
                matriz2_3d = matriz2[matriz2['COLABORADOR'].isin(contract_23_employees)].copy()
                
                # Merge with contract type information
                contract_info = pd.DataFrame(matrizA[pd.Series(matrizA['tipo_contrato']).isin([2, 3])][['matricula', 'tipo_contrato']])
                matriz2_3d = matriz2_3d.merge(contract_info, left_on='COLABORADOR', right_on='matricula', how='left')
                
                # Filter out unwanted HORARIO types and group by week/employee
                matriz2_3d = matriz2_3d[~matriz2_3d['HORARIO'].isin(['-', 'V', 'F'])].copy()
                
                # Count work days per week per employee (divide by 2 for morning/afternoon)
                week_counts = (pd.DataFrame(matriz2_3d)
                             .groupby(['COLABORADOR', 'WW'])
                             .agg(count=('COLABORADOR', 'size'))
                             .reset_index())
                week_counts['count'] = week_counts['count'] / 2
                
                # Merge back with matriz2_3d
                matriz2_3d = matriz2_3d.merge(week_counts, on=['COLABORADOR', 'WW'], how='left')
                
                # Update HORARIO based on count and contract type
                def update_horario_3d(row):
                    if row['count'] == 3 and row['tipo_contrato'] == 3:
                        return 'NL3D'
                    elif row['count'] == 2 and row['tipo_contrato'] == 2:
                        return 'NL2D'
                    else:
                        return row['HORARIO']
                
                matriz2_3d['HORARIO'] = matriz2_3d.apply(update_horario_3d, axis=1)
                
                # Remove unnecessary columns
                matriz2_3d = matriz2_3d.drop(['count', 'tipo_contrato_y'], axis=1, errors='ignore')
                
                # Merge back with main matriz2
                # First remove the employees that were processed
                matriz2 = matriz2[~matriz2['COLABORADOR'].isin(contract_23_employees)].copy()
                # Then add back the processed data
                matriz2 = pd.concat([matriz2, matriz2_3d], ignore_index=True)
            
            # Convert DATA back to string
            matriz2['DATA'] = matriz2['DATA'].astype(str)
            
            # Calculate work weeks for 2/3 day contract employees
            if len(contract_23_employees) > 0:
                matriz3d = pd.DataFrame(matriz2[pd.Series(matriz2['COLABORADOR']).isin(contract_23_employees)].copy())
                
                # Filter work days and count weeks
                matriz3d = matriz3d[~matriz3d['HORARIO'].isin(['-', 'V', 'F', 'NL3D', 'NL2D'])].copy()
                
                work_weeks = (matriz3d.groupby('COLABORADOR')
                            .agg(count=('WW', 'nunique'))
                            .reset_index())
                work_weeks.columns = ['matricula', 'count']
                
                # Merge with matrizA
                matrizA = matrizA.merge(work_weeks, on='matricula', how='left')
                
                # Update L_TOTAL for 2/3 day contracts
                matrizA = self._log_and_clip_l_total(matrizA, "before 2/3 day contract l_total update", clip_negative=False)
                matrizA.loc[matrizA['tipo_contrato'].isin([2, 3]), 'l_total'] = (
                    matrizA.loc[matrizA['tipo_contrato'].isin([2, 3]), 'count']
                )
                matrizA = self._log_and_clip_l_total(matrizA, "after 2/3 day contract l_total update", clip_negative=False)
            else:
                matrizA['count'] = 0
            
            matrizA = matrizA.fillna(0)
            matrizA = matrizA.drop('count', axis=1, errors='ignore')
            # Log and apply clip to all numeric columns explicitly
            matrizA = self._log_and_clip_l_total(matrizA, "before final clipping operation", clip_negative=False)
            numeric_cols = matrizA.select_dtypes(include=['number']).columns
            matrizA[numeric_cols] = pd.DataFrame(matrizA[numeric_cols]).clip(lower=0)
            matrizA = self._log_and_clip_l_total(matrizA, "after final clipping operation", clip_negative=False)
            
            # Add L_RES column
            matrizA['l_res'] = 0
            
            # Calculate auxiliary columns
            matrizA['aux'] = matrizA['l_dom'] + matrizA['l_d'] + matrizA['l_q'] + matrizA['c2d'] + matrizA['c3d'] + matrizA['cxx']
            matrizA['aux2'] = matrizA['l_total'] - matrizA['aux']
            
            # Adjust L_D if aux2 is negative
            aux2_negative_count = (matrizA['aux2'] < 0).sum()
            if aux2_negative_count > 0:
                self.logger.warning(f"Found {aux2_negative_count} employees with negative aux2 (l_total - sum of components). Adjusting l_d values.")
            matrizA.loc[matrizA['aux2'] < 0, 'l_d'] = matrizA['l_d'] + matrizA['aux2']
            matrizA = matrizA.drop(['aux', 'aux2'], axis=1)
            matrizA = self._log_and_clip_l_total(matrizA, "after aux2 adjustment", clip_negative=False)
            
            # Set L_RES for contract type 3
            matrizA.loc[matrizA['tipo_contrato'] == 3, 'l_res'] = matrizA.loc[matrizA['tipo_contrato'] == 3, 'l_total']
            
            # Add VZ (empty days) for 4-day contracts
            matrizA['vz'] = np.where(matrizA['tipo_contrato'] == 4, semanas_restantes - 4, 0)
            
            # Update HORARIO: change 'L' to 'L_'
            matriz2.loc[matriz2['HORARIO'] == 'L', 'HORARIO'] = 'L_'
            
            # Create backup matrices
            matrizA_bk_og = matrizA.copy()
            matrizA_bk_og['l_res'] = (matrizA_bk_og['l_total'] - matrizA_bk_og['l_dom'] - 
                                    matrizA_bk_og['l_d'] - matrizA_bk_og['l_q'] - 
                                    matrizA_bk_og['l_qs'] - matrizA_bk_og['c2d'] - 
                                    matrizA_bk_og['c3d'] - matrizA_bk_og['cxx'] - 
                                    matrizA_bk_og['vz'] - matrizA_bk_og['LRES_at'])
            matrizA_bk_og['l_total'] = matrizA_bk_og['l_total'] - matrizA_bk_og['LRES_at']
            matrizA_bk_og = matrizA_bk_og.drop('LRES_at', axis=1)
            matrizA_bk_og.loc[matrizA_bk_og['l_res'] < 0, 'vz'] = matrizA_bk_og['vz'] + matrizA_bk_og['l_res']
            
            matrizA_bk = matrizA.copy()
            matrizA_bk['l_res'] = (matrizA_bk['l_total'] - matrizA_bk['l_dom'] - 
                                matrizA_bk['l_d'] - matrizA_bk['l_q'] - 
                                matrizA_bk['l_qs'] - matrizA_bk['c2d'] - 
                                matrizA_bk['c3d'] - matrizA_bk['cxx'] - 
                                matrizA_bk['vz'] - matrizA_bk['LRES_at'])
            matrizA_bk['l_total'] = matrizA_bk['l_total'] - matrizA_bk['LRES_at']
            matrizA_bk = matrizA_bk.drop('LRES_at', axis=1)
            matrizA_bk.loc[matrizA_bk['l_res'] < 0, 'vz'] = matrizA_bk['vz'] + matrizA_bk['l_res']
            
            # Sort by L_TOTAL descending and clip negative values
            matrizA_bk = matrizA_bk.sort_values('l_total', ascending=False)
            # Before clipping, identify numeric columns
            numeric_cols = matrizA_bk.select_dtypes(include=['number']).columns
            # Apply clip only to numeric columns
            matrizA_bk[numeric_cols] = matrizA_bk[numeric_cols].clip(lower=0)
            
            # Create backup for matriz_data_turno (placeholder)
            matriz_data_turno_bk = pd.DataFrame({'COLUNA': [np.nan]})
            
            # Dev TIPO TURNO FIX ---------------------------------------------------------------------
            # Fix TIPO_TURNO for consistency
            matriz2_tipo_turno_fix = pd.DataFrame(matriz2[pd.Series(matriz2['HORARIO']).isin(['H', 'DFS'])])
            matriz2_tipo_turno_fix = pd.DataFrame(matriz2_tipo_turno_fix[['WW', 'TIPO_TURNO']])
            matriz2_tipo_turno_fix = matriz2_tipo_turno_fix.drop_duplicates(subset=['WW'])
            matriz2_tipo_turno_fix = matriz2_tipo_turno_fix.rename(columns={'TIPO_TURNO': 'TIPO_TURNO_FIX'})
            
            matriz2 = pd.DataFrame(matriz2).merge(matriz2_tipo_turno_fix, on='WW', how='left')
            matriz2.loc[pd.Series(matriz2['TIPO_TURNO']).isin(['NL']), 'TIPO_TURNO'] = matriz2['TIPO_TURNO_FIX']
            matriz2 = pd.DataFrame(matriz2).drop('TIPO_TURNO_FIX', axis=1, errors='ignore')
            
            matriz2_bk = matriz2.copy()
            
            # Individual Employee Processing Loop ----------------------------------------
            
            for colab in matrizA_bk['matricula']:
                mmA = matrizA_bk[matrizA_bk['matricula'] == colab].copy()
                if len(mmA) == 0:
                    continue
                    
                mmA = mmA.iloc[0:1].copy()  # Take first row if multiple
                tipo_contrato = int(mmA['tipo_contrato'].iloc[0])
                ciclo = str(mmA['ciclo'].iloc[0]).upper() if 'ciclo' in mmA.columns else ''
                dyf_max_t = int(mmA['dyf_max_t'].iloc[0]) if 'dyf_max_t' in mmA.columns else 0
                #self.logger.info(f"DEBUG: colab {colab} mmA: {mmA}")
                
                if tipo_contrato == 2:
                    # Process 2-day contracts using calcular_folgas2
                    new_c = matriz2_bk[matriz2_bk['COLABORADOR'] == colab].copy()
                    new_c = pd.DataFrame(new_c).sort_values(['COLABORADOR', 'DATA', 'TIPO_TURNO'])
                    
                    # Remove duplicates by keeping first occurrence per COLABORADOR/DATA
                    new_c = new_c.groupby(['COLABORADOR', 'DATA']).first().reset_index()
                    
                    # Group by week and calculate folgas
                    week_results = []
                    for ww in new_c['WW'].unique():
                        week_data = new_c[new_c['WW'] == ww]
                        folgas_result = calcular_folgas2(week_data)
                        folgas_result['COLABORADOR'] = colab
                        week_results.append(folgas_result)
                    
                    # Combine results
                    if week_results:
                        combined_results = pd.concat(week_results, ignore_index=True)
                        total_l_res = combined_results['L_RES'].sum()
                        total_l_dom = combined_results['L_DOM'].sum()
                        total_l_total = total_l_res + total_l_dom
                        
                        mmA.loc[:, 'l_res'] = total_l_res
                        mmA.loc[:, 'l_dom'] = total_l_dom
                        mmA.loc[:, 'l_total'] = total_l_total
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                    
                elif tipo_contrato == 3:
                    # Process 3-day contracts using calcular_folgas3
                    new_c = matriz2_bk[matriz2_bk['COLABORADOR'] == colab].copy()
                    new_c = pd.DataFrame(new_c).sort_values(['COLABORADOR', 'DATA', 'TIPO_TURNO'])
                    
                    # Remove duplicates
                    new_c = new_c.groupby(['COLABORADOR', 'DATA']).first().reset_index()
                    
                    # Group by week and calculate folgas
                    week_results = []
                    for ww in new_c['WW'].unique():
                        week_data = new_c[new_c['WW'] == ww]
                        folgas_result = calcular_folgas3(week_data)
                        folgas_result['COLABORADOR'] = colab
                        week_results.append(folgas_result)
                    
                    # Combine results
                    if week_results:
                        combined_results = pd.concat(week_results, ignore_index=True)
                        total_l_res = combined_results['L_RES'].sum()
                        total_l_dom = combined_results['L_DOM'].sum()
                        total_l_total = total_l_res + total_l_dom
                        
                        mmA.loc[:, 'l_res'] = total_l_res
                        mmA.loc[:, 'l_dom'] = total_l_dom
                        mmA.loc[:, 'l_total'] = total_l_total
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                    
                elif dyf_max_t == 0 and ciclo not in ['COMPLETO']:
                    #self.logger.info(f"DEBUG: primeiro if")
                    #self.logger.info(f"DEBUG: colab {colab} ciclo {ciclo} dyf_max_t {dyf_max_t}")
                    # Employee doesn't work any Sunday - force all Sundays with L
                    mmA.loc[:, 'l_total'] = mmA['l_total'].iloc[0] - mmA['l_dom'].iloc[0]
                    mmA.loc[:, 'l_dom'] = 0
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                    
                    # Update calendar matrix with domYfes with L
                    new_c = matriz2_bk[matriz2_bk['COLABORADOR'] == colab].copy()
                    new_c.loc[(new_c['DIA_TIPO'] == 'domYf') & (new_c['HORARIO'] != 'V'), 'HORARIO'] = 'L_DOM'
                    
                    matriz2_bk = matriz2_bk[matriz2_bk['COLABORADOR'] != colab]
                    matriz2_bk = pd.concat([pd.DataFrame(matriz2_bk), pd.DataFrame(new_c)], ignore_index=True)
                    
                elif ciclo in ['SIN DYF']:
                    #self.logger.info(f"DEBUG: segundo if")
                    #self.logger.info(f"DEBUG: colab {colab} ciclo {ciclo} dyf_max_t {dyf_max_t}")
                    # Special cycle: only assign Sundays and LD
                    mmA.loc[:, 'c2d'] = mmA['c2d'].iloc[0] - mmA['c3d'].iloc[0]
                    mmA.loc[:, 'l_total'] = (mmA['l_dom'].iloc[0] + mmA['l_d'].iloc[0] + 
                                            mmA['c2d'].iloc[0] + mmA['cxx'].iloc[0])
                    mmA.loc[:, 'l_q'] = 0
                    mmA.loc[:, 'l_qs'] = 0
                    mmA.loc[:, 'c3d'] = 0
                    mmA.loc[:, 'l_res'] = 0
                    mmA.loc[:, 'vz'] = 0
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                
                # Handle COMPLETO cycle - reset all L values
                if ciclo == 'COMPLETO':
                    #self.logger.info(f"DEBUG: terceiro if")
                    #self.logger.info(f"DEBUG: colab {colab} ciclo {ciclo} dyf_max_t {dyf_max_t}")
                    # Reset all columns from position 9 onwards to 0
                    #cols_to_reset = mmA.columns[8:]  # Assuming position 9 is index 8
                    cols_to_reset = ['l_total', 'l_dom', 'l_d', 'l_q', 'l_qs', 'c2d', 'c3d', 'cxx','descansos_atrb', 'dyf_max_t', 'lq_og', 'vz', 'l_res']
                    mmA.loc[:, cols_to_reset] = 0
                    self.logger.info(f"DEBUG: cols_to_reset: {cols_to_reset}")
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([pd.DataFrame(matrizA_bk), pd.DataFrame(mmA)], ignore_index=True)
                
                # Handle CXX for 4/5 day contracts
                if (tipo_contrato in [4, 5] and mmA['cxx'].iloc[0] > 0 and 
                    ciclo not in ['SIN DYF', 'COMPLETO']):
                    #self.logger.info(f"DEBUG: quarto if")
                    
                    mmA.loc[:, 'l_res2'] = mmA['l_res'].iloc[0] - mmA['cxx'].iloc[0]
                    mmA.loc[:, 'l_res'] = mmA['cxx'].iloc[0]
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([pd.DataFrame(matrizA_bk), pd.DataFrame(mmA)], ignore_index=True)
                else:
                    self.logger.info(f"DEBUG: ultimo else")
                    mmA.loc[:, 'l_res2'] = 0
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([pd.DataFrame(matrizA_bk), pd.DataFrame(mmA)], ignore_index=True)
            
            #self.logger.info(f"DEBUG: algoritmo: {self.auxiliary_data['GD_algorithmName']}")
            #self.logger.info(f"DEBUG: matrizA_bk shape: {matrizA_bk.shape}")
            #self.logger.info(f"DEBUG: matrizA_bk columns: {matrizA_bk.columns.tolist()}")
            
            # Check if target columns exist
            target_cols = ['l_res', 'l_d']
            existing_cols = [col for col in target_cols if col in matrizA_bk.columns]
            missing_cols = [col for col in target_cols if col not in matrizA_bk.columns]
            #self.logger.info(f"DEBUG: existing target columns: {existing_cols}")
            #self.logger.info(f"DEBUG: missing target columns: {missing_cols}")
            
            # Log before values
            for col in existing_cols:
                self.logger.info(f"DEBUG: {col} before reset: {matrizA_bk[col].tolist()}")
            
            if self.auxiliary_data['GD_algorithmName'] == 'salsa_algorithm':
                self.logger.info("DEBUG: Condition met - applying salsa_algorithm zeros")
                if 'l_res' in matrizA_bk.columns:
                    matrizA_bk.loc[:, 'l_res'] = 0
                    self.logger.info(f"DEBUG: Set l_res to 0, now values: {matrizA_bk['l_res'].tolist()}")
                else:
                    self.logger.warning("DEBUG: Column 'l_res' not found in matrizA_bk")
                    
                if 'l_d' in matrizA_bk.columns:
                    matrizA_bk.loc[:, 'l_d'] = 0
                    self.logger.info(f"DEBUG: Set l_d to 0, now values: {matrizA_bk['l_d'].tolist()}")
                else:
                    self.logger.warning("DEBUG: Column 'l_d' not found in matrizA_bk")
            else:
                self.logger.info(f"DEBUG: Condition NOT met - algorithm is {self.auxiliary_data['GD_algorithmName']}, not salsa_algorithm")
                
            # Log after values
            for col in existing_cols:
                if col in matrizA_bk.columns:
                    self.logger.info(f"DEBUG: {col} after reset: {matrizA_bk[col].tolist()}")

            # Final cleanup of matrizA_bk
            matrizA_bk = matrizA_bk.drop('dyf_max_t', axis=1, errors='ignore')
            matrizA_bk = matrizA_bk.reset_index(drop=True)
            
            # Initialize logs
            logs = None
            
            # Update matriz2_bk HORARIO: L_ -> L_DOM for domYf
            matriz2_bk.loc[(matriz2_bk['DIA_TIPO'] == 'domYf') & 
                        (matriz2_bk['HORARIO'] == 'L_'), 'HORARIO'] = 'L_DOM'
            
            #CRIAR MATRIZ_B--------------------------------------------------
            
            # Calculate +H (working hours) for morning shifts
            trab_manha_data = []
            for data in matriz2_bk['DATA'].unique():
                if data == 'TIPO_DIA':
                    continue
                    
                day_data = matriz2_bk[(matriz2_bk['DATA'] == data) & 
                                    (matriz2_bk['COLABORADOR'] != 'TIPO_DIA')].copy()
                
                # Group by COLABORADOR to analyze each employee's day
                employee_counts = []
                for colab in pd.Series(day_data['COLABORADOR']).unique():
                    colab_data = pd.DataFrame(day_data[day_data['COLABORADOR'] == colab])
                    
                    # Check if employee has M shift with H or NL
                    has_m = ((colab_data['TIPO_TURNO'] == 'M') & 
                            (pd.Series(colab_data['HORARIO']).str.contains('H|NL', case=False, na=False))).any()
                    
                    # Check if employee has T shift with H or NL  
                    has_t = ((colab_data['TIPO_TURNO'] == 'T') & 
                            (pd.Series(colab_data['HORARIO']).str.contains('H|NL', case=False, na=False))).any()
                    
                    # Calculate weight: 1 if only M, 0.5 if both M and T, 0 if no M
                    if has_m and has_t:
                        m_count = 0.5
                    elif has_m:
                        m_count = 1.0
                    else:
                        m_count = 0.0
                        
                    employee_counts.append(m_count)
                
                trab_manha_data.append({
                    'DATA': data,
                    'TURNO': 'M',
                    '+H': sum(employee_counts)
                })
            
            trab_manha = pd.DataFrame(trab_manha_data)
            
            # Calculate +H for afternoon shifts
            trab_tarde_data = []
            for data in matriz2_bk['DATA'].unique():
                if data == 'TIPO_DIA':
                    continue
                    
                day_data = matriz2_bk[(matriz2_bk['DATA'] == data) & 
                                    (matriz2_bk['COLABORADOR'] != 'TIPO_DIA')].copy()
                
                # Group by COLABORADOR to analyze each employee's day
                employee_counts = []
                for colab in pd.Series(day_data['COLABORADOR']).unique():
                    colab_data = day_data[day_data['COLABORADOR'] == colab]
                    
                    # Check if employee has T shift with H or NL (this is the main shift for afternoon)
                    has_m = ((colab_data['TIPO_TURNO'] == 'T') & 
                            (pd.Series(colab_data['HORARIO']).str.contains('H|NL', case=False, na=False))).any()
                    
                    # Check if employee has M shift with H or NL  
                    has_t = ((colab_data['TIPO_TURNO'] == 'M') & 
                            (pd.Series(colab_data['HORARIO']).str.contains('H|NL', case=False, na=False))).any()
                    
                    # Calculate weight: 1 if only T, 0.5 if both M and T, 0 if no T
                    if has_m and has_t:
                        m_count = 0.5
                    elif has_m:
                        m_count = 1.0
                    else:
                        m_count = 0.0
                        
                    employee_counts.append(m_count)
                
                trab_tarde_data.append({
                    'DATA': data,
                    'TURNO': 'T', 
                    '+H': sum(employee_counts)
                })
            
            trab_tarde = pd.DataFrame(trab_tarde_data)

            # Transform turno column to uppercase
            matrizB_ini['turno'] = matrizB_ini['turno'].str.upper()
            
            # Merge +H with matrizB for morning
            self.logger.info(f"matrizB_ini before filter:\n {matrizB_ini}")
            matrizB_m = matrizB_ini[matrizB_ini['turno'] == 'M'].copy()
            self.logger.info(f"matrizB_m after filter turno m:\n {matrizB_m}")
            
            # Convert dates to date objects
            # TODO: check if it does not need %Y-%m-%d
            matrizB_m['data'] = pd.to_datetime(matrizB_m['data']).dt.date
            trab_manha['DATA'] = pd.to_datetime(trab_manha['DATA']).dt.date
            
            #TODO: from this point, the dataframe is empty
            self.logger.info(f"trab_manha: {trab_manha}")
            matrizB_m = matrizB_m.merge(trab_manha, left_on=['data', 'turno'], 
                                    right_on=['DATA', 'TURNO'], how='left')
            matrizB_m = matrizB_m.drop(['DATA', 'TURNO'], axis=1, errors='ignore')
            self.logger.info(f"matrizB_m after merge with trab_manha:\n {matrizB_m}")
            
            # Merge +H with matrizB for afternoon
            self.logger.info(f"matrizB_ini before filter: {matrizB_ini}")
            matrizB_t = matrizB_ini[matrizB_ini['turno'] == 'T'].copy()
            self.logger.info(f"matrizB_t after filter turno t:\n {matrizB_t}")
            
            # Convert dates for afternoon merge to date objects
            matrizB_t['data'] = pd.to_datetime(matrizB_t['data']).dt.date
            trab_tarde['DATA'] = pd.to_datetime(trab_tarde['DATA']).dt.date
            
            matrizB_t = matrizB_t.merge(trab_tarde, left_on=['data', 'turno'],
                                    right_on=['DATA', 'TURNO'], how='left')
            matrizB_t = matrizB_t.drop(['DATA', 'TURNO'], axis=1, errors='ignore')
            self.logger.info(f"matrizB_t after merge with trab_tarde:\n {matrizB_t}")
            
            # Combine morning and afternoon
            matrizB_ini = pd.concat([matrizB_m, matrizB_t], ignore_index=True)
            self.logger.info(f"matrizB_ini: {matrizB_ini}")
            
            # Get param_pess_obj from external data or set default
            param_pess_obj = self.external_call_data.get('param_pessoas_objetivo', 0.5)
            
            # Calculate objective function and diff
            matrizB_ini['max_turno'] = pd.to_numeric(matrizB_ini['max_turno'], errors='coerce')
            matrizB_ini['min_turno'] = pd.to_numeric(matrizB_ini['min_turno'], errors='coerce')
            matrizB_ini['sd_turno'] = pd.to_numeric(matrizB_ini['sd_turno'], errors='coerce')
            matrizB_ini['media_turno'] = pd.to_numeric(matrizB_ini['media_turno'], errors='coerce')
            matrizB_ini['+H'] = pd.Series(pd.to_numeric(matrizB_ini['+H'], errors='coerce')).fillna(0)
            self.logger.info(f"matrizB_ini after merge with trab_tarde: {matrizB_ini}")
            
            # Calculate aux (coefficient of variation)
            matrizB_ini['aux'] = np.where(
                matrizB_ini['media_turno'] != 0,
                matrizB_ini['sd_turno'] / matrizB_ini['media_turno'],
                0
            )
            
            # Calculate pessObj (target people)
            matrizB_ini['pess_obj'] = np.where(
                matrizB_ini['aux'] >= param_pess_obj,
                np.ceil(matrizB_ini['media_turno']),
                np.round(matrizB_ini['media_turno'])
            )
            
            # Calculate diff (difference between actual and target)
            matrizB_ini['diff'] = matrizB_ini['+H'] - matrizB_ini['pess_obj']
            
            # Ensure min_turno is at least 1
            matrizB_ini['min_turno'] = np.where(matrizB_ini['min_turno'] == 0, 1, matrizB_ini['min_turno'])
            
            # Create final matrizB
            matrizB = matrizB_ini.copy()
            
            # Add weekday
            matrizB['data'] = pd.to_datetime(matrizB['data'])
            matrizB['WDAY'] = matrizB['data'].dt.dayofweek + 1
            self.logger.info(f"matrizB after adding WDAY: {matrizB}")
            
            # Create backup
            matrizB_bk = matrizB.copy()
            
            # Calculate holiday work statistics
            matrizA_bk_with_dyf = matrizA_bk.merge(
                matrizA_bk_og[['matricula', 'dyf_max_t']], 
                on='matricula', 
                how='left'
            )
            
            # Calculate festH (holiday work hours)
            fest_h_data = []
            for colab in matrizA_bk['matricula']:
                colab_data = matriz2_bk[
                    (matriz2_bk['COLABORADOR'] == colab) & 
                    (matriz2_bk['DIA_TIPO'] == 'domYf') & 
                    (matriz2_bk['WDAY'] != 1)
                ].copy()
                
                if len(colab_data) > 0:
                    # Group by DATA and check for work indicators
                    daily_work = colab_data.groupby('DATA').agg({
                        'HORARIO': lambda x: (x.astype(str).str.contains('H|NL', case=False, na=False)).any()
                    }).reset_index()
                    daily_work.columns = ['DATA', 'dias_h']
                    
                    # Add OUT and DFS indicators
                    out_dfs_data = colab_data.groupby('DATA').agg({
                        'HORARIO': lambda x: ((x.astype(str) == 'OUT') | (x.astype(str) == 'DFS')).any()
                    }).reset_index()
                    out_dfs_data.columns = ['DATA', 'nl_out_dfs']
                    
                    daily_work = daily_work.merge(out_dfs_data, on='DATA', how='left')
                    daily_work['dias_h'] = daily_work['dias_h'] | daily_work['nl_out_dfs']
                    
                    fest_h = daily_work['dias_h'].sum()
                else:
                    fest_h = 0
                
                fest_h_data.append({'matricula': colab, 'fest_h': fest_h})
            
            fest_h_df = pd.DataFrame(fest_h_data)
            
            # Merge with matrizA_bk
            matrizA_bk = matrizA_bk.merge(fest_h_df, on='matricula', how='left')
            matrizA_bk['fest_h'] = matrizA_bk['fest_h'].fillna(0)
            
            # Calculate minFestH based on DyF_MAX_T
            def calculate_min_fest_h(row):
                dyf_max_t = row['dyf_max_t'] if 'dyf_max_t' in row else 0
                fest_h = row['fest_h']
                
                if dyf_max_t == 33:
                    return round(fest_h * 0.6)
                elif dyf_max_t == 22:
                    return round(fest_h * 0.5)
                elif dyf_max_t == 5:
                    return round(fest_h * 0.3)
                else:
                    return 0
            
            matrizA_bk['min_fest_h'] = matrizA_bk.apply(calculate_min_fest_h, axis=1)
            matrizA_bk['obj_fes'] = matrizA_bk['fest_h'] - matrizA_bk['min_fest_h']
            
            # Drop intermediate columns
            matrizA_bk = matrizA_bk.drop(['fest_h', 'obj_fes'], axis=1, errors='ignore')
            
            self.logger.info("func_inicializa MatrizB creation completed successfully")
            
            try:
                self.logger.info("Storing final results in medium_data")
                self.logger.info(f"DEBUG: matrizB_bk: {matrizB_bk}")
                # Store final results in transformed_data
                self.medium_data.update({
                    'df_colaborador': matrizA_bk.copy(),
                    'df_calendario': matriz2_bk.copy(), 
                    'df_estimativas': matrizB_bk.copy(),
                    'tipos_de_turno': tipos_de_turno,
                    'matrizA_bk_og': matrizA_bk_og.copy(),
                    'matriz_data_turno_bk': matriz_data_turno_bk.copy(),
                    'logs': logs
                })
                
                if not self.medium_data:
                    self.logger.warning("Medium data storage verification failed")
                    return False
                    
                colaborador_data = self.medium_data.get('df_colaborador')
                calendario_data = self.medium_data.get('df_calendario')
                estimativas_data = self.medium_data.get('df_estimativas')
                
                self.logger.info(f"func_inicializa medium_data df_colaborador shape: {colaborador_data.shape if colaborador_data is not None else 'None'}")
                self.logger.info(f"func_inicializa medium_data df_calendario shape: {calendario_data.shape if calendario_data is not None else 'None'}")
                self.logger.info(f"func_inicializa medium_data df_estimativas shape: {estimativas_data.shape if estimativas_data is not None else 'None'}")
                
                # Save CSV files for debugging
                try:
                    matrizA_bk.to_csv(os.path.join('data', 'output', f'df_colaborador-{self.external_call_data.get("current_process_id", "")}-{self.auxiliary_data.get("current_posto_id", "")}.csv'), index=False, encoding='utf-8')
                    matriz2_bk.to_csv(os.path.join('data', 'output', f'df_calendario-{self.external_call_data.get("current_process_id", "")}-{self.auxiliary_data.get("current_posto_id", "")}.csv'), index=False, encoding='utf-8')
                    matrizB_bk.to_csv(os.path.join('data', 'output', f'df_estimativas-{self.external_call_data.get("current_process_id", "")}-{self.auxiliary_data.get("current_posto_id", "")}.csv'), index=False, encoding='utf-8')
                    self.logger.info("CSV debug files saved successfully")
                except Exception as csv_error:
                    self.logger.warning(f"Failed to save CSV debug files: {csv_error}")
                
                self.logger.info(f"func_inicializa logs status: {type(self.medium_data.get('logs'))}")
                self.logger.info("func_inicializa completed successfully")
                return True
            except KeyError as e:
                self.logger.error(f"KeyError when storing func_inicializa results: {e}", exc_info=True)
                return False
            except ValueError as e:
                self.logger.error(f"ValueError when storing func_inicializa results: {e}", exc_info=True)
                return False
            except Exception as e:
                self.logger.error(f"Error storing final results in func_inicializa: {e}", exc_info=True)
                return False
            
        except Exception as e:
            self.logger.error(f"Error in func_inicializa method: {e}", exc_info=True)
            return False
        
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
                results = algorithm.run(self.medium_data)

                if not results:
                    self.logger.error(f"Algorithm {algorithm_name} returned no results.")
                    return False

                if results.get('summary', {}).get('status') == 'failed':  # Fixed: was checking for 'completed' which is wrong
                    self.logger.error(f"Algorithm {algorithm_name} failed to run. Status: {results.get('summary', {}).get('status')}")
                    return False

                self.logger.info(f"DEBUG: results: {results}")

                    
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
                self.logger.info(f"DEBUG: df_results: {self.rare_data['df_results']}")
                self.rare_data['df_results'].to_csv(os.path.join('data', 'output', f'df_results-{self.external_call_data.get("current_process_id", "")}-{self.auxiliary_data.get("current_posto_id", "")}.csv'), index=False, encoding='utf-8')
                    
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
            df_colaborador = df_colaborador[['fk_colaborador', 'matricula', 'data_admissao']]
            self.logger.info(f"Adding wfm_proc_id to final_df")
            final_df['wfm_proc_id'] = self.external_call_data.get("current_process_id", "")
            self.logger.info(f"DEBUG: final_df: {final_df}")

            self.logger.info("Merging with df_colaborador")
            # Convert matricula to int for matching (remove leading zeros)
            df_colaborador['matricula_int'] = df_colaborador['matricula'].astype(str).str.lstrip('0').astype(int)
            self.logger.info(f"DEBUG: df_colaborador matricula_int: {df_colaborador['matricula_int'].tolist()}")
            final_df = pd.merge(final_df, df_colaborador, left_on='colaborador', right_on='matricula_int', how='left')
            self.logger.info(f"DEBUG: final_df: {final_df}")
            self.logger.info("Filtering dates greater than each employee's admission date")
            initial_rows = len(final_df)
            # Convert data_admissao to datetime if not already
            #self.logger.info(f"DEBUG: data_admissao: {final_df['data_admissao']}, type: {type(final_df['data_admissao'])}")
            final_df['data_admissao'] = pd.to_datetime(final_df['data_admissao'], format='%Y-%m-%d')
            #self.logger.info(f"DEBUG: data_admissao: {final_df['data_admissao']}, type: {type(final_df['data_admissao'])}")
            # Filter per employee: each row's date must be > that employee's admission date
            if 'data' in final_df.columns:
                final_df['data'] = pd.to_datetime(final_df['data'])
                final_df = final_df[final_df['data'] >= final_df['data_admissao']].copy()
                #self.logger.info(f"DEBUG: data: {final_df['data']}, type: {type(final_df['data'])}")
            elif 'date' in final_df.columns:
                final_df['date'] = pd.to_datetime(final_df['date'])
                #self.logger.info(f"DEBUG: date: {final_df['date']}, type: {type(final_df['date'])}")
                final_df = final_df[final_df['date'] >= final_df['data_admissao']].copy()
            
            filtered_rows = len(final_df)
            self.logger.info(f"Filtered {initial_rows - filtered_rows} rows (from {initial_rows} to {filtered_rows}) based on admission dates")

            self.logger.info(f"DEBUG: final_df: {final_df}")

            self.logger.info("Converting types out")
            final_df = convert_types_out(pd.DataFrame(final_df))
            self.logger.info(f"DEBUG: final_df:\n {final_df}")
            final_df = final_df.drop(columns=['matricula_int', 'horario', 'fk_colaborador', 'data_admissao', 'matricula_int', 'colaborador'])
            final_df = final_df.rename(columns={'matricula': 'colaborador'})
            final_df['colaborador'] = final_df['colaborador'].astype(str)
            final_df.to_csv(os.path.join('data', 'output', f'df_insert_results-{self.external_call_data.get("current_process_id", "")}-{self.auxiliary_data.get("current_posto_id", "")}.csv'), index=False, encoding='utf-8')
            #self.formatted_data['stage1_schedule'].to_csv(os.path.join('data', 'output', f'stage1_schedule-{self.external_call_data.get("current_process_id", "")}.csv'), index=False, encoding='utf-8')
            #self.formatted_data['stage2_schedule'].to_csv(os.path.join('data', 'output', f'stage2_schedule-{self.external_call_data.get("current_process_id", "")}.csv'), index=False, encoding='utf-8')
            self.logger.info("format_results completed successfully")
            self.logger.info(f"DEBUG: final_df:\n {final_df}")
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
        
    def insert_results(self, data_manager: BaseDataManager, query_path: str = os.path.join(ROOT_DIR, 'src', 'sql_querys', 'insert_results.sql')) -> bool:
        """
        Method for inserting results in the data source.
        """
        try:
            self.logger.info("Entered insert_results method.")
            final_df = self.formatted_data['df_final'].copy()
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
