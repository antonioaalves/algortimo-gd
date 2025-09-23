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
from src.config import PROJECT_NAME, CONFIG, ROOT_DIR
from src.helpers import calcular_max
from src.data_models.functions.loading_functions import load_valid_emp_csv
from src.data_models.functions.helper_functions import (
    count_dates_per_year,
    convert_types_out,
    bulk_insert_with_query
)
from src.data_models.validations.load_process_data_validations import (
    validate_posto_id,
)
from src.algorithms.factory import AlgorithmFactory


# Set up logger
logger = get_logger(PROJECT_NAME)


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
                self.logger.info(f"Loaded information from auxiliary_data into posto_id: {posto_id}")
            except Exception as e:
                self.logger.error(f"Error getting posto_id from auxiliary_data: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            # Validate input parameters
            if not validate_posto_id(posto_id):
                self.logger.error(f"posto_id provided is invalid: {posto_id}")
                return False, "errSubproc", "Invalid posto_id"

            # TODO: create validate function
            if start_date == '' or start_date == None or end_date == '' or end_date == None:
                self.logger.error(f"start_date or end_date provided are empty. start: {start_date}, end_date: {end_date}")
                return False, "errSubproc", "Invalid date parameters"

            try:
                # TODO: Review this
                self.logger.info("Initializing df_estimativas as an empty dataframe")
                # df_estimativas borns as an empty dataframe
                df_estimativas = pd.DataFrame()

                columns_select = ['nome', 'emp', 'fk_tipo_posto', 'loja', 'secao', 'h_tm_in', 'h_tm_out', 'h_tt_in', 'h_tt_out', 'h_seg_in', 'h_seg_out', 'h_ter_in', 'h_ter_out', 'h_qua_in', 'h_qua_out', 'h_qui_in', 'h_qui_out', 'h_sex_in', 'h_sex_out', 'h_sab_in', 'h_sab_out', 'h_dom_in', 'h_dom_out', 'h_fer_in', 'h_fer_out'] # TODO: define what columns to select
                self.logger.info(f"Columns to select: {columns_select}")
            except Exception as e:
                self.logger.error(f"Error initializing estimativas info: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            try:
                self.logger.info(f"Loading df_turnos from raw_data df_colaborador with columns selected: {columns_select}")
                # Get sql file path, if the cvs is being used, it gets the the path defined on dummy_data_filepaths
                # turnos information: doesnt need a query since is information resent on core_alg_params
                
                df_turnos = self.raw_data['df_colaborador'].copy()
                df_turnos = df_turnos[columns_select]
                self.logger.info(f"df_turnos shape (rows {df_turnos.shape[0]}, columns {df_turnos.shape[1]}): {df_turnos.columns.tolist()}")
            except Exception as e:
                self.logger.error(f"Error processing df_turnos from colaborador data: {e}", exc_info=True)
                return False, "errSubproc", str(e)

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
                return False, "errSubproc", str(e)

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
                return False, "errSubproc", str(e)

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
                return False, "errSubproc", str(e)

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
                return False, "errSubproc", str(e)

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
                return False, "errSubproc", str(e)

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
            df_colaborador = df_colaborador[['fk_colaborador', 'matricula', 'data_admissao', 'data_demissao']]

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
            # Convert matricula to int for matching (remove leading zeros)
            df_colaborador['matricula_int'] = df_colaborador['matricula'].astype(str).str.lstrip('0').astype(int)
            #self.logger.info(f"DEBUG: df_colaborador matricula_int: {df_colaborador['matricula_int'].tolist()}")
            final_df = pd.merge(final_df, df_colaborador, left_on='colaborador', right_on='matricula_int', how='left')
            #self.logger.info(f"DEBUG: final_df: {final_df}")
            self.logger.info("Filtering dates greater than each employee's admission date")
            initial_rows = len(final_df)
            # Convert data_admissao to datetime if not already
            #self.logger.info(f"DEBUG: data_admissao: {final_df['data_admissao']}, type: {type(final_df['data_admissao'])}")
            final_df['data_admissao'] = pd.to_datetime(final_df['data_admissao'], format='%Y-%m-%d')
            
            # Handle data_demissao: replace "0" or non-date values with NaT before conversion
            final_df['data_demissao'] = final_df['data_demissao'].replace(['0', 0], pd.NaT)
            final_df['data_demissao'] = pd.to_datetime(final_df['data_demissao'], format='%Y-%m-%d', errors='coerce')
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
            final_df = final_df.drop(columns=['matricula_int', 'horario', 'fk_colaborador', 'data_admissao', 'matricula_int', 'colaborador'])
            final_df = final_df.rename(columns={'matricula': 'colaborador'})
            final_df['colaborador'] = final_df['colaborador'].astype(str)
            final_df.to_csv(os.path.join('data', 'output', f'df_insert_results-{self.external_call_data.get("current_process_id", "")}-{self.auxiliary_data.get("current_posto_id", "")}.csv'), index=False, encoding='utf-8')
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
        
    def insert_results(self, data_manager: BaseDataManager, query_path: str = os.path.join(ROOT_DIR, 'src', 'sql_querys', 'insert_results.sql')) -> bool:
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
