"""Example service implementation for the my_new_project project.

This service demonstrates how to use the process management framework to create
a coordinated multi-stage data processing flow.
"""

import logging
from typing import Dict, Any, Optional, List, Union, Type, cast
from datetime import datetime
import pandas as pd

# Import base_data_project components
from src.algorithms.factory import AlgorithmFactory
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.process_management.manager import ProcessManager
from base_data_project.process_management.stage_handler import ProcessStageHandler
from base_data_project.service import BaseService
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.storage.models import BaseDataModel
from base_data_project.log_config import get_logger

# Import project-specific components
from src.configuration_manager.manager import ConfigurationManager
from src.models import DescansosDataModel
from src.algorithms.factory import AlgorithmFactory
from src.helpers import set_process_errors
from src.orquestrador_functions.Logs.message_loader import set_messages

class AlgoritmoGDService(BaseService):
    """
    Example service class that demonstrates how to coordinate data management,
    process tracking, and algorithm execution.
    
    This service implements a complete process flow with multiple stages:
    1. Data Loading: Load datab from sources
    2. Data Transformation: Clean and prepare the data
    3. Processing: Apply algorithms to the data
    4. Result Analysis: Analyze and save the results
    """

    def __init__(self, data_manager: BaseDataManager, project_name: str, process_manager: Optional[ProcessManager] = None, external_call_dict: Dict[str, Any] = {}, config_manager: ConfigurationManager = None, external_raw_connection=None):
        """
        Initialize the service with data and process managers.
        
        Args:
            data_manager: Data manager for data operations
            process_manager: Optional process manager for tracking
        """

        # Import CONFIG if not provided
        if config_manager is None:
            config_manager = ConfigurationManager()
        
        self.config_manager = config_manager

        # Work around the config property issue
        if process_manager:
            try:
                process_manager.config = config_manager
            except AttributeError:
                # If config is a property without setter, use __dict__ directly
                process_manager.__dict__['config'] = config_manager

        super().__init__(
            data_manager=data_manager, 
            process_manager=process_manager, 
            project_name=project_name,
            data_model_class=cast(BaseDataModel, DescansosDataModel)  # Tell the linter this is okay
        )

        # Override data model creation to pass config_manager
        self.data_model = DescansosDataModel(
            data_container=self.data_container,
            project_name=project_name,
            config_manager=config_manager
        )

        # Storing data here to pass it to data model in the first stage (when the class is instanciated)
        self.external_data = {
            'current_process_id': external_call_dict.get('current_process_id', 0), # TODO: Check this default
            'api_proc_id': external_call_dict.get('api_proc_id', 0),                 # arg1
            'wfm_proc_id': external_call_dict.get('wfm_proc_id', 0),                 # arg2
            'wfm_user': external_call_dict.get('wfm_user', 0),                       # arg3
            'start_date': external_call_dict.get('start_date', 0),                   # arg4
            'end_date': external_call_dict.get('end_date', 0),                       # arg5
            'wfm_proc_colab': external_call_dict.get('wfm_proc_colab', 0),           # arg6
            'child_number': external_call_dict.get('child_number', 1),               # arg7
        } if external_call_dict is not None else {}

        # Process tracking
        self.stage_handler = process_manager.get_stage_handler() if process_manager else None
        self.algorithm_results = {}

        self._register_decision_points()
        
        self.logger = get_logger(project_name)
        self.logger.info(f"project_name in service init: {project_name}")
        
        # Setup database connection for error logging - needed to pass as argument
        self.raw_connection = None
        if external_raw_connection is not None:
            # Use the provided external connection (from orquestrador)
            self.raw_connection = external_raw_connection
            self.logger.info("Using external database connection for error logging")
            
        #elif CONFIG.get('logging', {}).get('log_errors_db', True):
        elif config_manager.system.logging_config.get('log_errors_db', True):
            from base_data_project.data_manager.managers.managers import DBDataManager
            if isinstance(data_manager, DBDataManager):
                try:
                    self.raw_connection = data_manager.session.connection().connection
                    self.logger.info("Database connection established for error logging")
                except Exception as e:
                    self.logger.warning(f"Failed to establish database connection for error logging: {e}")
                    self.raw_connection = None
            else:
                self.logger.info("Non-database data manager detected, database error logging disabled")
        else:
            self.logger.info("Database error logging disabled in configuration")
        
        self.logger.info("AlgoritmoGDService initialized")

    def _register_decision_points(self):
        """Register decision points from config with the process manager"""
        if not self.process_manager:
            return
            
        #stages_config = CONFIG.get('stages', {})
        stages_config = self.config_manager.stages.stages
        
        for stage_name, stage_config in stages_config.items():
            sequence = stage_config.get('sequence')
            decisions = stage_config.get('decisions', {})
            
            if decisions and sequence is not None:
                self.logger.info(f"Registering stage {stage_name} with full decisions: {decisions}")
                # Flatten all decision groups into one dict of defaults
                defaults = {}
                for decision_group in decisions.values():
                    if isinstance(decision_group, dict):
                        defaults.update(decision_group)
                
                # Register with process manager
                self.process_manager.register_decision_point(
                    stage=sequence,
                    schema=dict,  # Keep it simple with dict schema
                    required=True,
                    defaults=defaults
                )
                
                self.logger.info(f"Registered decisions for stage {stage_name} (seq: {sequence})")

    def _dispatch_stage(self, stage_name, algorithm_name = None, algorithm_params = None):
        """Dispatch to appropriate stage method."""

        # Execute the appropriate stage
        if stage_name == "data_loading":
            return self._load_process_data()
        elif stage_name == "processing":
            return self._execute_processing_stage(algorithm_name=algorithm_name, algorithm_params=algorithm_params)
        else:
            self.logger.error(f"Unknown stage name: {stage_name}")
            return False

    def _load_process_data(self) -> bool:
        """
        Execute the data loading stage.
        
        This stage loads data from the data source(s).
        
        Returns:
            True if successful, False otherwise
        """
        try:
            stage_name = 'data_loading'
            self.logger.info("Executing process data loading stage")
            # Get decisions from process manager if available
            load_entities_dict = self.config_manager.paths.sql_processing_paths
    
            # Track progress
            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name, 
                    0.1, 
                    "Starting data loading raw"
                )
            self.logger.info(f"DEBUGGING: config_manager: {self.config_manager}")
            # Use existing data_model instance from BaseService
            # Declare the messages dataframe
            messages_df = self.data_model.auxiliary_data.get('messages_df', pd.DataFrame())
            child_num = str(self.external_data.get('child_number', 1))
            # Progress update
            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name, 
                    0.3, 
                    "Starting load raw data entities",
                    {"entities": load_entities_dict}
                )

            valid_process_loading, error_code, error_message = self.data_model.load_process_data(self.data_manager, load_entities_dict)

            if not valid_process_loading:
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name,
                        0.0,
                        "Failed to load raw data.",
                        {"valid_process_loading": valid_process_loading}
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        #pathOS=ROOT_DIR,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type='data_loading',
                        error_code=None,
                        description=set_messages(messages_df, error_code, {'1': child_num, '2': error_message}),
                        employee_id=None,
                        schedule_day=None
                    )
                return False

            # Progress update
            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name, 
                    0.5, 
                    "Starting to validate raw data entities",
                    {"loaded_entities": list(self.data_model.auxiliary_data.keys())}
                )            
            
            valid_raw_data = self.data_model.validate_process_data()

            if not valid_raw_data:
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name,
                        0.0,
                        "Failed to validate raw data.",
                        {'valid_raw_data': valid_raw_data}
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type='data_loading',
                        error_code=None,
                        description=set_messages(messages_df, error_code, {'1': child_num, '2': error_message}),
                        employee_id=None,
                        schedule_day=None
                    )
                return False


            if self.external_data['wfm_proc_colab'] != 'NA':
                self.external_data['colab_matricula'] = self.external_data['wfm_proc_colab']
            else:
                self.external_data['colab_matricula'] = None
            
            self.logger.info("Data loading stage completed successfully")

            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name=stage_name,
                    progress=1.0,
                    message="Raw data loading complete",
                    metadata={
                        "valid_process_loading": valid_process_loading,
                        "valid_raw_data": valid_raw_data,
                        "colab_matricula": self.external_data['colab_matricula'],
                        "data_shapes": {
                            "raw_data": self.data_model.auxiliary_data['valid_emp'].shape if not self.data_model.auxiliary_data['valid_emp'].empty else None
                        }
                    }
                )
            
            # Set process errors completition for process data loading
            if self.raw_connection and not messages_df.empty:
                set_process_errors(
                    connection=self.raw_connection,
                    pathOS=self.config_manager.system.project_root_dir,
                    user='WFM',
                    fk_process=self.external_data['current_process_id'],
                    type_error='I',
                    process_type='data_loading',
                    error_code=None,
                    description=set_messages(messages_df, error_code, {'1': child_num, '2': error_message}),
                    employee_id=None,
                    schedule_day=None
                )

            return True
            
        except Exception as e:
            error_msg = f"Error in data loading stage: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name=stage_name,
                    progress=0.0,
                    message=error_msg
                )
            if self.raw_connection and not messages_df.empty:
                set_process_errors(
                    connection=self.raw_connection,
                    pathOS=self.config_manager.system.project_root_dir,
                    user='WFM',
                    fk_process=self.external_data['current_process_id'],
                    type_error='E',
                    process_type='data_loading',
                    error_code=None,
                    description=set_messages(messages_df, error_code, {'1': child_num, '2': str(e)}),
                    employee_id=None,
                    schedule_day=None
                )
            return False

    def _execute_processing_stage(self, algorithm_name: Optional[str] = None, 
                                algorithm_params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Execute the processing stage using substages. These substages could divided into various methos or the logic could be applied inside this method.
        This stage demonstrates using the substage feature and includes:
        1. connection: establish a connection to data source;
        2. load_matrixes: Load dataframes containing all the data;
        3. func_inicializa: Function that initializes data transformation for each matrix;
        4. allocation_cycle: Allocation cycle for all the required days;
        5. format_results: Format results to be inserted;
        6. insert_results: Insert results to the database.
        
        Args:
            algorithm_name: Name of the algorithm to use
            algorithm_params: Parameters for the algorithm
            
        Returns:
            True if successful, False otherwise
        """
        try:
            stage_name = 'processing'
            process_type = 'processing_stage'
            decisions = {}
            messages_df = self.data_model.auxiliary_data.get('messages_df', pd.DataFrame())
            child_num = str(self.external_data.get('child_number', 1))
            # TODO: check if it should exit the loop if anything fails or continue
            if self.stage_handler and self.process_manager:
                stage_sequence = self.stage_handler.stages[stage_name]['sequence']
                insert_results = self.process_manager.current_decisions.get(stage_sequence, {}).get('insertions', {}).get('insert_results', False)
                #algorithm_name = self.process_manager.current_decisions.get(stage_sequence, {}).get('algorithm', {}).get('name', algorithm_name)
                #algorithm_params = self.process_manager.current_decisions.get(stage_sequence, {}).get('algorithm', {}).get('parameters', algorithm_params)
                self.logger.info(f"Looking for defaults with stage_sequence: {stage_sequence}, type: {type(stage_sequence)}")
                stage_config = self.config_manager.stages.stages['stages'].get('processing', {})
                decisions = stage_config.get('decisions', {})

                self.logger.info(f"DEBUG: Decisions: {decisions}")
                self.logger.info(f"DEBUG: Decisions type: {type(decisions)}")

                # Get algorithm name from current_decisions (set in treat_params_substage)
                algorithm_name = self.process_manager.current_decisions.get(stage_sequence, {}).get('algorithm_name', '')
                self.logger.info(f"DEBUG: Retrieved algorithm_name from current_decisions: {algorithm_name}")
                
                if not algorithm_name:
                    # Fallback to parameter defaults (GD_algorithmName)
                    algorithm_name = self.config_manager.parameters.get_parameter_defaults().get('GD_algorithmName', '')
                    self.logger.info(f"DEBUG: Got algorithm_name from parameter defaults: {algorithm_name}")
                #algorithm_name = ''
                algorithm_params = decisions.get('algorithm', {}).get('parameters', algorithm_params)
                insert_results = decisions.get('insertions', {}).get('insert_results', False)
                #self.logger.info(f"Found defaults: {defaults}")
                #self.logger.info(f"Retrieving these values from config algorithm_name: {algorithm_name}, algorithm_params: {algorithm_params}, insert_results: {insert_results}")

                #if algorithm_name is None:
                #    self.logger.error("No algorithm name provided in decisions")
                #    return False

                if algorithm_params is None:
                    self.logger.error("No algorithm parameters provided in decisions")
                    return False

                # Type assertions to help type checker
                #assert isinstance(algorithm_name, str)
                assert isinstance(algorithm_params, dict)

            # Log start of the process to database
            #self.logger.info(f"DEBUG set_process_errors condition BEFORE LOOP: raw_connection={self.raw_connection is not None}, messages_df_empty={messages_df.empty}, messages_df_len={len(messages_df)}")
            
            # Check condition step by step
            #conn_check = self.raw_connection is not None
            #df_check = not messages_df.empty
            #self.logger.info(f"DEBUG CONDITION: conn_check={conn_check}, df_check={df_check}, combined={conn_check and df_check}")
            
            #if self.raw_connection and not messages_df.empty:
            #    #self.logger.info(f"DEBUG SERVICE: INSIDE THE IF CONDITION!")
            #    description = set_messages(messages_df, 'iniProc', {'1': child_num})
            #    #self.logger.info(f"DEBUG SERVICE: About to call set_process_errors with description: {description}")
            #    set_process_errors(
            #        connection=self.raw_connection,
            #        pathOS=ROOT_DIR,
            #        user='WFM',
            #        fk_process=self.external_data['current_process_id'],
            #        type_error='I',
            #        process_type=process_type,
            #        error_code=None,
            #        description=description,
            #        employee_id=None,
            #        schedule_day=None
            #    )
            #    self.logger.info(f"DEBUG SERVICE: set_process_errors call completed")
            #else:
            #    self.logger.info(f"DEBUG SERVICE: CONDITION FAILED - not calling set_process_errors")

            posto_id_list = self.data_model.auxiliary_data.get('posto_id_list', [])
            for posto_id in posto_id_list:
                #if posto_id != 121: continue # TODO: remove this, just for testing purposes
                # Save the current posto_id to the auxiliary data
                self.data_model.auxiliary_data['current_posto_id'] = posto_id
                self.logger.info(f"Current posto_id: {posto_id}")
                progress = 0.0
                # Log messages to database
                #self.logger.info(f"DEBUG set_process_errors condition: raw_connection={self.raw_connection is not None}, messages_df_empty={messages_df.empty}, messages_df_len={len(messages_df)}")
                if self.raw_connection and not messages_df.empty:
                    #self.logger.info(f"DEBUG SERVICE: INSIDE IF CONDITION for posto {posto_id}!")
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='I',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'iniSubprocPosto', {'1': child_num, '2': str(posto_id)}),
                        employee_id=None,
                        schedule_day=None
                    )

                if self.stage_handler:
                    self.stage_handler.start_substage(stage_name, 'treat_params')
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.1)/len(posto_id_list),
                        message="Starting the processing stage and consequent substages"
                    )

                # SUBSTAGE 1: treat_params
                valid_treat_params = self._execute_treatment_params_substage(stage_name)
                if not valid_treat_params:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Error treating parameters, returning False"
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidParamsTreat', {'1': child_num, '2': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.2)/len(posto_id_list),
                        message="Valid treat_params substage, advancing to next substage"
                    )

                # SUBSTAGE 2: load_matrices
                if self.stage_handler:
                    self.stage_handler.start_substage(stage_name, 'load_matrices')
                valid_loading_matrices = self._execute_load_matrices_substage(stage_name, posto_id)
                if not valid_loading_matrices:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Invalid matrices loading substage, returning False"
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidLoadMatrices', {'1': child_num, '2': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.3)/len(posto_id_list),
                        message="Valid matrices loading, advancing to the next substage"
                    )

                # SUBSTAGE 3: func_inicializa
                if self.stage_handler:
                    self.stage_handler.start_substage(stage_name, 'func_inicializa')
                valid_func_inicializa = self._execute_func_inicializa_substage(stage_name)
                if not valid_func_inicializa:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Invalid result in func_inicializa substage, returning False"
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidFuncInicializa', {'1': child_num, '2': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.4)/len(posto_id_list),
                        message="Valid func_inicializa, advancing to the next substage"
                    )

                # SUBSTAGE 4: allocation_cycle
                if self.stage_handler:
                    self.stage_handler.start_substage(stage_name, 'allocation_cycle')
                # Type assertions to help type checker
                self.logger.info(f"DEBUG: current_decisions structure: {self.process_manager.current_decisions if self.process_manager else 'No process_manager'}")
                algorithm_name = self.process_manager.current_decisions.get(2, {}).get('algorithm_name', '') if self.process_manager else ''
                self.logger.info(f"DEBUG: Algorithm name before calling allocation_cycle substage: {algorithm_name}")
                self.logger.info(f"DEBUG: Retrieved from current_decisions[2]: {self.process_manager.current_decisions.get(2, {}) if self.process_manager else 'No process_manager'}")
                assert isinstance(algorithm_name, str)
                assert isinstance(algorithm_params, dict)
                valid_allocation_cycle = self._execute_allocation_cycle_substage(algorithm_params=algorithm_params, stage_name=stage_name, algorithm_name=algorithm_name)
                if not valid_allocation_cycle:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Invalid result in allocation_cycle substage, returning False"
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidAllocationCycle', {'1': child_num, '2': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.5)/len(posto_id_list),
                        message="Valid allocation_cycle, advancing to the next substage"
                    )

                # SUBSTAGE 5: format_results
                if self.stage_handler:
                    self.stage_handler.start_substage(stage_name, 'format_results')
                valid_format_results = self._execute_format_results_substage(stage_name)
                if not valid_format_results:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Invalid result in format_results substage, returning False"
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidFormatResults', {'1': child_num, '2': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.6)/len(posto_id_list),
                        message="Valid format_results, advancing to the next substage"
                    )

                # SUBSTAGE 6: insert_results
                if insert_results:
                    if self.stage_handler:
                        self.stage_handler.start_substage(stage_name, 'insert_results')
                    valid_insert_results = self._execute_insert_results_substage(stage_name)
                    if not valid_insert_results:
                        if self.stage_handler:
                            self.stage_handler.track_progress(
                                stage_name=stage_name,
                                progress=0.0,
                                message="Invalid result in insert_results substage, returning False"
                            )
                        if self.raw_connection and not messages_df.empty:
                            set_process_errors(
                                connection=self.raw_connection,
                                pathOS=self.config_manager.system.project_root_dir,
                                user='WFM',
                                fk_process=self.external_data['current_process_id'],
                                type_error='E',
                                process_type=process_type,
                                error_code=None,
                                description=set_messages(messages_df, 'invalidInsertResults', {'1': child_num}),
                                employee_id=None,
                                schedule_day=None
                            )
                        return False
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=(progress+0.7)/len(posto_id_list),
                            message="Valid insert_results, advancing to the next substage"
                        )
                        progress += 1

            # TODO: Needs to ensure it inserted it correctly?
            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name=stage_name,
                    progress=1.0,
                    message="Finnished processing stage with success. Returnig True."
                )
            if self.raw_connection and not messages_df.empty:
                set_process_errors(
                    connection=self.raw_connection,
                    pathOS=self.config_manager.system.project_root_dir,
                    user='WFM',
                    fk_process=self.external_data['current_process_id'],
                    type_error='I',
                    process_type=process_type,
                    error_code=None,
                    description=set_messages(messages_df, 'okSubprocPosto', {'1': child_num, '2': str(posto_id), '3': ''}),
                    employee_id=None,
                    schedule_day=None
                )
            return True

        except Exception as e:
            self.logger.error(f"Error in processing stage: {str(e)}", exc_info=True)
            # TODO: add progress tracking
            if self.raw_connection and not messages_df.empty:
                set_process_errors(
                    connection=self.raw_connection,
                    pathOS=self.config_manager.system.project_root_dir,
                    user='WFM',
                    fk_process=self.external_data['current_process_id'],
                    type_error='E',
                    process_type=process_type,
                    error_code=None,
                    description=set_messages(messages_df, 'errSubprocPosto', {'1': child_num, '2': str(posto_id), '3': str(e)}),
                    employee_id=None,
                    schedule_day=None
                )
            return False

    def _execute_result_analysis_stage(self) -> bool:
        """
        Execute the result analysis stage.
        
        This stage analyzes the processing results and saves the output.
        
        Returns:
            True if successful, False otherwise
        """
        # Implement the logic if needed
        return True

    def _execute_treatment_params_substage(self, stage_name: str = 'processing') -> bool:
        """
        Execute the processing substage of connection. This could be implemented as a method or directly on the _execute_processing_stage() method
        """
        try:
            substage_name = 'treat_params'
            self.logger.info("Starting to treating parameters substage")
            
            try:
                self.logger.info(f"DEBUG: About to execute substage logic")
                self.logger.info(f"DEBUG SUBSTAGE: Inside _execute_treatment_params_substage")
            except Exception as e:
                self.logger.error(f"DEBUG: Exception in substage after line 547: {e}", exc_info=True)
                return False
            
            # Log start of parameter treatment to database
            messages_df = self.data_model.auxiliary_data.get('messages_df', pd.DataFrame())
            child_num = str(self.external_data.get('child_number', 1))
            self.logger.info(f"DEBUG SUBSTAGE: messages_df.empty = {messages_df.empty}, raw_connection = {self.raw_connection is not None}")
            if self.raw_connection and not messages_df.empty:
                self.logger.info(f"DEBUG SUBSTAGE: About to call set_process_errors for param treatment")
                description = set_messages(messages_df, 'iniSubprocPosto', {'1': child_num, '2': 'param_treatment'})
                set_process_errors(
                    connection=self.raw_connection,
                    pathOS=self.config_manager.system.project_root_dir,
                    user='WFM',
                    fk_process=self.external_data['current_process_id'],
                    type_error='I',
                    process_type='treat_params',
                    error_code=None,
                    description=description,
                    employee_id=None,
                    schedule_day=None
                )
                self.logger.info(f"DEBUG SUBSTAGE: set_process_errors completed for param treatment")
            else:
                self.logger.info(f"DEBUG SUBSTAGE: Skipping set_process_errors for param treatment - condition failed")

            # Establish connection to data source
            params = self.data_model.treat_params()
            self.logger.info(f"DEBUG SUBSTAGE: params: {params}")
            valid_params = params.get('success', False)
            algorithm_name = params.get('algorithm_name', '')
            #self.logger.info(f"DEBUG: Algorithm name: {algorithm_name}, type: {type(algorithm_name)}")

            # TODO: Add data_model method for overriding
            
            if not valid_params:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name,
                        success=False, 
                        result_data={"error": "Error treating parameters"}
                    )
                return False
            if algorithm_name == '':
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name,
                        success=False, 
                        result_data={"error": "No algorithm name provided in decisions"}
                    )
                return False

            if algorithm_name and self.process_manager:
                # Get current decisions for stage
                stage_sequence, current_decisions = self.get_decisions_for_stage('processing')
                
                # Add algorithm name to the decisions
                current_decisions['algorithm_name'] = algorithm_name
                self.logger.info(f"DEBUG: Storing algorithm_name '{algorithm_name}' in current_decisions[{stage_sequence}]")
                self.logger.info(f"DEBUG: Current decisions: {current_decisions}")
                
                # Store in current_decisions properly
                self.process_manager.current_decisions[stage_sequence] = current_decisions
                self.logger.info(f"DEBUG: After storing - current_decisions structure: {self.process_manager.current_decisions}")

            # Track progress for the connection substage
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name=stage_name, 
                    substage_name=substage_name,
                    progress=0.5,  # 50% complete
                    message="Parameters treated successfully"
                )

            valid_params_info = self.data_model.validate_params()
            if not valid_params_info:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name,
                        success=False, 
                        result_data={"error": "Error validating parameters"}
                    )
                return False

            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name=stage_name, 
                    substage_name=substage_name,
                    success=True, 
                    result_data={"params_info": "Parameters treated successfully"}
                )
            return True
    
        except Exception as e:
            self.logger.error(f"Error treating parameters: {str(e)}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name=stage_name, 
                    substage_name=substage_name, 
                    success=False, 
                    result_data={"error": str(e)}
                )
            return False
        
    def _execute_load_matrices_substage(self, stage_name: str, posto_id: int) -> bool:
        """
        Execute the processing substage of load_matrices. This could be implemented as a method or directly on the _execute_processing_stage() method
        """
        self.logger.info(f"Entering loading matrices substage for posto_id: {posto_id}")

        try:
            substage_name = "load_matrices"
            process_type = 'load_matrices_substage'
            messages_df = self.data_model.auxiliary_data.get('messages_df', pd.DataFrame())
            child_num = str(self.external_data.get('child_number', 1))
            secao_id = str(self.external_data.get('secao_id', ''))
            # Checkpoint for posto_id
            if not posto_id:
                # TODO: do something, likely raise error
                self.logger.error("No posto_id provided, cannot load matrices")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name,
                        success=False,
                        result_data={
                            'posto_id': posto_id,
                            'message': "No posto_id provided"
                        }
                    )
                return False

            # Load colaborador info (df_colaborador)
            try:
                self.logger.info(f"Loading colaborador info for posto_id: {posto_id}")
                # Has to be in this order
                # Get colaborador info using data model (it uses the data manager)
                valid_load_colaborador_info = self.data_model.load_colaborador_info(
                    data_manager=self.data_manager, 
                    posto_id=posto_id
                )
                if not valid_load_colaborador_info:
                    # Set process error for colaborador loading failure
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'noMColab', {'1': child_num, '2': str(posto_id), '3': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False, # TODO: create a progress logic values
                            result_data={"valid_load_colaborador_info": valid_load_colaborador_info}
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_substage_progress(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        progress=0.1,
                        message="Valid load colaborador info"
                    )
                
                # Validate colaborador info
                valid_colaborador_info_data = self.data_model.validate_colaborador_info()
                if not valid_colaborador_info_data:
                    # Set process error for colaborador validation failure
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidColab', {'1': child_num, '2': str(posto_id), '3': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_colaborador_info_data": valid_colaborador_info_data
                            }
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_substage_progress(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        progress=0.2,
                        message="Valid colaborador info data"
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='I',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'okColab', {'1': child_num, '2': str(posto_id), '3': ''}),
                        employee_id=None,
                        schedule_day=None
                    )

            except Exception as e:
                self.logger.error(f"Error loading colaborador info: {str(e)}", exc_info=True)
                self.logger.error(f"posto_id type: {type(posto_id)}")
                self.logger.error(f"data_manager type: {type(self.data_manager)}")
                if hasattr(self.data_model, 'auxiliary_data') and 'valid_emp' in self.data_model.auxiliary_data:
                    self.logger.error(f"valid_emp shape: {self.data_model.auxiliary_data['valid_emp'].shape}")
                    self.logger.error(f"valid_emp columns: {self.data_model.auxiliary_data['valid_emp'].columns.tolist()}")
                    self.logger.error(f"valid_emp dtypes: {self.data_model.auxiliary_data['valid_emp'].dtypes}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'errColab', {'1': child_num, '2': str(posto_id), '3': ''}),
                        employee_id=None,
                        schedule_day=None
                    )
                return False

            
            # Load estimativas info (df_estimativas)
            try:
                self.logger.info(f"Loading estimativas info for posto_id: {posto_id}")
                # Get estimativas info
                valid_load_estimativas_info = self.data_model.load_estimativas_info(
                    data_manager=self.data_manager, 
                    posto_id=posto_id,
                    start_date=self.external_data['start_date'],
                    end_date=self.external_data['end_date']
                )
                if not valid_load_estimativas_info:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False, # TODO: create a progress logic values
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info
                            }
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'noMinIdeal', {'1': child_num, '2': str(posto_id), '3': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
                
                # Validate estimativas info
                valid_estimativas_validation, invalid_entities = self.data_model.validate_estimativas_info()
                if not valid_estimativas_validation:
                    # Set process error based on invalid entities
                    messages_df = self.data_model.auxiliary_data.get('messages_df', pd.DataFrame())
                    if self.raw_connection and not messages_df.empty:
                        child_num = str(self.external_data.get('child_number', 1))
                        description_list = []
                        # Choose appropriate message key based on invalid entities
                        if 'df_faixa_horario' in invalid_entities:
                            message_key = 'errFaixaSec'
                            description_list.append(set_messages(messages_df, message_key, {'1': child_num, '2': str(secao_id), '3': ''}))
                        if 'df_granularidade' in invalid_entities:
                            message_key = 'errGran'
                            description_list.append(set_messages(messages_df, message_key, {'1': child_num, '2': str(posto_id), '3': ''}))
                        if 'df_estimativas' in invalid_entities:
                            message_key = 'errAllInfo'
                            description_list.append(set_messages(messages_df, message_key, {'1': child_num, '2': str(posto_id), '3': ''}))
                        for description in description_list:
                            set_process_errors(
                                connection=self.raw_connection,
                                pathOS=self.config_manager.system.project_root_dir,
                                user='WFM',
                                fk_process=self.external_data['current_process_id'],
                                type_error='E',
                                process_type=process_type,
                                error_code=None,
                                description=description,
                                employee_id=None,
                                schedule_day=None
                            )
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_estimativas_validation": valid_estimativas_validation,
                                "invalid_entities": invalid_entities
                            }
                        )
                    return False
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='I',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'okMinIdeal', {'1': child_num, '2': str(posto_id), '3': ''}),
                        employee_id=None,
                        schedule_day=None
                    )
            
            except Exception as e:
                self.logger.error(f"Error loading estimativas info: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'errMinIdeal', {'1': child_num, '2': str(posto_id), '3': str(e)}),
                        employee_id=None,
                        schedule_day=None
                    )
                return False
            
            # Load calendario info (df_calendario)
            try:
                self.logger.info(f"Loading calendario info for posto_id: {posto_id}")
                # Get calendario info
                valid_load_calendario_info = self.data_model.load_calendario_info(
                    data_manager=self.data_manager, 
                    process_id=self.external_data['current_process_id'],
                    posto_id=posto_id,
                    start_date=self.external_data['start_date'],
                    end_date=self.external_data['end_date']
                )
                if not valid_load_calendario_info:
                    #self.logger.error("Error loading calendario info")
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info
                            }
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'noCalendar', {'1': child_num, '2': str(posto_id), '3': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
                valid_calendario_validation, invalid_entities = self.data_model.validate_calendario_info()
                if not valid_calendario_validation:
                    # Set process error based on invalid entities
                    messages_df = self.data_model.auxiliary_data.get('messages_df', pd.DataFrame())
                    if self.raw_connection and not messages_df.empty:
                        child_num = str(self.external_data.get('child_number', 1))
                        description_list = []
                        if 'df_calendario_empty' in invalid_entities:
                            message_key = 'noCalendar'
                            description_list.append(set_messages(messages_df, message_key, {'1': child_num, '2': str(posto_id), '3': ''}))
                        if 'df_calendario_missing' in invalid_entities:
                            message_key = 'errCalendar'
                            description_list.append(set_messages(messages_df, message_key, {'1': child_num, '2': str(posto_id), '3': ''}))
                        for description in description_list:
                            set_process_errors(
                                connection=self.raw_connection,
                                pathOS=self.config_manager.system.project_root_dir,
                                user='WFM',
                                fk_process=self.external_data['current_process_id'],
                                type_error='E',
                                process_type=process_type,
                                error_code=None,
                                description=description,
                                employee_id=None,
                                schedule_day=None
                            )
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info,
                                "valid_calendario_validation": valid_calendario_validation,
                                "invalid_entities": invalid_entities
                            }
                        )
                    return False
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='I',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'okCalendar', {'1': child_num, '2': str(posto_id), '3': ''}),
                        employee_id=None,
                        schedule_day=None
                    )
            
            except Exception as e:
                self.logger.error(f"Error loading calendario info: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'errCalendar', {'1': child_num, '2': str(posto_id), '3': str(e)}),
                        employee_id=None,
                        schedule_day=None
                )
                return False
            
            try:
                self.logger.info(f"Loading estimativas transformations for posto_id: {posto_id}")
                # Do all the merges and data transformations
                valid_estimativas_transformations = self.data_model.load_estimativas_transformations()
                if not valid_estimativas_transformations:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info,
                                "valid_estimativas_transformations": valid_estimativas_transformations                            
                            }
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidMinIdeal', {'1': child_num, '2': str(posto_id), '3': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
                
            except Exception as e:
                self.logger.error(f"Error loading estimativas transformations: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'invalidMinIdeal', {'1': child_num, '2': str(posto_id), '3': str(e)}),
                        employee_id=None,
                        schedule_day=None
                    )
                return False

            try:
                self.logger.info(f"Loading colaborador transformations for posto_id: {posto_id}")
                valid_colaborador_transformations = self.data_model.load_colaborador_transformations()
                if not valid_colaborador_transformations:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info,
                                "valid_estimativas_transformations": valid_estimativas_transformations,
                                "valid_colaborador_transformations": valid_colaborador_transformations                
                            }
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidColab', {'1': child_num, '2': str(posto_id), '3': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
            
            except Exception as e:
                self.logger.error(f"Error loading colaborador transformations: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'invalidColab', {'1': child_num, '2': str(posto_id), '3': str(e)}),
                        employee_id=None,
                        schedule_day=None
                    )
                return False
            
            try:
                self.logger.info(f"Loading calendario transformations for posto_id: {posto_id}")
                valid_calendario_transformations = self.data_model.load_calendario_transformations()
                if not valid_calendario_transformations:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info,
                                "valid_estimativas_transformations": valid_estimativas_transformations,
                                "valid_colaborador_transformations": valid_colaborador_transformations,
                                "valid_calendario_transformations": valid_calendario_transformations                   
                            }
                        )
                    if self.raw_connection and not messages_df.empty:
                        set_process_errors(
                            connection=self.raw_connection,
                            pathOS=self.config_manager.system.project_root_dir,
                            user='WFM',
                            fk_process=self.external_data['current_process_id'],
                            type_error='E',
                            process_type=process_type,
                            error_code=None,
                            description=set_messages(messages_df, 'invalidCalendar', {'1': child_num, '2': str(posto_id), '3': ''}),
                            employee_id=None,
                            schedule_day=None
                        )
                    return False
            except Exception as e:
                self.logger.error(f"Error loading calendario transformations: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                if self.raw_connection and not messages_df.empty:
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'invalidCalendar', {'1': child_num, '2': str(posto_id), '3': str(e)}),
                        employee_id=None,
                        schedule_day=None
                    )
                return False
            
            try:
                self.logger.info(f"Validating loaded matrices for posto_id: {posto_id}")
                # Ensure the loaded data is valid
                valid_substage = self.data_model.validate_matrices_loading()
                if not valid_substage:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info
                            }
                        )
                    return False
            except Exception as e:
                self.logger.error(f"Error validating loaded matrices: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False
            
                
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name=stage_name,
                    substage_name=substage_name,
                    success=True,
                    result_data={
                            "valid_load_colaborador_info": valid_load_colaborador_info,
                            "valid_load_estimativas_info": valid_load_estimativas_info,
                            "valid_load_calendario_info": valid_load_calendario_info
                    }
                )
            
            return True

        except Exception as e:
                self.logger.error(f"Error loading matrices: {str(e)}", exc_info=True)
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False

    def _execute_func_inicializa_substage(self, stage_name: str = 'processing') -> bool:
        """
        Execute the processing substage of func_inicializa. This could be implemented as a method or directly on the _execute_processing_stage() method.
        """
        try:
            substage_name = 'func_inicializa'
            self.logger.info("Initializing func inicializa substage")
            # TODO: define semanas restantes
            start_date = self.external_data.get('start_date')
            end_date = self.external_data.get('end_date')
            
            if not isinstance(start_date, str) or not isinstance(end_date, str):
                self.logger.error("Invalid start_date or end_date")
                return False
                
            success = self.data_model.func_inicializa(
                start_date=start_date,
                end_date=end_date,
                fer=self.data_model.auxiliary_data.get('df_festivos'),
            )
            if not success:
                self.logger.warning("Performing func_inicializa unsuccessful, returning False")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        success=False
                    )
                return False
            
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name=stage_name,
                    substage_name=substage_name,
                    progress=0.5,
                    message="func_inicializa successful, running validations"
                )
            
            validation_result = self.data_model.validate_func_inicializa()
            self.logger.info(f"func_inicializa returning: {validation_result}")
            if not validation_result:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        success=False,
                        result_data={
                            'success_func_inicializa': success,
                            'validation_result': validation_result
                        }
                    )
                return False
            
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name='processing',
                    substage_name='func_inicializa',
                    success=True,
                    result_data={
                        'func_inicializa_success': success,
                        'validation_result': validation_result,
                        'data': self.data_model.medium_data
                    }
                )
            return True
        
        except Exception as e:
            self.logger.error(f"Error running func_inicializa substage: {e}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "processing", 
                    "func_inicializa", 
                    False, 
                    {"error": str(e)}
                )
            return False

    def _execute_allocation_cycle_substage(self, algorithm_params: Dict[str, Any], stage_name: str = 'processing', algorithm_name: str = 'example_algorithm') -> bool:
        """
        Execute the processing substage of allocation_cycle.  This could be implemented as a method or directly on the _execute_processing_stage() method.
        """
        try:
            substage_name = 'allocation_cycle'
            self.logger.info("Initializing allocation cycle substage")
            
            self.logger.info(f"DEBUG SUBSTAGE: Inside _execute_allocation_cycle_substage")
            
            # Log start of algorithm execution to database
            #messages_df = self.data_model.auxiliary_data.get('messages_df', pd.DataFrame())
            #child_num = str(self.external_data.get('child_number', 1))
            #self.logger.info(f"DEBUG SUBSTAGE ALLOC: messages_df.empty = {messages_df.empty}, raw_connection = {self.raw_connection is not None}")
            #if self.raw_connection and not messages_df.empty:
            #    self.logger.info(f"DEBUG SUBSTAGE ALLOC: About to call set_process_errors for allocation_cycle")
            #    set_process_errors(
            #        connection=self.raw_connection,
            #        pathOS=ROOT_DIR,
            #        user='WFM',
            #        fk_process=self.external_data['current_process_id'],
            #        type_error='I',
            #        process_type='allocation_cycle',
            #        error_code=None,
            #        description=set_messages(messages_df, 'iniSubprocPosto', {'1': child_num, '2': 'allocation_cycle'}),
            #        employee_id=None,
            #        schedule_day=None
            #    )
            #    self.logger.info(f"DEBUG SUBSTAGE ALLOC: set_process_errors completed for allocation_cycle")
            #else:
            #    self.logger.info(f"DEBUG SUBSTAGE ALLOC: Skipping set_process_errors for allocation_cycle - condition failed")

            valid_algorithm_run = self.data_model.allocation_cycle(
                algorithm_name=algorithm_name, 
                algorithm_params=algorithm_params,
            )
            if not valid_algorithm_run:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        success=False,
                        result_data={
                            'valid_algorithm_run': valid_algorithm_run
                        }
                    )
                return False
            
            valid_algorithm_results = self.data_model.validate_allocation_cycle()
            if not valid_algorithm_results:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        success=False,
                        result_data={
                            'valid_algorithm_run': valid_algorithm_run,
                            'valid_algorithm_results': valid_algorithm_results
                        }
                    )
                return False

                    
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name=stage_name,
                    substage_name=substage_name,
                    success=True,
                    result_data={
                        'valid_algorithm_run': valid_algorithm_run,
                        'valid_algorithm_results': valid_algorithm_results                        
                    }
                )

            return True

        except Exception as e:
            self.logger.error(f"Error in algorithm stage: {str(e)}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "data_loading", 
                    "allocation_cycle", 
                    False, 
                    {"error": str(e)}
                )
            return False

    def _execute_format_results_substage(self, stage_name: str) -> bool:
        """
        Execute the processing substage of format_results for insertion. This could be implemented as a method or directly on the _execute_processing_stage() method.
        """
        try:
            self.logger.info(f"Starting format_results substage for stage: {stage_name}")
            stage_name = 'format_results'
            success = self.data_model.format_results()
            if not success:
                self.logger.warning("Performing allocation_cycle unsuccessful, returning False")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name='format_results',
                        success=False
                    )
                return False
            
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name='processing',
                    substage_name='format_results',
                    progress=0.5,
                    message="format_results successful, running validations"
                )
            
            validation_result = self.data_model.validate_format_results()
            self.logger.info(f"format_results returning: {validation_result}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name='processing',
                    substage_name='format_results',
                    success=True,
                    result_data={
                        'format_results_success': success,
                        'validation_result': validation_result,
                        'data': self.data_model.formatted_data
                    }
                )
            return validation_result
        except Exception as e:
            self.logger.error(f"Error in format_results substage: {str(e)}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "processing", 
                    "format_results", 
                    False, 
                    {"error": str(e)}
                )
            return False

    def _execute_insert_results_substage(self, stage_name: str) -> bool:
        """
        Execute the processing substage of insert_result.  This could be implemented as a method or directly on the _execute_processing_stage() method.
        """
        try:
            self.logger.info(f"Starting insert_results substage for stage: {stage_name}")
            messages_df = self.data_model.auxiliary_data.get('messages_df', pd.DataFrame())
            child_num = str(self.external_data.get('child_number', 1))
            posto_id = self.data_model.auxiliary_data.get('current_posto_id', None)
            process_type = self.external_data.get('process_type', None)

            success = self.data_model.insert_results(data_manager=self.data_manager)
            if not success:
                self.logger.warning("Performing insert_results unsuccessful, returning False")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name='processing',
                        substage_name='insert_results',
                        success=False
                    )
                set_process_errors(
                    connection=self.raw_connection,
                    pathOS=self.config_manager.system.project_root_dir,
                    user='WFM',
                    fk_process=self.external_data['current_process_id'],
                    type_error='E',
                    process_type=process_type,
                    error_code=None,
                    description=set_messages(messages_df, 'errInsertWFM', {'1': child_num, '2': str(posto_id)}),
                    employee_id=None,
                    schedule_day=None
                )
                return False
            
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name='processing',
                    substage_name='insert_results',
                    progress=0.5,
                    message="insert_results successful, running validations"
                )

            validation_result = self.data_model.validate_insert_results(data_manager=self.data_manager)
            if not validation_result:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name='processing',
                        substage_name='insert_results',
                        success=False
                    )
                    set_process_errors(
                        connection=self.raw_connection,
                        pathOS=self.config_manager.system.project_root_dir,
                        user='WFM',
                        fk_process=self.external_data['current_process_id'],
                        type_error='E',
                        process_type=process_type,
                        error_code=None,
                        description=set_messages(messages_df, 'errInsertWFM', {'1': child_num, '2': str(posto_id)}),
                        employee_id=None,
                        schedule_day=None
                    )
                return False

            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name='processing',
                    substage_name='insert_results',
                    success=True,
                    result_data={
                        'insert_results_success': success,
                        'validation_result': validation_result,
                    }
                )
            set_process_errors(
                connection=self.raw_connection,
                pathOS=self.config_manager.system.project_root_dir,
                user='WFM',
                fk_process=self.external_data['current_process_id'],
                type_error='I',
                process_type=process_type,
                error_code=None,
                description=set_messages(messages_df, 'okInsertWFM', {'1': child_num, '2': str(posto_id)}),
                employee_id=None,
                schedule_day=None
            )
            return validation_result            
        except Exception as e:
            self.logger.error(f"Error in insert_results substage: {str(e)}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "processing", 
                    "insert_results", 
                    False, 
                    {"error": str(e)}
                )
            return False            

    def finalize_process(self) -> None:
        """Finalize the process and clean up any resources."""
        self.logger.info("Finalizing process")
        
        # Nothing to do if no process manager
        if not self.stage_handler:
            return
        
        # Log completion
        self.logger.info(f"Process {self.current_process_id} completed")

    def get_process_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current process.
        
        Returns:
            Dictionary with process summary information
        """
        if self.stage_handler:
            return self.stage_handler.get_process_summary()
        else:
            return {
                "status": "no_tracking",
                "process_id": self.current_process_id
            }

    def get_stage_decision(self, stage: int, decision_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific decision for a stage from the process manager.
        
        Args:
            stage: Stage number
            decision_name: Name of the decision
            
        Returns:
            Decision dictionary or None if not available
        """
        if self.process_manager:
            return self.process_manager.current_decisions.get(stage, {}).get(decision_name)
        return None