"""Data model for Alcampo on the pre-STRSOL-1279 CAV schema.

Sibling of SalsaDataModel. Quotas come from core_algorithm_variables.
Salsa stays on core_pro_emp_contract, process rules, movements, and WFM annual variables.
"""

from typing import Dict, Any, Tuple
import os

import pandas as pd

from base_data_project.storage.containers import BaseDataContainer
from base_data_project.data_manager.managers.managers import CSVDataManager, DBDataManager
from base_data_project.data_manager.managers.base import BaseDataManager
from src.data_models.base import BaseDescansosDataModel
from src.configuration_manager.base import BaseConfig
from src.configuration_manager.instance import get_config
from src.data_models.functions.helper_functions import (
    count_dates_per_year,
    get_param_for_posto,
    get_valid_emp_info,
    get_first_and_last_day_passado_arguments,
    get_section_employees_id_list,
    get_past_employees_id_list,
    get_matriculas_for_employee_id,
    get_employee_id_matriculas_map_dict,
    get_df_estrutura_wfm_info,
    create_employee_query_string,
    count_holidays_in_period,
    count_sundays_in_period,
    convert_fields_to_int,
    is_eci_unit,
    get_sibling_section_name,
    treat_df_faixa_secao_to_long,
)
from src.data_models.functions.data_treatment_functions import (
    treat_df_valid_emp,
    treat_df_closed_days,
    treat_df_feriados,
    treat_df_contratos,
    treat_df_calendario_passado,
    treat_df_ausencias_ferias,
    treat_df_disponibilidade,
    set_tipo_contrato_to_df_colaborador,
    add_prioridade_folgas_to_df_colaborador,
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
    add_days_off,
    filter_df_dates,
    extract_tipos_turno,
    add_date_related_columns,
    define_dia_tipo,
    adjust_horario_for_admission_date,
    calculate_and_merge_allocated_employees,
    restrict_turnos_by_disponibilidade,
    date_adjustments_to_df_colaborador,
    adjust_counters_for_contract_types,
    merge_contract_data,
    fill_calendario_passado_defaults,
    fill_estimativas_passado_grid,
)
from src.data_models.functions.loading_functions import load_valid_emp_csv
from src.data_models.validations.load_process_data_validations import (
    validate_parameters_cfg,
    validate_employees_id_list,
    validate_posto_id_list,
    validate_df_valid_emp,
    validate_past_employee_id_list,
    validate_date_passado,
    validate_df_feriados,
    validate_df_ausencias_ferias,
    validate_valid_emp_info,
    validate_num_sundays_year,
    validate_df_estrutura_wfm,
)
from src.data_models.validations.func_inicializa_validations import validate_all_core_dataframes

_LQ_KEEP_CAV = ('CICLO', 'MOT', 'P')
_ANNUAL_APPLY_COLS = (
    'apply_l_dom', 'apply_c2d', 'apply_l_sab', 'apply_l_dom_or_sab',
    'apply_total_l', 'apply_c3d', 'apply_l_d', 'apply_cxx', 'apply_tc',
)


class AlcampoDataModel(BaseDescansosDataModel):
    """CAV load, quota math, and medium_data for the Alcampo solver."""

    def __init__(self, data_container: BaseDataContainer, project_name: str = 'algoritmo_GD', config_manager: BaseConfig = None, external_data: Dict[str, Any] = None):
        self.config_manager = config_manager if config_manager is not None else get_config()
        super().__init__(data_container=data_container, project_name=project_name)
        self.auxiliary_data = {
            'df_messages': pd.DataFrame(),
            'df_valid_emp': None,
            'df_params_lq': None,
            'df_feriados': None,
            'df_closed_days': None,
            'df_params': None,
            'df_contratos': pd.DataFrame(),
            'df_process_rules': pd.DataFrame(),
            'df_pro_emp_mov': pd.DataFrame(),
            'df_annual_variables': pd.DataFrame(),
        }
        self.algorithm_treatment_params = {'admissao_proporcional': None}
        self.raw_data: Dict[str, Any] = {
            'df_calendario': None,
            'df_colaborador': None,
            'df_estimativas': None,
        }
        self.medium_data: Dict[str, Any] = {
            'df_calendario': None,
            'df_colaborador': None,
            'df_estimativas': None,
        }
        self.rare_data: Dict[str, Any] = {
            'df_results': None,
            'stage1_schedule': None,
            'stage2_schedule': None,
        }
        self.formatted_data: Dict[str, Any] = {
            'df_final': None,
            'stage1_schedule': None,
            'stage2_schedule': None,
        }
        if external_data:
            self.external_call_data = external_data
            self.logger.info(f"Using runtime external_data: current_process_id={external_data.get('current_process_id')}")
        else:
            self.external_call_data = self.config_manager.parameters.external_call_data if self.config_manager else {}
            self.logger.info(f"Using JSON defaults: current_process_id={self.external_call_data.get('current_process_id')}")
        self.logger.info("AlcampoDataModel initialized")

    def _alcampo_raw_query(self, entity: str) -> str:
        paths = getattr(self.config_manager.paths, 'alcampo_sql_raw_paths', {}) or {}
        return paths.get(entity, '')

    def _alcampo_aux_query(self, entity: str) -> str:
        paths = getattr(self.config_manager.paths, 'alcampo_sql_auxiliary_paths', {}) or {}
        return paths.get(entity, '')

    def load_process_data(self, data_manager: BaseDataManager, entities_dict: Dict[str, str]) -> Tuple[bool, str, str]:
        df_messages = pd.DataFrame()
        try:
            from src.orquestrador_functions.Logs.message_loader import (
                load_df_messages,
                set_runtime_message_lang,
                apply_unit_message_lang_from_estrutura,
            )
            set_runtime_message_lang(None)
            df_messages = load_df_messages(self.config_manager.system.project_root_dir)
        except Exception as e:
            self.logger.error(f"Error loading df_messages: {e}")
        self.auxiliary_data['df_messages'] = df_messages.copy()

        try:
            self.logger.info("Loading Alcampo process data")
            if not entities_dict:
                return False, "errSubproc", "No entities passed as argument"

            try:
                if isinstance(data_manager, CSVDataManager):
                    df_valid_emp = load_valid_emp_csv()
                elif isinstance(data_manager, DBDataManager):
                    df_valid_emp = data_manager.load_data(
                        'valid_emp',
                        query_file=self.config_manager.paths.sql_processing_paths['valid_emp'],
                        process_id="'" + str(self.external_call_data['current_process_id']) + "'",
                    )
                else:
                    return False, "errSubproc", f"Unsupported data_manager: {type(data_manager)}"
                success, df_valid_emp, error_msg = treat_df_valid_emp(df_valid_emp)
                if not success:
                    return False, "errNoColab", error_msg
                if not validate_df_valid_emp(df_valid_emp):
                    return False, "errNoColab", "df_valid_emp is invalid"
            except Exception as e:
                self.logger.error(f"Error loading valid_emp: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            unit_id, secao_id, posto_id_list, employees_id_by_posto_dict, employees_id_total_list = get_valid_emp_info(df_valid_emp)
            if not validate_valid_emp_info(unit_id, secao_id, posto_id_list, employees_id_total_list):
                return False, "errSubproc", "Invalid valid_emp info"

            try:
                df_estrutura_wfm = data_manager.load_data(
                    entity='df_estrutura_wfm',
                    query_file=self.config_manager.paths.sql_auxiliary_paths['df_estrutura_wfm'],
                    secao_id=secao_id,
                )
            except Exception as e:
                return False, "errSubproc", str(e)
            if not validate_df_estrutura_wfm(df_estrutura_wfm):
                return False, "errSubproc", "df_estrutura_wfm is invalid"

            nome_pais = get_df_estrutura_wfm_info(df_estrutura_wfm)
            message_lang = apply_unit_message_lang_from_estrutura(df_estrutura_wfm)
            eci_flag = is_eci_unit(df_estrutura_wfm)
            df_eci_section_results = pd.DataFrame()
            eci_sibling_results_flag = False
            if eci_flag:
                nome_secao = str(df_estrutura_wfm['nome_secao'].iloc[0])
                sibling_section_name = get_sibling_section_name(nome_secao)
                if sibling_section_name:
                    try:
                        df_sibling_section = data_manager.load_data(
                            entity='df_eci_sibling_section',
                            query_file=self.config_manager.paths.sql_auxiliary_paths['df_eci_sibling_section'],
                            unit_id="'" + str(unit_id) + "'",
                            sibling_section_name="'" + sibling_section_name + "'",
                        )
                        if not df_sibling_section.empty:
                            sibling_secao_id = df_sibling_section['fk_secao'].iloc[0]
                            df_eci_employees = data_manager.load_data(
                                entity='df_eci_section_employees',
                                query_file=self.config_manager.paths.sql_auxiliary_paths['df_eci_section_employees'],
                                secao_id="'" + str(sibling_secao_id) + "'",
                            )
                            eci_sibling_results_flag = not df_eci_employees.empty
                            if not df_eci_employees.empty:
                                start_date_temp = self.external_call_data.get('start_date', '')
                                end_date_temp = self.external_call_data.get('end_date', '')
                                df_eci_section_results = data_manager.load_data(
                                    entity='df_eci_section_results',
                                    query_file=self.config_manager.paths.sql_auxiliary_paths['df_calendario_passado'],
                                    start_date=start_date_temp,
                                    end_date=end_date_temp,
                                    colabs=create_employee_query_string(df_eci_employees['employee_id'].tolist()),
                                )
                    except Exception as e:
                        self.logger.warning(f"Error loading ECI sibling section data: {e}")
                        df_eci_section_results = pd.DataFrame()

            start_date = self.external_call_data.get('start_date', '')
            end_date = self.external_call_data.get('end_date', '')
            wfm_proc_colab = self.external_call_data.get('wfm_proc_colab', None)
            first_year_date, last_year_date, main_year = count_dates_per_year(start_date_str=start_date, end_date_str=end_date)
            first_day_passado, last_day_passado, case_type = get_first_and_last_day_passado_arguments(
                start_date_str=start_date,
                end_date_str=end_date,
                main_year=main_year,
                wfm_proc_colab=wfm_proc_colab,
            )

            try:
                df_mpd_valid_employees = data_manager.load_data(
                    'df_mpd_valid_employees',
                    query_file=self.config_manager.paths.sql_processing_paths['df_mpd_valid_employees'],
                    process_id="'" + str(self.external_call_data['current_process_id']) + "'",
                )
            except Exception as e:
                return False, "errSubproc", str(e)

            success, section_employees_id_list, error_msg = get_section_employees_id_list(df_mpd_valid_employees)
            if not success:
                return False, "errSubproc", error_msg

            num_sundays_year = count_sundays_in_period(
                first_day_year_str=first_year_date,
                last_day_year_str=last_year_date,
                start_date_str=start_date,
                end_date_str=end_date,
            )
            if not validate_num_sundays_year(num_sundays_year):
                return False, "errSubproc", f"num_sundays_year is invalid: {num_sundays_year}"

            try:
                df_fk_colaborador_matricula = data_manager.load_data(
                    'df_fk_colaborador_matricula',
                    query_file=self.config_manager.paths.sql_processing_paths['df_fk_colaborador_matricula'],
                    colabs_id=create_employee_query_string(section_employees_id_list),
                )
                success, employee_id_matriculas_map, error_msg = get_employee_id_matriculas_map_dict(df_fk_colaborador_matricula)
                if not success:
                    return False, "errSubproc", error_msg
            except Exception as e:
                return False, "errSubproc", str(e)

            if not validate_employees_id_list(employees_id_total_list):
                return False, "errSubproc", "employees_id_list is empty"
            if not validate_posto_id_list(posto_id_list):
                return False, "errSubproc", "posto_id_list is empty"
            if not validate_date_passado(first_day_passado) or not validate_date_passado(last_day_passado):
                return False, "errSubproc", "passado dates are empty"

            try:
                if isinstance(data_manager, CSVDataManager):
                    df_params_lq = data_manager.load_data('params_lq')
                else:
                    df_params_lq = data_manager.load_data(
                        'params_lq',
                        query_file=self.config_manager.paths.sql_processing_paths['params_lq'],
                    )
            except Exception as e:
                return False, "errSubproc", str(e)

            try:
                df_feriados = data_manager.load_data(
                    'df_feriados',
                    query_file=self.config_manager.paths.sql_processing_paths['df_feriados'],
                    unit_id="'" + str(unit_id) + "'",
                    start_date=first_day_passado,
                    end_date=last_day_passado,
                )
                if df_feriados is None or df_feriados.empty:
                    return False, "ERR_LOAD_FERIADOS_EMPTY", str(unit_id)
            except Exception as e:
                return False, "errSubproc", str(e)

            success, df_feriados, error_msg = treat_df_feriados(df_feriados)
            if not success:
                return False, "errSubproc", error_msg
            success, df_feriados, error_msg = add_date_related_columns(
                df=df_feriados,
                date_col='schedule_day',
                add_id_col=False,
                use_case=1,
                main_year=main_year,
                first_date=first_day_passado,
                last_date=last_day_passado,
            )
            if not success:
                return False, "errSubproc", error_msg
            if not validate_df_feriados(df_feriados):
                return False, "errSubproc", "df_feriados is invalid"

            num_feriados_abertos, num_feriados_fechados = count_holidays_in_period(
                start_date_str=first_year_date,
                end_date_str=last_year_date,
                df_feriados=df_feriados,
                use_case=0,
            )

            try:
                df_closed_days = data_manager.load_data(
                    'df_closed_days',
                    query_file=self.config_manager.paths.sql_processing_paths['df_closed_days'],
                    unit_id="'" + str(unit_id) + "'",
                    start_date=first_year_date,
                    end_date=last_year_date,
                )
                self.logger.info(
                    f"df_closed_days shape (rows {df_closed_days.shape[0]}, columns {df_closed_days.shape[1]}): "
                    f"{df_closed_days.columns.tolist()} for {first_year_date} to {last_year_date}"
                )
            except Exception as e:
                return False, "errSubproc", str(e)
            if (
                df_closed_days is not None
                and not df_closed_days.empty
                and 'data' not in df_closed_days.columns
                and 'schedule_day' in df_closed_days.columns
            ):
                df_closed_days = df_closed_days.rename(columns={'schedule_day': 'data'})
            success, df_closed_days, error_msg = treat_df_closed_days(df_closed_days, first_year_date, last_year_date)
            if not success:
                return False, "errSubproc", error_msg
            if df_closed_days.empty:
                self.logger.info("No closed days in the process year - proceeding with an empty frame")

            df_faixa_secao = pd.DataFrame()
            try:
                query_path = self.config_manager.paths.sql_auxiliary_paths.get('df_faixa_horario', '')
                if query_path:
                    df_faixa_secao_wide = data_manager.load_data(
                        'df_faixa_secao',
                        query_file=query_path,
                        secao_id=secao_id,
                        start_date="'" + first_day_passado + "'",
                        end_date="'" + last_day_passado + "'",
                    )
                    success, df_faixa_secao, error_msg = treat_df_faixa_secao_to_long(df_faixa_secao_wide)
                    if not success:
                        df_faixa_secao = pd.DataFrame()
            except Exception as e:
                self.logger.warning(f"Error loading df_faixa_secao: {e}")
                df_faixa_secao = pd.DataFrame()

            try:
                df_params = data_manager.load_data(
                    'df_params',
                    query_file=self.config_manager.paths.sql_processing_paths['params_df'],
                    unit_id="'" + str(unit_id) + "'",
                )
            except Exception as e:
                return False, "errSubproc", str(e)

            try:
                parameters_cfg = data_manager.load_data(
                    'parameters_cfg',
                    query_file=self.config_manager.paths.sql_processing_paths['parameters_cfg'],
                )
                if parameters_cfg.empty:
                    return False, "errSubproc", "parameters_cfg is empty"
                parameters_cfg = str(parameters_cfg["WFM.S_PCK_CORE_PARAMETER.GETCHARATTR('ADMISSAO_PROPORCIONAL')"].iloc[0]).lower()
            except Exception as e:
                return False, "errSubproc", str(e)
            if not validate_parameters_cfg(parameters_cfg):
                return False, "errSubproc", "admissao_proporcional is not a valid value"

            self.auxiliary_data.update({
                'df_messages': df_messages.copy(),
                'message_lang': message_lang,
                'df_valid_emp': df_valid_emp.copy(),
                'df_estrutura_wfm': df_estrutura_wfm.copy(),
                'df_params_lq': df_params_lq.copy(),
                'df_params': df_params.copy(),
                'df_feriados': df_feriados.copy(),
                'df_closed_days': df_closed_days.copy(),
                'df_faixa_secao': df_faixa_secao.copy(),
                'df_mpd_valid_employees': df_mpd_valid_employees.copy(),
                'df_fk_colaborador_matricula': df_fk_colaborador_matricula.copy(),
                'unit_id': unit_id,
                'secao_id': secao_id,
                'posto_id_list': posto_id_list,
                'main_year': main_year,
                'first_year_date': first_year_date,
                'last_year_date': last_year_date,
                'first_date_passado': first_day_passado,
                'last_date_passado': last_day_passado,
                'employees_id_total_list': employees_id_total_list,
                'section_employees_id_list': section_employees_id_list,
                'employees_id_by_posto_dict': employees_id_by_posto_dict,
                'employee_id_matriculas_map': employee_id_matriculas_map.copy(),
                'case_type': case_type,
                'is_eci_unit': eci_flag,
                'df_eci_section_results': df_eci_section_results.copy(),
                'num_sundays_year': num_sundays_year,
                'num_feriados_abertos': num_feriados_abertos,
                'num_feriados_fechados': num_feriados_fechados,
                'df_process_rules': pd.DataFrame(),
                'df_pro_emp_mov': pd.DataFrame(),
            })
            self.algorithm_treatment_params['admissao_proporcional'] = parameters_cfg
            self.algorithm_treatment_params['wfm_proc_colab'] = wfm_proc_colab
            self.algorithm_treatment_params['df_feriados'] = df_feriados.copy()
            self.algorithm_treatment_params['nome_pais'] = nome_pais
            self.algorithm_treatment_params['eci_flag'] = eci_flag
            self.algorithm_treatment_params['eci_sibling_results_flag'] = eci_sibling_results_flag
            self.algorithm_treatment_params['df_process_rules'] = pd.DataFrame()
            self.algorithm_treatment_params['df_pro_emp_mov'] = pd.DataFrame()
            return True, "validSubProc", ''
        except Exception as e:
            self.logger.error(f"Error loading process data: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_process_data(self) -> bool:
        self.logger.info("Alcampo validate_process_data")
        return True

    def treat_params(self) -> Tuple[bool, str, str]:
        try:
            df_params = self.auxiliary_data['df_params'].copy()
            algorithm_treatment_params = self.algorithm_treatment_params
            params_names_list = self.config_manager.parameters.get_parameter_names()
            params_defaults = self.config_manager.parameters.get_parameter_defaults()
            retrieved_params = get_param_for_posto(
                df=df_params,
                posto_id=self.auxiliary_data['current_posto_id'],
                unit_id=self.auxiliary_data['unit_id'],
                secao_id=self.auxiliary_data['secao_id'],
                params_names_list=params_names_list,
            ) or {}
            algorithm_name = ''
            for param_name in params_names_list:
                param_value = retrieved_params.get(param_name, params_defaults.get(param_name))
                self.auxiliary_data[param_name] = param_value
                if param_name == 'GD_algorithmName':
                    algorithm_name = param_value
                if param_name == 'NUM_DIAS_CONS':
                    algorithm_treatment_params['NUM_DIAS_CONS'] = int(param_value)
                if param_name == 'ld_sunday_param':
                    algorithm_treatment_params['ld_sunday_param'] = float(param_value)
                if param_name == 'ld_holiday_param':
                    algorithm_treatment_params['ld_holiday_param'] = float(param_value)
            algorithm_treatment_params['start_date'] = self.external_call_data['start_date']
            algorithm_treatment_params['end_date'] = self.external_call_data['end_date']
            self.auxiliary_data['algorithm_name'] = algorithm_name
            self.algorithm_treatment_params = algorithm_treatment_params
            return True, "", ""
        except Exception as e:
            self.logger.error(f"Error treating parameters: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_params(self):
        return True

    def _treat_cav_colaborador(self, df_colaborador: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
        if df_colaborador is None or df_colaborador.empty:
            return False, pd.DataFrame(), "CAV df_colaborador is empty"
        df = df_colaborador.copy()
        df.columns = [str(c).lower() for c in df.columns]
        if 'convenio' in df.columns and 'labor_union' not in df.columns:
            df = df.rename(columns={'convenio': 'labor_union'})
        if 'min_dias_trabalhados' in df.columns and 'min_dia_trab' not in df.columns:
            df = df.rename(columns={'min_dias_trabalhados': 'min_dia_trab'})
        if 'max_dias_trabalhados' in df.columns and 'max_dia_trab' not in df.columns:
            df = df.rename(columns={'max_dias_trabalhados': 'max_dia_trab'})
        df['employee_id'] = df['employee_id'].astype(str)
        df = df.drop_duplicates(subset=['employee_id'], keep='first')
        seq = df['seq_turno']
        seq_null = seq.isna() | (seq.astype(str).str.strip() == '') | (seq.astype(str) == '0')
        if seq_null.any():
            return False, pd.DataFrame(), "seq_turno=0 or null - columna SEQ_TURNO mal parametrizada"
        for col in ('min_dia_trab', 'max_dia_trab', 'dyf_max_t', 'lq', 'c2d', 'c3d', 'cxx', 'dofhc', 'l_total', 'out'):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        if 'labor_union' in df.columns:
            df['labor_union'] = df['labor_union'].astype(str).str.upper()
        df['data_admissao'] = pd.to_datetime(df.get('data_admissao'), errors='coerce')
        df['data_demissao'] = pd.to_datetime(df.get('data_demissao'), errors='coerce')
        if 'matricula' in df.columns:
            df['matricula'] = df['matricula'].astype(str)
        if 'total_dom_fes' not in df.columns:
            df['total_dom_fes'] = 0
        for col in ('vz', 'l_res', 'l_res2'):
            if col not in df.columns:
                df[col] = 0
        if 'semana_1' in df.columns and 'semana1' not in df.columns:
            df['semana1'] = df['semana_1']
        start_date = pd.to_datetime(self.external_call_data.get('start_date'))
        end_date = pd.to_datetime(self.external_call_data.get('end_date'))
        if 'contract_id' not in df.columns:
            df['contract_id'] = 'cav'
        if 'begin_date' not in df.columns:
            df['begin_date'] = start_date
        if 'end_date' not in df.columns:
            df['end_date'] = end_date
        return True, df, ""

    def _overlay_params_lq(self, df_colaborador: pd.DataFrame, df_params_lq: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
        """Keep CAV lq for CICLO / MoT / P. Other sequences take params_lq when present."""
        df = df_colaborador.copy()
        if 'lq' not in df.columns:
            df['lq'] = 0
        df['lq'] = pd.to_numeric(df['lq'], errors='coerce').fillna(0)
        df['_cav_lq'] = df['lq']
        if df_params_lq is None or df_params_lq.empty:
            df = df.drop(columns=['_cav_lq'])
            return True, df, ""
        params = df_params_lq.copy()
        params.columns = [str(c).lower() for c in params.columns]
        if 'sys_p_name' not in params.columns or 'numbervalue' not in params.columns:
            df = df.drop(columns=['_cav_lq'])
            return True, df, ""
        mapping = pd.DataFrame({
            'seq_key': ['M', 'T', 'MT', 'MMT', 'MTT', 'CICLO'],
            'sys_p_name': ['SQ_TURNO_M', 'SQ_TURNO_T', 'SQ_TURNO_MT', 'SQ_TURNO_MMT', 'SQ_TURNO_MTT', 'CICLO'],
        })
        params = params.merge(mapping, on='sys_p_name', how='inner')
        params = params[['seq_key', 'numbervalue']].drop_duplicates(subset=['seq_key'])
        params = params.rename(columns={'numbervalue': 'lq_param'})
        params['seq_key'] = params['seq_key'].astype(str).str.upper()
        df['_seq_key'] = df['seq_turno'].astype(str).str.upper()
        df = df.merge(params, left_on='_seq_key', right_on='seq_key', how='left')
        keep_cav = df['_seq_key'].isin(_LQ_KEEP_CAV)
        has_param = df['lq_param'].notna()
        df.loc[~keep_cav & has_param, 'lq'] = pd.to_numeric(df.loc[~keep_cav & has_param, 'lq_param'], errors='coerce')
        df.loc[keep_cav, 'lq'] = df.loc[keep_cav, '_cav_lq']
        df = df.drop(columns=['_seq_key', 'seq_key', 'lq_param', '_cav_lq'], errors='ignore')
        return True, df, ""

    def load_colaborador_info(self, data_manager: BaseDataManager, posto_id: int = 0) -> Tuple[bool, str, str]:
        try:
            employees_id_list_for_posto = self.auxiliary_data['employees_id_by_posto_dict'].get(posto_id, [])
            case_type = self.auxiliary_data['case_type']
            first_date_passado = self.auxiliary_data['first_date_passado']
            last_date_passado = self.auxiliary_data['last_date_passado']
            process_id = self.external_call_data['current_process_id']
            wfm_proc_colab = self.external_call_data['wfm_proc_colab']
            df_mpd_valid_employees = self.auxiliary_data['df_mpd_valid_employees']

            success, past_employees_id_list, error_msg = get_past_employees_id_list(
                wfm_proc_colab=wfm_proc_colab,
                df_mpd_valid_employees=df_mpd_valid_employees,
                fk_tipo_posto=posto_id,
                employees_id_list_for_posto=employees_id_list_for_posto,
            )
            if not success:
                return False, "errSubproc", error_msg

            cav_query = self._alcampo_raw_query('df_colaborador')
            if not cav_query:
                return False, "errSubproc", "Alcampo qry_ma.sql path is missing"
            try:
                df_colaborador = data_manager.load_data(
                    entity='df_colaborador',
                    query_file=cav_query,
                    colabs_id=create_employee_query_string(employee_id_list=past_employees_id_list),
                    start_date="'" + str(first_date_passado) + "'",
                    end_date="'" + str(last_date_passado) + "'",
                    process_id=process_id,
                )
            except Exception as e:
                self.logger.error(f"Error loading CAV df_colaborador: {e}", exc_info=True)
                return False, "errSubproc", str(e)

            success, df_colaborador, error_msg = self._treat_cav_colaborador(df_colaborador)
            if not success:
                return False, "errSubproc", error_msg

            try:
                df_valid_emp = self.auxiliary_data['df_valid_emp']
                if 'fk_tipo_posto' not in df_colaborador.columns and 'fk_tipo_posto' in df_valid_emp.columns:
                    posto_map = df_valid_emp[['employee_id', 'fk_tipo_posto']].drop_duplicates(subset=['employee_id']).copy()
                    posto_map['employee_id'] = posto_map['employee_id'].astype(str)
                    df_colaborador = df_colaborador.merge(posto_map, on='employee_id', how='left')
            except Exception as e:
                self.logger.warning(f"Could not merge fk_tipo_posto: {e}")

            contract_query = self._alcampo_aux_query('df_contratos')
            if not contract_query:
                return False, "errSubproc", "Alcampo contract query path is missing"
            try:
                df_contratos = data_manager.load_data(
                    entity='df_contratos',
                    query_file=contract_query,
                    colabs_id=create_employee_query_string(employee_id_list=past_employees_id_list),
                    start_date="'" + str(first_date_passado) + "'",
                    end_date="'" + str(last_date_passado) + "'",
                    process_id=process_id,
                )
            except Exception as e:
                self.logger.error(f"Error loading df_contratos: {e}", exc_info=True)
                return False, "errSubproc", str(e)
            if df_contratos is None or df_contratos.empty:
                return False, "errSubproc", "df_contratos is empty: calendar has no hours contribution"

            df_core_pro_work_shift = pd.DataFrame()
            try:
                df_core_pro_work_shift = data_manager.load_data(
                    entity='df_core_pro_work_shift',
                    query_file=self.config_manager.paths.sql_auxiliary_paths.get('df_core_pro_work_shift', ''),
                    process_id=process_id,
                )
            except Exception as e:
                self.logger.warning(f"Error loading df_core_pro_work_shift: {e}")

            df_disponibilidade = pd.DataFrame()
            try:
                df_disponibilidade = data_manager.load_data(
                    entity='df_disponibilidade',
                    query_file=self.config_manager.paths.sql_auxiliary_paths.get('df_disponibilidade'),
                    process_id="'" + str(process_id) + "'",
                    start_date=first_date_passado,
                    end_date=last_date_passado,
                    colabs_id=create_employee_query_string(past_employees_id_list),
                )
                if not df_disponibilidade.empty:
                    success, df_disponibilidade, error_msg = treat_df_disponibilidade(
                        df_disponibilidade=df_disponibilidade,
                        df_core_pro_work_shift=df_core_pro_work_shift,
                        section_id=posto_id,
                    )
                    if not success:
                        df_disponibilidade = pd.DataFrame()
            except Exception as e:
                self.logger.warning(f"Error loading df_disponibilidade: {e}")
                df_disponibilidade = pd.DataFrame()

            if not validate_past_employee_id_list(past_employees_id_list, case_type):
                return False, "errSubproc", "past_employees_id_list is empty"

            self.raw_data['df_colaborador'] = df_colaborador.copy()
            self.auxiliary_data['df_contratos'] = df_contratos.copy()
            self.auxiliary_data['df_disponibilidade'] = df_disponibilidade.copy() if not df_disponibilidade.empty else pd.DataFrame()
            self.auxiliary_data['employees_id_list_for_posto'] = employees_id_list_for_posto
            self.auxiliary_data['employees_id_90_list'] = []
            self.auxiliary_data['past_employees_id_list'] = past_employees_id_list
            employees_id_by_posto_dict = dict(self.auxiliary_data.get('employees_id_by_posto_dict', {}))
            employees_id_by_posto_dict[posto_id] = employees_id_list_for_posto
            self.auxiliary_data['employees_id_by_posto_dict'] = employees_id_by_posto_dict
            self.algorithm_treatment_params['employees_id_list_for_posto'] = employees_id_list_for_posto
            self.algorithm_treatment_params['employees_id_90_list'] = []
            self.algorithm_treatment_params['df_process_rules'] = pd.DataFrame()
            self.algorithm_treatment_params['df_pro_emp_mov'] = pd.DataFrame()
            return True, "", ""
        except Exception as e:
            self.logger.error(f"Error in load_colaborador_info: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_colaborador_info(self) -> bool:
        df_colaborador = self.raw_data.get('df_colaborador')
        if df_colaborador is None or df_colaborador.empty:
            self.logger.error("df_colaborador is empty")
            return False
        return True

    def load_calendario_info(self, data_manager: BaseDataManager, process_id: int = 0, posto_id: int = 0) -> Tuple[bool, str, str]:
        try:
            if posto_id == 0 or posto_id is None:
                return False, "errSubproc", "posto_id is 0 or None"
            employees_id_list_for_posto = self.auxiliary_data['employees_id_list_for_posto']
            past_employees_id_list = self.auxiliary_data['past_employees_id_list']
            employee_id_matriculas_map = self.auxiliary_data['employee_id_matriculas_map']
            first_date_passado = self.auxiliary_data['first_date_passado']
            last_date_passado = self.auxiliary_data['last_date_passado']
            case_type = self.auxiliary_data['case_type']
            wfm_proc_colab = self.external_call_data['wfm_proc_colab']
            start_date_str = self.external_call_data['start_date']
            end_date_str = self.external_call_data['end_date']

            df_calendario_passado = data_manager.load_data(
                'df_calendario_passado',
                query_file=self.config_manager.paths.sql_auxiliary_paths['df_calendario_passado'],
                start_date=first_date_passado,
                end_date=last_date_passado,
                colabs=create_employee_query_string(past_employees_id_list),
            )
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
                return False, "errSubproc", error_msg

            employees_id_for_posto_int = [int(emp_id) for emp_id in employees_id_list_for_posto]
            success, matriculas_for_posto, error_msg = get_matriculas_for_employee_id(
                employee_id_list=employees_id_for_posto_int,
                employee_id_matriculas_map=employee_id_matriculas_map,
            )
            if not success:
                return False, "errSubproc", error_msg

            ausencias_query = self._alcampo_aux_query('df_ausencias_ferias') or self.config_manager.paths.sql_auxiliary_paths['df_ausencias_ferias']
            df_ausencias_ferias = data_manager.load_data(
                'df_ausencias_ferias',
                query_file=ausencias_query,
                colabs_id=create_employee_query_string(matriculas_for_posto),
                start_date=first_date_passado,
                end_date=last_date_passado,
            )
            success, df_ausencias_ferias, error_msg = treat_df_ausencias_ferias(
                df_ausencias_ferias=df_ausencias_ferias,
                start_date=start_date_str,
                end_date=end_date_str,
                classification_mode='legacy_motivo_list',
            )
            if not success:
                return False, "errSubproc", error_msg
            if not df_ausencias_ferias.empty and not validate_df_ausencias_ferias(df_ausencias_ferias):
                return False, "errSubproc", "df_ausencias_ferias validation failed"

            try:
                df_days_off = data_manager.load_data(
                    'df_days_off',
                    query_file=self.config_manager.paths.sql_auxiliary_paths['df_days_off'],
                    colabs_id=create_employee_query_string(matriculas_for_posto),
                )
                if df_days_off.empty:
                    df_days_off = pd.DataFrame(columns=pd.Index(['employee_id', 'schedule_dt', 'sched_type']))
            except Exception as e:
                self.logger.error(f"Error loading df_days_off: {e}", exc_info=True)
                df_days_off = pd.DataFrame()

            self.auxiliary_data['df_calendario_passado'] = df_calendario_passado.copy() if not df_calendario_passado.empty else pd.DataFrame()
            self.auxiliary_data['df_ausencias_ferias'] = df_ausencias_ferias.copy()
            self.auxiliary_data['df_days_off'] = df_days_off.copy()
            self.auxiliary_data['df_ciclos'] = pd.DataFrame()
            return True, "", ""
        except Exception as e:
            self.logger.error(f"Error in load_calendario_info: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_calendario_info(self) -> tuple[bool, list[str]]:
        return True, []

    def load_calendario_transformations(self) -> Tuple[bool, str, str]:
        try:
            past_employees_id_list = self.auxiliary_data['past_employees_id_list']
            employee_id_matriculas_map = self.auxiliary_data['employee_id_matriculas_map']
            start_date = self.external_call_data['start_date']
            end_date = self.external_call_data['end_date']
            first_date_passado = self.auxiliary_data['first_date_passado']
            last_date_passado = self.auxiliary_data['last_date_passado']
            main_year = self.auxiliary_data['main_year']
            df_calendario_passado = self.auxiliary_data['df_calendario_passado'].copy()
            df_ausencias_ferias = self.auxiliary_data['df_ausencias_ferias'].copy()
            df_days_off = self.auxiliary_data['df_days_off'].copy()
            df_feriados = self.auxiliary_data['df_feriados'].copy()
            df_colaborador = self.raw_data['df_colaborador'].copy()
            if 'semana1' not in df_colaborador.columns:
                df_colaborador['semana1'] = df_colaborador['semana_1'] if 'semana_1' in df_colaborador.columns else 'M'

            success, df_calendario, error_msg = create_df_calendario(
                start_date=start_date,
                end_date=end_date,
                main_year=main_year,
                employee_id_matriculas_map=employee_id_matriculas_map,
                past_employees_id_list=past_employees_id_list,
                df_feriados=df_feriados,
            )
            if not success:
                return False, "errSubproc", error_msg
            success, df_calendario, error_msg = add_date_related_columns(
                df=df_calendario,
                date_col='schedule_day',
                add_id_col=True,
                use_case=0,
                main_year=main_year,
                first_date=first_date_passado,
                last_date=last_date_passado,
            )
            if not success:
                return False, "errSubproc", error_msg

            success, df_calendario, error_msg = add_seq_turno(df_calendario, df_colaborador)
            if not success:
                return False, "errSubproc", error_msg

            success, df_calendario, error_msg = add_ausencias_ferias(df_calendario, df_ausencias_ferias)
            if not success:
                return False, "errSubproc", error_msg
            success, df_calendario, error_msg = add_days_off(df_calendario, df_days_off)
            if not success:
                return False, "errSubproc", error_msg
            success, df_calendario, error_msg = add_calendario_passado(df_calendario, df_calendario_passado)
            if not success:
                return False, "errSubproc", error_msg
            success, df_calendario, error_msg = filter_df_dates(
                df=df_calendario,
                first_date_str=first_date_passado,
                last_date_str=last_date_passado,
                date_col_name='schedule_day',
                use_case=1,
            )
            if not success:
                return False, "errSubproc", error_msg
            success, tipos_turno_list, error_msg = extract_tipos_turno(df_calendario=df_calendario, tipo_turno_col='tipo_turno')
            if not success:
                return False, "errSubproc", error_msg
            self.auxiliary_data['tipos_de_turno'] = tipos_turno_list
            success, df_calendario, error_msg = define_dia_tipo(
                df=df_calendario,
                df_feriados=df_feriados,
                date_col='schedule_day',
                tipo_turno_col='tipo_turno',
                horario_col='horario',
                wd_col='wd',
            )
            if not success:
                return False, "errSubproc", error_msg

            df_disponibilidade = self.auxiliary_data.get('df_disponibilidade', pd.DataFrame())
            success, df_calendario_restricted, msg = restrict_turnos_by_disponibilidade(
                df_calendario=df_calendario,
                df_disponibilidade=df_disponibilidade,
            )
            if success:
                df_calendario = df_calendario_restricted
            success, df_calendario, error_msg = fill_calendario_passado_defaults(
                df_calendario=df_calendario,
                first_date_passado=first_date_passado,
                last_date_passado=last_date_passado,
            )
            if not success:
                return False, "errSubproc", error_msg
            self.raw_data['df_calendario'] = df_calendario.copy()
            return True, "", ""
        except Exception as e:
            self.logger.error(f"Error in load_calendario_transformations: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_matrices_loading(self) -> bool:
        return True

    def load_colaborador_transformations(self) -> Tuple[bool, str, str]:
        try:
            labor_union_bd = self.auxiliary_data['GD_convenioBD']
            num_sundays_year = self.auxiliary_data['num_sundays_year']
            num_feriados_abertos = self.auxiliary_data['num_feriados_abertos']
            num_feriados_fechados = self.auxiliary_data['num_feriados_fechados']
            num_fer_dom = num_sundays_year + num_feriados_abertos + num_feriados_fechados
            df_params_lq = self.auxiliary_data['df_params_lq']
            df_valid_emp = self.auxiliary_data['df_valid_emp']
            df_feriados = self.auxiliary_data['df_feriados']
            df_colaborador = self.raw_data['df_colaborador'].copy()
            start_date_str = self.external_call_data['start_date']
            end_date_str = self.external_call_data['end_date']
            main_year = self.auxiliary_data['main_year']

            df_contratos = self.auxiliary_data.get('df_contratos', pd.DataFrame())
            if df_contratos is None or df_contratos.empty:
                return False, "errSubproc", "df_contratos is empty: calendar has no hours contribution"
            success, df_contratos, error_msg = treat_df_contratos(df_contratos=df_contratos)
            if not success:
                return False, "errSubproc", error_msg
            self.auxiliary_data['df_contratos'] = df_contratos.copy()

            success, df_colaborador, error_msg = self._overlay_params_lq(df_colaborador, df_params_lq)
            if not success:
                return False, "errSubproc", error_msg
            success, df_colaborador, error_msg = set_tipo_contrato_to_df_colaborador(df_colaborador, use_case=1)
            if not success:
                return False, "errSubproc", error_msg
            if df_colaborador['tipo_contrato'].isna().any():
                return False, "errSubproc", "tipo_contrato is null after min/max lookup"
            success, df_colaborador, error_msg = add_prioridade_folgas_to_df_colaborador(df_colaborador, df_valid_emp, use_case=1)
            if not success:
                return False, "errSubproc", error_msg
            success, df_colaborador, error_msg = set_c2d_to_df_colaborador(df_colaborador, use_case=1)
            if not success:
                return False, "errSubproc", error_msg
            success, df_colaborador, error_msg = set_c3d_to_df_colaborador(df_colaborador, labor_union_bd, use_case=2)
            if not success:
                return False, "errSubproc", error_msg
            success, df_colaborador, error_msg = add_l_d_to_df_colaborador(df_colaborador, labor_union_bd, use_case=1)
            if not success:
                return False, "errSubproc", error_msg
            success, df_colaborador, error_msg = add_l_dom_to_df_colaborador(
                df_colaborador=df_colaborador,
                df_feriados=df_feriados,
                labor_union_bd=labor_union_bd,
                start_date_str=start_date_str,
                end_date_str=end_date_str,
                num_sundays=num_sundays_year,
                num_feriados=num_feriados_abertos + num_feriados_fechados,
                num_feriados_fechados=num_feriados_fechados,
                num_fer_dom=num_fer_dom,
                use_case=2,
            )
            if not success:
                return False, "errSubproc", error_msg
            if (pd.to_numeric(df_colaborador['l_dom'], errors='coerce') < 0).any():
                return False, "errSubproc", "l_dom is negative - DyF_MAX_T misconfigured"
            success, df_colaborador, error_msg = add_l_q_to_df_colaborador(df_colaborador, labor_union_bd, use_case=2)
            if not success:
                return False, "errSubproc", error_msg
            success, df_colaborador, error_msg = add_l_total_to_df_colaborador(
                df_colaborador=df_colaborador,
                df_feriados=df_feriados,
                labor_union_bd=labor_union_bd,
                num_sundays=num_sundays_year,
                num_fer_dom=num_fer_dom,
                use_case=1,
            )
            if not success:
                return False, "errSubproc", error_msg
            success, df_colaborador, error_msg = date_adjustments_to_df_colaborador(df_colaborador, main_year)
            if not success:
                return False, "errSubproc", error_msg
            success, df_colaborador, error_msg = adjust_counters_for_contract_types(df_colaborador, use_case=1)
            if not success:
                return False, "errSubproc", error_msg

            df_colaborador = df_colaborador.rename(columns={'ld': 'l_d', 'lq': 'l_q'})
            self.raw_data['df_colaborador'] = df_colaborador.copy()
            return True, "", ""
        except Exception as e:
            self.logger.error(f"Error in load_colaborador_transformations: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def _build_synthetic_annual_variables(self, df_colaborador: pd.DataFrame, df_calendario: pd.DataFrame) -> pd.DataFrame:
        start_date = pd.to_datetime(self.external_call_data.get('start_date'))
        end_date = pd.to_datetime(self.external_call_data.get('end_date'))
        cal = df_calendario[['employee_id', 'schedule_day']].copy()
        cal['employee_id'] = cal['employee_id'].astype(str)
        cal['schedule_day'] = pd.to_datetime(cal['schedule_day'], errors='coerce')
        bounds = cal.groupby('employee_id')['schedule_day'].agg(['min', 'max']).rename(columns={'min': 'cal_min', 'max': 'cal_max'})
        rows = []
        for _, emp in df_colaborador.iterrows():
            employee_id = str(emp['employee_id'])
            if employee_id not in bounds.index:
                continue
            cal_min = bounds.loc[employee_id, 'cal_min']
            cal_max = bounds.loc[employee_id, 'cal_max']
            begin_date = start_date if pd.notna(cal_min) and cal_min <= start_date <= cal_max else cal_min
            end = end_date if pd.notna(cal_max) and cal_min <= end_date <= cal_max else cal_max
            row = {
                'employee_id': employee_id,
                'begin_date': begin_date,
                'end_date': end,
                'l_dom': emp.get('l_dom', 0),
                'c2d': emp.get('c2d', 0),
                'c3d': emp.get('c3d', 0),
                'l_d': emp.get('l_d', 0),
                'l_q': emp.get('l_q', 0),
                'l_total': emp.get('l_total', 0),
                'cxx': emp.get('cxx', 0),
                'l_sab': 0,
                'l_dom_or_sab': 0,
                'tc': emp.get('dofhc', 0),
            }
            for col in _ANNUAL_APPLY_COLS:
                row[col] = True
            rows.append(row)
        return pd.DataFrame(rows)

    def func_inicializa(self) -> Tuple[bool, str, str]:
        try:
            df_calendario = self.raw_data['df_calendario'].copy()
            df_colaborador = self.raw_data['df_colaborador'].copy()
            df_estimativas = self.raw_data['df_estimativas'].copy()
            df_eci_section_results = self.auxiliary_data.get('df_eci_section_results', pd.DataFrame()).copy()
            main_year = self.auxiliary_data['main_year']
            start_date = self.external_call_data.get('start_date')
            end_date = self.external_call_data.get('end_date')

            valid, error_msg = validate_all_core_dataframes(
                df_calendario=df_calendario,
                df_estimativas=df_estimativas,
                df_colaborador=df_colaborador,
                start_date=start_date,
                end_date=end_date,
            )
            if not valid:
                return False, "errValidation", error_msg

            df_contratos = self.auxiliary_data.get('df_contratos', pd.DataFrame())
            success, df_calendario, error_msg = merge_contract_data(
                df_calendario=df_calendario,
                df_contratos=df_contratos,
                employee_col='employee_id',
                date_col='schedule_day',
            )
            if not success:
                return False, "errSubproc", error_msg

            success, df_calendario, error_msg = adjust_horario_for_admission_date(
                df_calendario=df_calendario,
                df_colaborador=df_colaborador,
                employee_col='employee_id',
                date_col='schedule_day',
                horario_col='horario',
                dia_tipo_col='dia_tipo',
            )
            if not success:
                return False, "errSubproc", error_msg

            first_date_passado = self.auxiliary_data.get('first_date_passado')
            last_date_passado = self.auxiliary_data.get('last_date_passado')
            if first_date_passado and last_date_passado:
                success, df_calendario, error_msg = fill_calendario_passado_defaults(
                    df_calendario=df_calendario,
                    first_date_passado=first_date_passado,
                    last_date_passado=last_date_passado,
                )
                if not success:
                    return False, "errSubproc", error_msg
                success, df_estimativas, error_msg = fill_estimativas_passado_grid(
                    df_estimativas=df_estimativas,
                    first_date_passado=first_date_passado,
                    last_date_passado=last_date_passado,
                )
                if not success:
                    return False, "errSubproc", error_msg

            param_pess_obj = self.external_call_data.get('param_pessoas_objetivo', 0.5)
            success, df_estimativas, error_msg = calculate_and_merge_allocated_employees(
                df_estimativas=df_estimativas,
                df_eci_section_results=df_eci_section_results,
                date_col_est='schedule_day',
                shift_col_est='turno',
                param_pess_obj=param_pess_obj,
            )
            if not success:
                return False, "errSubproc", error_msg

            success, df_colaborador, error_msg = convert_fields_to_int(
                df=df_colaborador,
                fields=['l_d', 'l_dom', 'l_q', 'l_total', 'c2d', 'c3d', 'cxx'],
            )
            if not success:
                return False, "errSubproc", error_msg

            df_annual = self._build_synthetic_annual_variables(df_colaborador, df_calendario)
            self.auxiliary_data['df_annual_variables'] = df_annual.copy()
            self.algorithm_treatment_params['df_annual_variables'] = df_annual.copy()
            self.algorithm_treatment_params['df_process_rules'] = pd.DataFrame()
            self.algorithm_treatment_params['df_pro_emp_mov'] = pd.DataFrame()

            self.medium_data['df_colaborador'] = df_colaborador.copy()
            self.medium_data['df_calendario'] = df_calendario.copy()
            self.medium_data['df_estimativas'] = df_estimativas.copy()

            try:
                output_dir = self.config_manager.paths.get_output_dir()
                process_id = self.external_call_data.get("current_process_id", "")
                posto_id = self.auxiliary_data.get("current_posto_id", "")
                df_colaborador.to_csv(os.path.join(output_dir, f'df_colaborador-{process_id}-{posto_id}.csv'), index=False, encoding='utf-8')
                df_calendario.to_csv(os.path.join(output_dir, f'df_calendario-{process_id}-{posto_id}.csv'), index=False, encoding='utf-8')
                df_estimativas.to_csv(os.path.join(output_dir, f'df_estimativas-{process_id}-{posto_id}.csv'), index=False, encoding='utf-8')
                if not df_annual.empty:
                    df_annual.to_csv(os.path.join(output_dir, f'df_annual_variables-{process_id}-{posto_id}.csv'), index=False, encoding='utf-8')
            except Exception as csv_error:
                self.logger.warning(f"Failed to save CSV debug files: {csv_error}")
            return True, "", ""
        except Exception as e:
            self.logger.error(f"Error in func_inicializa: {e}", exc_info=True)
            return False, "errSubproc", str(e)

    def validate_func_inicializa(self) -> bool:
        for key in ('df_colaborador', 'df_calendario', 'df_estimativas'):
            frame = self.medium_data.get(key)
            if frame is None or frame.empty:
                self.logger.error(f"medium_data[{key}] is empty")
                return False
        annual = self.algorithm_treatment_params.get('df_annual_variables')
        if annual is None or annual.empty:
            self.logger.error("synthetic df_annual_variables is empty")
            return False
        if 'carga_diaria' not in self.medium_data['df_calendario'].columns:
            self.logger.error("df_calendario is missing carga_diaria")
            return False
        return True
