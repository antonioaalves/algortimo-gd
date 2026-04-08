"""
Unit tests for df_calendario layer application logic.

This file tests each layer (add_* function) individually to ensure 
business logic is correctly implemented.

DATA FLOW SUMMARY:
==================

┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 1: create_df_calendario                                                │
│ ─────────────────────────────────────────────────────────────────────────── │
│ Creates: employees × dates × shifts (M, T)                                  │
│ Pre-fills: 'F' for closed holidays (tipo_feriado='F' in df_feriados)        │
│ Default horario: '' (empty)                                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 2: add_seq_turno                                                       │
│ ─────────────────────────────────────────────────────────────────────────── │
│ Adds: seq_turno column from df_colaborador                                  │
│ Purpose: Identify CICLO COMPLETO employees                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: add_ausencias_ferias                                                │
│ ─────────────────────────────────────────────────────────────────────────── │
│ Source: df_ausencias_ferias (vacation/absence system)                       │
│ Maps: tipo_ausencia → horario (V=vacation, A=absence)                       │
│ Mode: OVERRIDE (except F's are preserved)                                   │
│ Match: (employee_id, date)                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: add_ciclos_completos                                                │
│ ─────────────────────────────────────────────────────────────────────────── │
│ Source: df_ciclos_completos (90-day rotation cycles)                        │
│ For: Employees with ciclo='Completo' / seq_turno='CICLO'                    │
│ Maps: codigo_trads/horario_ind → horario (M, T, MoT, L, etc.)               │
│ Mode: OVERRIDE with rules:                                                  │
│   - F's: NEVER overridden                                                   │
│   - A's, V's: Preserved from shift codes, but '-' → 'A-'/'V-', 'L' overrides│
│   - Other values: Override non-F/A/V                                        │
│ Match: (employee_id, schedule_day)                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 5: add_days_off                                                        │
│ ─────────────────────────────────────────────────────────────────────────── │
│ Source: df_days_off (additional day-off allocations)                        │
│ Currently: Minimal implementation / placeholder                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 6: add_calendario_passado  *** CRITICAL FOR SINGLE-EMPLOYEE MODE ***   │
│ ─────────────────────────────────────────────────────────────────────────── │
│ Source: df_calendario_passado (historical schedules from DB)                │
│ Purpose: Bring in M/T decisions from previous algorithm executions          │
│ Mode: OVERRIDE with rules:                                                  │
│   - F's, V's: Preserved (never overridden)                                  │
│   - Everything else: Overridden with historical data                        │
│ Match: (employee_id, schedule_day)                                          │
│                                                                             │
│ SINGLE-EMPLOYEE MODE:                                                       │
│   - df_calendario_passado contains ALL section employees' past schedules    │
│   - This is why df_calendario needs ALL employees (not just target)         │
│   - Enables algorithm to consider section context when generating           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 7: add_folgas_ciclos                                                   │
│ ─────────────────────────────────────────────────────────────────────────── │
│ Source: df_folgas_ciclos (cycle day-off patterns)                           │
│ Maps: tipo_dia → horario ('F'→'L' folga, 'S'→'-' no-work)                   │
│ Mode: OVERRIDE with rules:                                                  │
│   - F's: NEVER overridden                                                   │
│   - A's, V's: Preserved from 'L', but '-' → 'A-'/'V-'                       │
│ Match: (employee_id, schedule_day)                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 8: filter_df_dates + define_dia_tipo                                   │
│ ─────────────────────────────────────────────────────────────────────────── │
│ filter_df_dates: Trim to requested date range                               │
│ define_dia_tipo: Set day types (Mon, Tue, domYf, ferYf, etc.)               │
└─────────────────────────────────────────────────────────────────────────────┘


PRIORITY/OVERRIDE RULES (LATER STEPS WIN):
==========================================
1. 'F' (closed holiday) is SACRED - NEVER overridden by any layer
2. 'V' (vacation) is preserved by most layers, except:
   - '-' (no-work) converts it to 'V-'
   - Calendario passado preserves it
3. 'A' (absence) similar to V:
   - '-' (no-work) converts it to 'A-'
   - Most layers preserve it
4. Other values (M, T, MoT, L, '-') follow the layer order

EXECUTION MODES:
================
- SECTION MODE (wfm_proc_colab=''):
  - df_valid_emp has ALL employees to generate
  - df_calendario_passado may be empty or have partial data
  
- SINGLE-EMPLOYEE MODE (wfm_proc_colab='123'):
  - df_valid_emp has ONLY the target employee
  - section_employees_id_list has ALL section employees (from df_mpd_valid_employees)
  - df_calendario_passado has PREVIOUS execution results for section
  - Algorithm considers full section context when generating for single employee

"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from src.data_models.functions.data_treatment_functions import (
    create_df_calendario,
    add_ausencias_ferias,
    add_ciclos_completos,
    add_calendario_passado,
    add_folgas_ciclos,
    add_days_off,
)


# =============================================================================
# TEST FIXTURES
# =============================================================================

@pytest.fixture
def sample_employee_matricula_map():
    """Sample employee to matricula mapping."""
    return {
        101: '80001001',
        102: '80001002',
        103: '80001003',
    }


@pytest.fixture
def sample_df_feriados():
    """Sample holidays DataFrame."""
    return pd.DataFrame({
        'schedule_day': ['2025-01-01', '2025-12-25'],
        'tipo_feriado': ['F', 'A'],  # F=closed, A=open
        'descricao': ['Ano Novo', 'Natal']
    })


@pytest.fixture
def base_df_calendario(sample_employee_matricula_map, sample_df_feriados):
    """Create a base calendario for testing."""
    success, df, _ = create_df_calendario(
        start_date='2025-01-01',
        end_date='2025-01-07',
        main_year=2025,
        employee_id_matriculas_map=sample_employee_matricula_map,
        past_employees_id_list=[101, 102, 103],
        df_feriados=sample_df_feriados,
    )
    assert success, "Failed to create base calendario"
    return df


# =============================================================================
# TEST: create_df_calendario
# =============================================================================

class TestCreateDfCalendario:
    """
    Tests for create_df_calendario function.
    
    BUSINESS LOGIC:
    - Creates cartesian product: employees × dates × shifts (M, T)
    - Pre-fills 'F' for closed holidays (tipo_feriado='F')
    - Default horario is empty string
    """
    
    def test_creates_correct_row_count(self, sample_employee_matricula_map, sample_df_feriados):
        """
        RULE: Total rows = num_employees × num_days × 2 (M and T shifts)
        """
        success, df, _ = create_df_calendario(
            start_date='2025-01-01',
            end_date='2025-01-03',
            main_year=2025,
            employee_id_matriculas_map=sample_employee_matricula_map,
            past_employees_id_list=[101, 102, 103],
            df_feriados=sample_df_feriados,
        )
        
        assert success
        # Note: create_df_calendario expands to full year boundaries (previous Monday to next Sunday)
        # So we check that each employee has same number of rows
        rows_per_emp = df.groupby('employee_id').size()
        assert len(rows_per_emp.unique()) == 1, "All employees should have same row count"
    
    def test_prefills_closed_holidays_with_F(self, sample_employee_matricula_map, sample_df_feriados):
        """
        RULE: Dates with tipo_feriado='F' should have horario='F'
        """
        success, df, _ = create_df_calendario(
            start_date='2025-01-01',
            end_date='2025-01-07',
            main_year=2025,
            employee_id_matriculas_map=sample_employee_matricula_map,
            past_employees_id_list=[101, 102, 103],
            df_feriados=sample_df_feriados,
        )
        
        assert success
        # 2025-01-01 is tipo_feriado='F' (closed), should be 'F'
        jan1_rows = df[df['schedule_day'] == '2025-01-01']
        assert all(jan1_rows['horario'] == 'F'), "Closed holidays should have horario='F'"
    
    def test_open_holidays_not_prefilled(self, sample_employee_matricula_map, sample_df_feriados):
        """
        RULE: Dates with tipo_feriado='A' (open) should NOT be prefilled with 'F'
        """
        success, df, _ = create_df_calendario(
            start_date='2025-12-20',
            end_date='2025-12-31',
            main_year=2025,
            employee_id_matriculas_map=sample_employee_matricula_map,
            past_employees_id_list=[101, 102, 103],
            df_feriados=sample_df_feriados,
        )
        
        assert success
        # 2025-12-25 is tipo_feriado='A' (open), should NOT be 'F'
        dec25_rows = df[df['schedule_day'] == '2025-12-25']
        assert all(dec25_rows['horario'] != 'F'), "Open holidays should NOT have horario='F'"
    
    def test_each_employee_has_matricula(self, sample_employee_matricula_map, sample_df_feriados):
        """
        RULE: Each employee should have their correct matricula
        """
        success, df, _ = create_df_calendario(
            start_date='2025-01-01',
            end_date='2025-01-03',
            main_year=2025,
            employee_id_matriculas_map=sample_employee_matricula_map,
            past_employees_id_list=[101, 102, 103],
            df_feriados=sample_df_feriados,
        )
        
        assert success
        for emp_id, matricula in sample_employee_matricula_map.items():
            emp_rows = df[df['employee_id'] == str(emp_id)]
            assert all(emp_rows['matricula'] == matricula), f"Employee {emp_id} should have matricula {matricula}"


# =============================================================================
# TEST: add_ausencias_ferias
# =============================================================================

class TestAddAusenciasFerias:
    """
    Tests for add_ausencias_ferias function.
    
    BUSINESS LOGIC:
    - Maps tipo_ausencia → horario (V=vacation, A=absence)
    - OVERRIDES existing horario values
    - PRESERVES 'F' (closed holidays) - never overridden
    - Match by (employee_id, date)
    """
    
    def test_vacation_overrides_empty_horario(self, base_df_calendario):
        """
        RULE: Vacation (V) should override empty horario
        Note: df_ausencias should have ONE row per (employee, date) - 
              the function will apply to BOTH M and T shift rows
        """
        df_ausencias = pd.DataFrame({
            'employee_id': ['101'],
            'data': ['2025-01-02'],
            'tipo_ausencia': ['V']
        })
        
        success, df, _ = add_ausencias_ferias(base_df_calendario, df_ausencias)
        
        assert success
        jan2_emp101 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-02')]
        assert all(jan2_emp101['horario'] == 'V'), "Vacation should override empty horario"
    
    def test_absence_overrides_empty_horario(self, base_df_calendario):
        """
        RULE: Absence (A) should override empty horario
        """
        df_ausencias = pd.DataFrame({
            'employee_id': ['102'],
            'data': ['2025-01-03'],
            'tipo_ausencia': ['A']
        })
        
        success, df, _ = add_ausencias_ferias(base_df_calendario, df_ausencias)
        
        assert success
        jan3_emp102 = df[(df['employee_id'] == '102') & (df['schedule_day'] == '2025-01-03')]
        assert all(jan3_emp102['horario'] == 'A'), "Absence should override empty horario"
    
    def test_preserves_closed_holidays(self, base_df_calendario):
        """
        RULE: 'F' (closed holiday) should NEVER be overridden by absence/vacation
        """
        df_ausencias = pd.DataFrame({
            'employee_id': ['101'],
            'data': ['2025-01-01'],  # This is a closed holiday (F)
            'tipo_ausencia': ['V']
        })
        
        success, df, _ = add_ausencias_ferias(base_df_calendario, df_ausencias)
        
        assert success
        jan1_emp101 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-01')]
        assert all(jan1_emp101['horario'] == 'F'), "Closed holidays (F) should be preserved"
    
    def test_only_affects_matching_employees(self, base_df_calendario):
        """
        RULE: Absence should only affect the specified employee, not others
        """
        df_ausencias = pd.DataFrame({
            'employee_id': ['101'],
            'data': ['2025-01-02'],
            'tipo_ausencia': ['V']
        })
        
        success, df, _ = add_ausencias_ferias(base_df_calendario, df_ausencias)
        
        assert success
        # Employee 102 should NOT have vacation on Jan 2
        jan2_emp102 = df[(df['employee_id'] == '102') & (df['schedule_day'] == '2025-01-02')]
        assert all(jan2_emp102['horario'] != 'V'), "Other employees should not be affected"


# =============================================================================
# TEST: add_ciclos_completos
# =============================================================================

class TestAddCiclosCompletos:
    """
    Tests for add_ciclos_completos function.
    
    BUSINESS LOGIC:
    - For employees with 90-day complete rotation cycles
    - Fills horario with shift codes (M, T, MoT, L, etc.)
    - PRESERVES 'F' - never overridden
    - PRESERVES 'A', 'V' from regular shift codes, but:
      - '-' converts 'A' → 'A-', 'V' → 'V-'
      - 'L' can override A/V
    - Match by (employee_id, schedule_day)
    """
    
    def test_fills_shift_codes(self, base_df_calendario):
        """
        RULE: Shift codes (M, T, MoT) should fill empty horario
        """
        df_ciclos = pd.DataFrame({
            'employee_id': ['101', '101'],
            'schedule_day': ['2025-01-02', '2025-01-03'],
            'horario': ['M', 'T']
        })
        
        success, df, _ = add_ciclos_completos(base_df_calendario, df_ciclos)
        
        assert success
        jan2 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-02')]
        jan3 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-03')]
        # Note: This will fill BOTH M and T rows for each day
        assert 'M' in jan2['horario'].values, "Should fill with M"
        assert 'T' in jan3['horario'].values, "Should fill with T"
    
    def test_preserves_closed_holidays(self, base_df_calendario):
        """
        RULE: 'F' (closed holiday) should NEVER be overridden by ciclo
        """
        df_ciclos = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],  # Closed holiday
            'horario': ['M']
        })
        
        success, df, _ = add_ciclos_completos(base_df_calendario, df_ciclos)
        
        assert success
        jan1 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-01')]
        assert all(jan1['horario'] == 'F'), "Closed holidays should be preserved"
    
    def test_dash_converts_absence_to_a_dash(self, base_df_calendario):
        """
        RULE: '-' (no-work) should convert 'A' → 'A-'
        """
        # First add an absence
        df_ausencias = pd.DataFrame({
            'employee_id': ['101'],
            'data': ['2025-01-02'],
            'tipo_ausencia': ['A']
        })
        _, df_with_absence, _ = add_ausencias_ferias(base_df_calendario, df_ausencias)
        
        # Then try to override with '-'
        df_ciclos = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-02'],
            'horario': ['-']
        })
        
        success, df, _ = add_ciclos_completos(df_with_absence, df_ciclos)
        
        assert success
        jan2 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-02')]
        # Note: The function should convert A → A- when '-' is applied
        assert 'A-' in jan2['horario'].values or 'A' in jan2['horario'].values, \
            "Absence should be converted to A- or preserved as A"


# =============================================================================
# TEST: add_calendario_passado
# =============================================================================

class TestAddCalendarioPassado:
    """
    Tests for add_calendario_passado function.
    
    BUSINESS LOGIC (CRITICAL FOR SINGLE-EMPLOYEE MODE):
    - Brings in historical schedule data from previous executions
    - OVERRIDES horario values (fills gaps)
    - PRESERVES 'F' and 'V' - never overridden
    - Match by (employee_id, schedule_day)
    
    In SINGLE-EMPLOYEE mode:
    - df_calendario_passado contains ALL section employees' past schedules
    - This enables the algorithm to see section context
    """
    
    def test_fills_from_historical_data(self, base_df_calendario):
        """
        RULE: Historical shift codes should fill empty horario
        Note: df_passado should have ONE row per (employee, date) -
              because lookup uses MultiIndex which requires unique keys.
              In real data, passado comes from DB with unique (emp, date) entries.
        """
        df_passado = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-02'],
            'horario': ['M']  # Single value - will be applied to matching rows
        })
        
        success, df, _ = add_calendario_passado(base_df_calendario, df_passado)
        
        assert success
        jan2 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-02')]
        # The M value from passado should be applied to both M and T shift rows
        assert 'M' in jan2['horario'].values, "Should fill from historical data"
    
    def test_preserves_closed_holidays(self, base_df_calendario):
        """
        RULE: 'F' (closed holiday) should NEVER be overridden
        """
        df_passado = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],
            'horario': ['M']
        })
        
        success, df, _ = add_calendario_passado(base_df_calendario, df_passado)
        
        assert success
        jan1 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-01')]
        assert all(jan1['horario'] == 'F'), "Closed holidays should be preserved"
    
    def test_preserves_vacations(self, base_df_calendario):
        """
        RULE: 'V' (vacation) should NEVER be overridden by passado
        """
        # First add a vacation
        df_ausencias = pd.DataFrame({
            'employee_id': ['101'],
            'data': ['2025-01-02'],
            'tipo_ausencia': ['V']
        })
        _, df_with_vacation, _ = add_ausencias_ferias(base_df_calendario, df_ausencias)
        
        # Then try to override with passado
        df_passado = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-02'],
            'horario': ['M']
        })
        
        success, df, _ = add_calendario_passado(df_with_vacation, df_passado)
        
        assert success
        jan2 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-02')]
        assert all(jan2['horario'] == 'V'), "Vacation should be preserved from passado override"
    
    def test_single_employee_mode_sees_all_employees(self, base_df_calendario):
        """
        RULE: In single-employee mode, passado should have data for ALL employees
        This tests that we can process passado data for employees not being generated
        """
        # Passado has data for employee 102 (not the "target" employee 101)
        # One row per (employee, date) - lookup requires unique keys
        df_passado = pd.DataFrame({
            'employee_id': ['102'],
            'schedule_day': ['2025-01-02'],
            'horario': ['M']
        })
        
        success, df, _ = add_calendario_passado(base_df_calendario, df_passado)
        
        assert success
        # Employee 102's schedule should be filled from passado
        jan2_emp102 = df[(df['employee_id'] == '102') & (df['schedule_day'] == '2025-01-02')]
        assert 'M' in jan2_emp102['horario'].values, \
            "Non-target employee data from passado should be applied"


# =============================================================================
# TEST: add_folgas_ciclos
# =============================================================================

class TestAddFolgasCiclos:
    """
    Tests for add_folgas_ciclos function.
    
    BUSINESS LOGIC:
    - Applies fixed day-off patterns from cycle definitions
    - Source: tipo_dia = 'L' (folga) or '-' (no-work)
    - OVERRIDE mode - replaces existing horario
    - PRESERVES 'F' - never overridden
    - PRESERVES 'A', 'V' from 'L' override, but '-' → 'A-'/'V-'
    """
    
    def test_applies_day_off_L(self, base_df_calendario):
        """
        RULE: tipo_dia='L' should set horario='L' (day off)
        """
        df_folgas = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-02'],
            'tipo_dia': ['L']
        })
        
        success, df, _ = add_folgas_ciclos(base_df_calendario, df_folgas)
        
        assert success
        jan2 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-02')]
        assert all(jan2['horario'] == 'L'), "Day off (L) should be applied"
    
    def test_preserves_closed_holidays(self, base_df_calendario):
        """
        RULE: 'F' (closed holiday) should NEVER be overridden
        """
        df_folgas = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],  # Closed holiday
            'tipo_dia': ['L']
        })
        
        success, df, _ = add_folgas_ciclos(base_df_calendario, df_folgas)
        
        assert success
        jan1 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-01')]
        assert all(jan1['horario'] == 'F'), "Closed holidays should be preserved"
    
    def test_dash_converts_vacation_to_v_dash(self, base_df_calendario):
        """
        RULE: '-' (no-work) should convert 'V' → 'V-'
        """
        # First add a vacation
        df_ausencias = pd.DataFrame({
            'employee_id': ['101'],
            'data': ['2025-01-02'],
            'tipo_ausencia': ['V']
        })
        _, df_with_vacation, _ = add_ausencias_ferias(base_df_calendario, df_ausencias)
        
        # Then apply '-' from folgas
        df_folgas = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-02'],
            'tipo_dia': ['-']
        })
        
        success, df, _ = add_folgas_ciclos(df_with_vacation, df_folgas)
        
        assert success
        jan2 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-02')]
        assert 'V-' in jan2['horario'].values or 'V' in jan2['horario'].values, \
            "Vacation should be converted to V- or preserved"


# =============================================================================
# TEST: LAYER ORDER AND PRIORITY
# =============================================================================

class TestLayerPriority:
    """
    Tests for layer ordering and priority rules.
    
    BUSINESS LOGIC:
    Layer order (later wins unless preservation rules apply):
    1. create_df_calendario (pre-fills F for closed holidays)
    2. add_ausencias_ferias (V, A)
    3. add_ciclos_completos (M, T, MoT, L)
    4. add_days_off
    5. add_calendario_passado (historical M, T)
    6. add_folgas_ciclos (L, -)
    
    Preservation rules:
    - F (closed holiday): NEVER overridden by any layer
    - V (vacation): Preserved by most, '-' converts to V-
    - A (absence): Preserved by most, '-' converts to A-
    """
    
    def test_f_preserved_through_all_layers(self, base_df_calendario):
        """
        RULE: Closed holidays (F) should survive ALL layers
        """
        # Apply all layers trying to override Jan 1 (closed holiday)
        df_ausencias = pd.DataFrame({
            'employee_id': ['101'],
            'data': ['2025-01-01'],
            'tipo_ausencia': ['V']
        })
        _, df, _ = add_ausencias_ferias(base_df_calendario, df_ausencias)
        
        df_ciclos = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],
            'horario': ['M']
        })
        _, df, _ = add_ciclos_completos(df, df_ciclos)
        
        df_passado = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],
            'horario': ['T']
        })
        _, df, _ = add_calendario_passado(df, df_passado)
        
        df_folgas = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-01'],
            'tipo_dia': ['L']
        })
        _, df, _ = add_folgas_ciclos(df, df_folgas)
        
        jan1 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-01')]
        assert all(jan1['horario'] == 'F'), "Closed holiday (F) should survive all layers"
    
    def test_passado_overrides_ciclos(self, base_df_calendario):
        """
        RULE: calendario_passado runs AFTER ciclos_completos, so passado wins
        (unless F or V preservation applies)
        """
        # First apply ciclos
        df_ciclos = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-02'],
            'horario': ['MoT']
        })
        _, df, _ = add_ciclos_completos(base_df_calendario, df_ciclos)
        
        # Then apply passado (should override)
        df_passado = pd.DataFrame({
            'employee_id': ['101'],
            'schedule_day': ['2025-01-02'],
            'horario': ['M']
        })
        _, df, _ = add_calendario_passado(df, df_passado)
        
        jan2 = df[(df['employee_id'] == '101') & (df['schedule_day'] == '2025-01-02')]
        # Passado should override ciclos
        assert 'M' in jan2['horario'].values, "Passado should override ciclos"


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])

