"""
Test cases for date treatment logic in helper_functions.py

This module contains comprehensive test cases for:
- get_first_and_last_day_passado
- get_monday_of_previous_week  
- get_sunday_of_next_week
"""

import pytest
import pandas as pd
import datetime as dt
from unittest.mock import patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_models.functions.helper_functions import (
    get_first_and_last_day_passado,
    get_monday_of_previous_week,
    get_sunday_of_next_week
)


class TestGetMondayOfPreviousWeek:
    """Test cases for get_monday_of_previous_week function"""
    
    def test_monday_input(self):
        """Test when input date is a Monday"""
        # 2024-01-08 is a Monday
        result = get_monday_of_previous_week('2024-01-08')
        expected = '2024-01-01'  # Previous Monday
        assert result == expected
    
    def test_tuesday_input(self):
        """Test when input date is a Tuesday"""
        # 2024-01-09 is a Tuesday
        result = get_monday_of_previous_week('2024-01-09')
        expected = '2024-01-01'  # Previous Monday
        assert result == expected
    
    def test_wednesday_input(self):
        """Test when input date is a Wednesday"""
        # 2024-01-10 is a Wednesday
        result = get_monday_of_previous_week('2024-01-10')
        expected = '2024-01-01'  # Previous Monday
        assert result == expected
    
    def test_sunday_input(self):
        """Test when input date is a Sunday"""
        # 2024-01-14 is a Sunday
        result = get_monday_of_previous_week('2024-01-14')
        expected = '2024-01-01'  # Previous Monday
        assert result == expected
    
    def test_year_boundary(self):
        """Test when previous Monday falls in previous year"""
        # 2024-01-02 is a Tuesday
        result = get_monday_of_previous_week('2024-01-02')
        expected = '2023-12-25'  # Previous Monday in 2023
        assert result == expected
    
    def test_leap_year(self):
        """Test leap year handling"""
        # 2024-03-01 is a Friday in a leap year
        result = get_monday_of_previous_week('2024-03-01')
        expected = '2024-02-19'  # Previous Monday
        assert result == expected
    
    def test_invalid_date_format(self):
        """Test handling of invalid date format"""
        with patch('src.data_models.functions.helper_functions.logger') as mock_logger:
            result = get_monday_of_previous_week('invalid-date')
            assert result == ''
            mock_logger.error.assert_called_once()
    
    def test_empty_string(self):
        """Test handling of empty string input"""
        with patch('src.data_models.functions.helper_functions.logger') as mock_logger:
            result = get_monday_of_previous_week('')
            assert result == ''
            mock_logger.error.assert_called_once()


class TestGetSundayOfNextWeek:
    """Test cases for get_sunday_of_next_week function"""
    
    def test_monday_input(self):
        """Test when input date is a Monday"""
        # 2024-01-01 is a Monday
        result = get_sunday_of_next_week('2024-01-01')
        expected = '2024-01-07'  # Next Sunday
        assert result == expected
    
    def test_tuesday_input(self):
        """Test when input date is a Tuesday"""
        # 2024-01-02 is a Tuesday
        result = get_sunday_of_next_week('2024-01-02')
        expected = '2024-01-07'  # Next Sunday
        assert result == expected
    
    def test_saturday_input(self):
        """Test when input date is a Saturday"""
        # 2024-01-06 is a Saturday
        result = get_sunday_of_next_week('2024-01-06')
        expected = '2024-01-07'  # Next day (Sunday)
        assert result == expected
    
    def test_sunday_input(self):
        """Test when input date is a Sunday"""
        # 2024-01-07 is a Sunday
        result = get_sunday_of_next_week('2024-01-07')
        expected = '2024-01-14'  # Next Sunday
        assert result == expected
    
    def test_month_boundary(self):
        """Test when next Sunday falls in next month"""
        # 2024-01-30 is a Tuesday
        result = get_sunday_of_next_week('2024-01-30')
        expected = '2024-02-04'  # Next Sunday in February
        assert result == expected
    
    def test_year_boundary(self):
        """Test when next Sunday falls in next year"""
        # 2024-12-30 is a Monday
        result = get_sunday_of_next_week('2024-12-30')
        expected = '2025-01-05'  # Next Sunday in 2025
        assert result == expected
    
    def test_invalid_date_format(self):
        """Test handling of invalid date format"""
        with patch('src.data_models.functions.helper_functions.logger') as mock_logger:
            result = get_sunday_of_next_week('invalid-date')
            assert result == ''
            mock_logger.error.assert_called_once()


class TestGetFirstAndLastDayPassado:
    """Test cases for get_first_and_last_day_passado function"""
    
    def setup_method(self):
        """Set up test data"""
        self.main_year = '2024'
        self.jan_1 = '2024-01-01'
        self.dec_31 = '2024-12-31'
        self.mid_year_start = '2024-06-01'
        self.mid_year_end = '2024-09-30'
    
    def test_case_1_full_year_empty_wfm(self):
        """
        CASE 1: start_date = 01-01 e end_date = 31-12 e wfm_proc_colab = ''
        Should return Monday of previous week and day before start_date
        """
        result = get_first_and_last_day_passado(
            self.jan_1, self.dec_31, self.main_year, ''
        )
        expected_first = get_monday_of_previous_week(self.jan_1)
        expected_last = '2023-12-31'  # Day before 2024-01-01
        assert result == (expected_first, expected_last)
    
    def test_case_2_partial_start_full_end_empty_wfm(self):
        """
        CASE 2: start_date > 01-01 e end_date = 31-12 e wfm_proc_colab = ''
        Should return first January and day before start_date
        """
        result = get_first_and_last_day_passado(
            self.mid_year_start, self.dec_31, self.main_year, ''
        )
        expected_first = '2024-01-01'
        expected_last = '2024-05-31'  # Day before 2024-06-01
        assert result == (expected_first, expected_last)
    
    def test_case_3_full_start_partial_end_empty_wfm(self):
        """
        CASE 3: start_date = 01-01 e end_date < 31-12 e wfm_proc_colab = ''
        Should return Monday of previous week and day before start_date
        """
        result = get_first_and_last_day_passado(
            self.jan_1, self.mid_year_end, self.main_year, ''
        )
        expected_first = get_monday_of_previous_week(self.jan_1)
        expected_last = '2023-12-31'  # Day before 2024-01-01
        assert result == (expected_first, expected_last)
    
    def test_case_4_partial_start_partial_end_empty_wfm(self):
        """
        CASE 4: start_date > 01-01 e end_date < 31-12 e wfm_proc_colab = ''
        Should return first January and last December
        """
        result = get_first_and_last_day_passado(
            self.mid_year_start, self.mid_year_end, self.main_year, ''
        )
        expected_first = '2024-01-01'
        expected_last = '2024-12-31'
        assert result == (expected_first, expected_last)
    
    def test_case_5_full_year_with_wfm(self):
        """
        CASE 5: start_date = 01-01 e end_date = 31-12 e wfm_proc_colab != ''
        Should return Monday of previous week and Sunday of next week
        """
        result = get_first_and_last_day_passado(
            self.jan_1, self.dec_31, self.main_year, 'some_value'
        )
        expected_first = get_monday_of_previous_week(self.jan_1)
        expected_last = get_sunday_of_next_week(self.dec_31)
        assert result == (expected_first, expected_last)
    
    def test_case_6_partial_start_full_end_with_wfm(self):
        """
        CASE 6: start_date > 01-01 e end_date = 31-12 e wfm_proc_colab != ''
        Should return first January and Sunday of next week
        """
        result = get_first_and_last_day_passado(
            self.mid_year_start, self.dec_31, self.main_year, 'some_value'
        )
        expected_first = '2024-01-01'
        expected_last = get_sunday_of_next_week(self.dec_31)
        assert result == (expected_first, expected_last)
    
    def test_case_7_full_start_partial_end_with_wfm(self):
        """
        CASE 7: start_date = 01-01 e end_date < 31-12 e wfm_proc_colab != ''
        Should return Monday of previous week and Sunday of next week
        """
        result = get_first_and_last_day_passado(
            self.jan_1, self.mid_year_end, self.main_year, 'some_value'
        )
        expected_first = get_monday_of_previous_week(self.jan_1)
        expected_last = get_sunday_of_next_week(self.mid_year_end)
        assert result == (expected_first, expected_last)
    
    def test_case_8_partial_start_partial_end_with_wfm(self):
        """
        CASE 8: start_date > 01-01 e end_date < 31-12 e wfm_proc_colab != ''
        Should return first January and last December
        """
        result = get_first_and_last_day_passado(
            self.mid_year_start, self.mid_year_end, self.main_year, 'some_value'
        )
        expected_first = '2024-01-01'
        expected_last = '2024-12-31'
        assert result == (expected_first, expected_last)
    
    def test_edge_case_february_leap_year(self):
        """Test with February dates in leap year"""
        result = get_first_and_last_day_passado(
            '2024-02-01', '2024-02-29', self.main_year, ''
        )
        expected_first = '2024-01-01'
        expected_last = '2024-12-31'
        assert result == (expected_first, expected_last)
    
    def test_edge_case_single_day_range(self):
        """Test when start_date and end_date are the same"""
        result = get_first_and_last_day_passado(
            '2024-06-15', '2024-06-15', self.main_year, ''
        )
        expected_first = '2024-01-01'
        expected_last = '2024-12-31'
        assert result == (expected_first, expected_last)
    
    def test_wfm_proc_colab_edge_cases(self):
        """Test various wfm_proc_colab values"""
        # Test with None (should be treated as empty)
        result1 = get_first_and_last_day_passado(
            self.jan_1, self.dec_31, self.main_year, None
        )
        # Note: This might fail depending on how None is handled in the actual function
        
        # Test with whitespace (should be treated as non-empty)
        result2 = get_first_and_last_day_passado(
            self.jan_1, self.dec_31, self.main_year, ' '
        )
        expected_first = get_monday_of_previous_week(self.jan_1)
        expected_last = get_sunday_of_next_week(self.dec_31)
        assert result2 == (expected_first, expected_last)
    
    def test_invalid_date_format(self):
        """Test handling of invalid date formats"""
        with patch('src.data_models.functions.helper_functions.logger') as mock_logger:
            result = get_first_and_last_day_passado(
                'invalid-date', self.dec_31, self.main_year, ''
            )
            assert result == ('', '')
            mock_logger.error.assert_called_once()
    
    def test_start_date_after_end_date(self):
        """Test when start_date is after end_date"""
        # This should probably be handled as an error case
        result = get_first_and_last_day_passado(
            self.dec_31, self.jan_1, self.main_year, ''
        )
        # The function doesn't explicitly handle this case, 
        # so it will fall through to the else clause
        with patch('src.data_models.functions.helper_functions.logger') as mock_logger:
            result = get_first_and_last_day_passado(
                self.dec_31, self.jan_1, self.main_year, ''
            )
            # This should trigger the else clause and log an error
    
    
    def test_exception_handling(self):
        """Test that exceptions are properly caught and logged"""
        with patch('pandas.to_datetime', side_effect=Exception('Test exception')):
           with patch('src.data_models.functions.helper_functions.logger') as mock_logger:
                result = get_first_and_last_day_passado(
                    self.jan_1, self.dec_31, self.main_year, ''
                )
                assert result == ('', '')
                mock_logger.error.assert_called_once()


class TestIntegrationScenarios:
    """Integration test scenarios combining all three functions"""
    
    def test_full_workflow_case_5(self):
        """Test complete workflow for case 5 scenario"""
        start_date = '2024-01-01'  # Monday
        end_date = '2024-12-31'    # Tuesday
        main_year = '2024'
        wfm_proc_colab = 'employee123'
        
        # Execute main function
        first_day, last_day = get_first_and_last_day_passado(
            start_date, end_date, main_year, wfm_proc_colab
        )
        
        # Verify results using helper functions
        expected_first = get_monday_of_previous_week(start_date)  # 2023-12-25
        expected_last = get_sunday_of_next_week(end_date)        # 2025-01-05
        
        assert first_day == expected_first
        assert last_day == expected_last
        assert first_day == '2023-12-25'
        assert last_day == '2025-01-05'
    
    def test_full_workflow_case_2(self):
        """Test complete workflow for case 2 scenario"""
        start_date = '2024-06-15'  # Saturday
        end_date = '2024-12-31'    # Tuesday
        main_year = '2024'
        wfm_proc_colab = ''
        
        first_day, last_day = get_first_and_last_day_passado(
            start_date, end_date, main_year, wfm_proc_colab
        )
        
        assert first_day == '2024-01-01'
        assert last_day == '2024-06-14'  # Day before start_date
    
    def test_boundary_conditions(self):
        """Test various boundary conditions"""
        test_cases = [
            # (start, end, main_year, wfm, expected_case_description)
            ('2024-01-01', '2024-12-31', '2024', '', 'Case 1'),
            ('2024-01-02', '2024-12-31', '2024', '', 'Case 2'),
            ('2024-01-01', '2024-12-30', '2024', '', 'Case 3'),
            ('2024-01-02', '2024-12-30', '2024', '', 'Case 4'),
            ('2024-01-01', '2024-12-31', '2024', 'x', 'Case 5'),
            ('2024-01-02', '2024-12-31', '2024', 'x', 'Case 6'),
            ('2024-01-01', '2024-12-30', '2024', 'x', 'Case 7'),
            ('2024-01-02', '2024-12-30', '2024', 'x', 'Case 8'),
        ]
        
        for start, end, year, wfm, description in test_cases:
            result = get_first_and_last_day_passado(start, end, year, wfm)
            # Verify that we get valid date strings back
            assert len(result) == 2
            assert all(isinstance(date_str, str) for date_str in result)
            assert all(len(date_str) == 10 or date_str == '' for date_str in result)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
