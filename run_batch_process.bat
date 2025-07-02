@echo off
setlocal enabledelayedexpansion

REM =============================================================================
REM Batch Process Runner for my_new_project
REM This script runs batch_process.py for multiple process IDs
REM =============================================================================

echo.
echo ===============================================
echo   Batch Process Runner for my_new_project
echo ===============================================
echo.

REM Configuration - Modify these values as needed
REM =============================================================================

REM List of process IDs to run (space-separated)
set PROCESS_IDS=253037 253035 253032 253029 253009 252995 252977 252974 252971

REM Common configuration (modify as needed)
set USE_DB=--use-db
set ALGORITHM=alcampoAlgorithm
set WFM_USER=BATCH_USER
set START_DATE=2025-07-01
set END_DATE=2025-12-31
set WFM_PROC_COLAB=

REM Optional: Set Python executable path if not in PATH
REM set PYTHON_EXE=C:\Python39\python.exe
set PYTHON_EXE=python

REM Optional: Set the script path if running from different directory
REM set SCRIPT_PATH=C:\path\to\your\project\batch_process.py
set SCRIPT_PATH=batch_process.py

REM =============================================================================
REM Processing Loop
REM =============================================================================

set TOTAL_PROCESSES=0
set SUCCESS_COUNT=0
set FAILED_COUNT=0

REM Count total processes
for %%i in (%PROCESS_IDS%) do (
    set /a TOTAL_PROCESSES+=1
)

echo Total processes to run: %TOTAL_PROCESSES%
echo.

REM Create logs directory if it doesn't exist
if not exist "logs\batch_runs" mkdir "logs\batch_runs"

REM Get current timestamp for log file
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "timestamp=%dt:~0,4%-%dt:~4,2%-%dt:~6,2%_%dt:~8,2%-%dt:~10,2%-%dt:~12,2%"

set LOG_FILE=logs\batch_runs\batch_run_%timestamp%.log

echo Starting batch run at %date% %time% > "%LOG_FILE%"
echo Process IDs: %PROCESS_IDS% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

set CURRENT_PROCESS=0

REM Loop through each process ID
for %%P in (%PROCESS_IDS%) do (
    set /a CURRENT_PROCESS+=1
    
    echo.
    echo -----------------------------------------------
    echo Processing [!CURRENT_PROCESS!/%TOTAL_PROCESSES%]: Process ID %%P
    echo -----------------------------------------------
    echo.
    
    REM Log the current process
    echo [%date% %time%] Starting Process ID: %%P >> "%LOG_FILE%"
    
    REM Build the command
    set CMD=%PYTHON_EXE% %SCRIPT_PATH% %USE_DB% --algorithm %ALGORITHM% --current-process-id %%P --api-proc-id %%P --wfm-proc-id %%P --wfm-user %WFM_USER% --start-date %START_DATE% --end-date %END_DATE%
    
    REM Add wfm-proc-colab if not empty
    if not "%WFM_PROC_COLAB%"=="" (
        set CMD=!CMD! --wfm-proc-colab %WFM_PROC_COLAB%
    )
    
    echo Running: !CMD!
    echo.
    
    REM Execute the command
    !CMD!
    
    REM Check the exit code
    if !ERRORLEVEL! EQU 0 (
        echo.
        echo [SUCCESS] Process ID %%P completed successfully
        echo [%date% %time%] SUCCESS: Process ID %%P >> "%LOG_FILE%"
        set /a SUCCESS_COUNT+=1
    ) else (
        echo.
        echo [ERROR] Process ID %%P failed with exit code !ERRORLEVEL!
        echo [%date% %time%] ERROR: Process ID %%P - Exit code !ERRORLEVEL! >> "%LOG_FILE%"
        set /a FAILED_COUNT+=1
    )
    
    echo.
    echo -----------------------------------------------
    echo.
)

REM =============================================================================
REM Summary
REM =============================================================================

echo.
echo ===============================================
echo   BATCH RUN SUMMARY
echo ===============================================
echo Total processes: %TOTAL_PROCESSES%
echo Successful: %SUCCESS_COUNT%
echo Failed: %FAILED_COUNT%
echo.
echo Log file: %LOG_FILE%
echo Completed at: %date% %time%
echo ===============================================
echo.

REM Log the summary
echo. >> "%LOG_FILE%"
echo =============================================== >> "%LOG_FILE%"
echo BATCH RUN SUMMARY >> "%LOG_FILE%"
echo =============================================== >> "%LOG_FILE%"
echo Total processes: %TOTAL_PROCESSES% >> "%LOG_FILE%"
echo Successful: %SUCCESS_COUNT% >> "%LOG_FILE%"
echo Failed: %FAILED_COUNT% >> "%LOG_FILE%"
echo Completed at: %date% %time% >> "%LOG_FILE%"
echo =============================================== >> "%LOG_FILE%"

REM Pause to see results (comment out if running automated)
pause

REM Exit with error code if any processes failed
if %FAILED_COUNT% GTR 0 (
    echo Exiting with error code 1 due to failed processes
    exit /b 1
) else (
    echo All processes completed successfully
    exit /b 0
)