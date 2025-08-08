# =============================================================================
# Batch Process Runner for my_new_project (Manual venv activation)
# Run this AFTER activating your virtual environment manually
# =============================================================================

Write-Host ""
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "  Batch Process Runner for my_new_project" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""

# Configuration - Modify these values as needed
# =============================================================================

# List of process IDs to run
$ProcessIds = @(249370, 249373, 249343)
# Common configuration (modify as needed)
$UseDb = $true                      # Set to $false for CSV mode
$Algorithm = "scr   scri    scri    "      # Algorithm to use
$ScriptPath = "batch_process.py"    # Python script to run

# =============================================================================
# Virtual Environment Check
# =============================================================================

Write-Host "Checking if virtual environment is activated..." -ForegroundColor Yellow

# Check if we're in a virtual environment
if ($env:VIRTUAL_ENV) {
    Write-Host "Virtual environment detected: $env:VIRTUAL_ENV" -ForegroundColor Green
} else {
    Write-Host "WARNING: No virtual environment detected!" -ForegroundColor Red
    Write-Host "Please activate your virtual environment first:" -ForegroundColor Yellow
    Write-Host "  .\.venv\Scripts\Activate.ps1" -ForegroundColor Yellow
    Write-Host ""
    $continue = Read-Host "Continue anyway? (y/N)"
    if ($continue -ne 'y' -and $continue -ne 'Y') {
        exit 1
    }
}

Write-Host ""

# =============================================================================
# Processing Setup
# =============================================================================

$TotalProcesses = $ProcessIds.Count
$SuccessCount = 0
$FailedCount = 0
$FailedProcesses = @()

Write-Host "Total processes to run: $TotalProcesses" -ForegroundColor Yellow
Write-Host ""

# =============================================================================
# Processing Loop
# =============================================================================

for ($i = 0; $i -lt $ProcessIds.Count; $i++) {
    $ProcessId = $ProcessIds[$i]
    $CurrentProcess = $i + 1
    
    Write-Host ""
    Write-Host "-----------------------------------------------" -ForegroundColor Gray
    Write-Host "Processing [$CurrentProcess/$TotalProcesses]: Process ID $ProcessId" -ForegroundColor White
    Write-Host "-----------------------------------------------" -ForegroundColor Gray
    Write-Host ""
    
    # Build the command with all required external_call_data parameters
    $Command = "python $ScriptPath"
    
    if ($UseDb) {
        $Command += " --use-db"
    } else {
        $Command += " --use-csv"
    }
    
    $Command += " --algorithm $Algorithm"
    $Command += " --current-process-id $ProcessId"
    $Command += " --api-proc-id 999"
    $Command += " --wfm-proc-id $ProcessId"
    $Command += " --wfm-user BATCH_USER"
    $Command += " --start-date 2025-01-01"
    $Command += " --end-date 2025-12-31"
    
    Write-Host "Running: $Command" -ForegroundColor Cyan
    Write-Host ""
    
    # Execute the command
    try {
        Invoke-Expression $Command
        $ExitCode = $LASTEXITCODE
        
        if ($ExitCode -eq 0) {
            Write-Host ""
            Write-Host "[SUCCESS] Process ID $ProcessId completed successfully" -ForegroundColor Green
            $SuccessCount++
        } else {
            Write-Host ""
            Write-Host "[ERROR] Process ID $ProcessId failed with exit code $ExitCode" -ForegroundColor Red
            $FailedCount++
            $FailedProcesses += $ProcessId
        }
    }
    catch {
        Write-Host ""
        Write-Host "[ERROR] Process ID $ProcessId failed with exception: $($_.Exception.Message)" -ForegroundColor Red
        $FailedCount++
        $FailedProcesses += $ProcessId
    }
    
    Write-Host ""
    Write-Host "-----------------------------------------------" -ForegroundColor Gray
    Write-Host ""
}

# =============================================================================
# Summary
# =============================================================================

Write-Host ""
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "   BATCH RUN SUMMARY" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "Total processes: $TotalProcesses" -ForegroundColor White
Write-Host "Successful: $SuccessCount" -ForegroundColor Green
Write-Host "Failed: $FailedCount" -ForegroundColor Red

if ($FailedProcesses.Count -gt 0) {
    Write-Host "Failed Process IDs: $($FailedProcesses -join ', ')" -ForegroundColor Red
}

Write-Host ""
Write-Host "Completed at: $(Get-Date)" -ForegroundColor White
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""