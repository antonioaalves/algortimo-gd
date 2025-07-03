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
$ProcessIds = @(227, 230, 256, 278, 301, 364, 573, 586, 599, 651, 654, 680, 683, 687, 709, 716, 719, 726, 749, 760, 763, 766, 769, 778, 857, 960, 966, 995, 1007, 1052, 1056, 1080, 1182, 1209, 1319, 1421, 1427)
# Common configuration (modify as needed)
$UseDb = $true                    # Set to $false for CSV mode
$Algorithm = "alcampoAlgorithm"   # Algorithm to use
$WfmUser = "BATCH_USER"          # WFM user
$StartDate = "2025-07-01"        # Start date
$EndDate = "2025-12-31"          # End date
$WfmProcColab = $null            # WFM process collaborator (set to $null if not needed)

$ScriptPath = "batch_process.py" # Python script to run

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

# Create logs directory if it doesn't exist
$LogDir = "logs\batch_runs"
if (!(Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}

# Get current timestamp for log file
$Timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$LogFile = "$LogDir\batch_run_$Timestamp.log"

# Initialize log file
"Starting batch run at $(Get-Date)" | Out-File -FilePath $LogFile -Encoding UTF8
"Process IDs: $($ProcessIds -join ', ')" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"Virtual Environment: $env:VIRTUAL_ENV" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"" | Out-File -FilePath $LogFile -Append -Encoding UTF8

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
    
    # Log the current process
    "[$(Get-Date)] Starting Process ID: $ProcessId" | Out-File -FilePath $LogFile -Append -Encoding UTF8
    
    # Build the command string
    $Command = "python $ScriptPath"
    
    if ($UseDb) {
        $Command += " --use-db"
    } else {
        $Command += " --use-csv"
    }
    
    $Command += " --algorithm $Algorithm"
    $Command += " --current-process-id $ProcessId"
    $Command += " --api-proc-id $ProcessId"
    $Command += " --wfm-proc-id $ProcessId"
    $Command += " --wfm-user `"$WfmUser`""
    $Command += " --start-date $StartDate"
    $Command += " --end-date $EndDate"
    
    if ($WfmProcColab) {
        $Command += " --wfm-proc-colab `"$WfmProcColab`""
    }
    
    Write-Host "Running: $Command" -ForegroundColor Cyan
    Write-Host ""
    
    # Execute the command using Invoke-Expression
    try {
        Invoke-Expression $Command
        $ExitCode = $LASTEXITCODE
        
        if ($ExitCode -eq 0) {
            Write-Host ""
            Write-Host "[SUCCESS] Process ID $ProcessId completed successfully" -ForegroundColor Green
            "[$(Get-Date)] SUCCESS: Process ID $ProcessId" | Out-File -FilePath $LogFile -Append -Encoding UTF8
            $SuccessCount++
        } else {
            Write-Host ""
            Write-Host "[ERROR] Process ID $ProcessId failed with exit code $ExitCode" -ForegroundColor Red
            "[$(Get-Date)] ERROR: Process ID $ProcessId - Exit code $ExitCode" | Out-File -FilePath $LogFile -Append -Encoding UTF8
            $FailedCount++
            $FailedProcesses += $ProcessId
        }
    }
    catch {
        Write-Host ""
        Write-Host "[ERROR] Process ID $ProcessId failed with exception: $($_.Exception.Message)" -ForegroundColor Red
        "[$(Get-Date)] ERROR: Process ID $ProcessId - Exception: $($_.Exception.Message)" | Out-File -FilePath $LogFile -Append -Encoding UTF8
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
Write-Host "Log file: $LogFile" -ForegroundColor Yellow
Write-Host "Completed at: $(Get-Date)" -ForegroundColor White
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""

# Log the summary
"" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"===============================================" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"BATCH RUN SUMMARY" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"===============================================" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"Total processes: $TotalProcesses" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"Successful: $SuccessCount" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"Failed: $FailedCount" | Out-File -FilePath $LogFile -Append -Encoding UTF8
if ($FailedProcesses.Count -gt 0) {
    "Failed Process IDs: $($FailedProcesses -join ', ')" | Out-File -FilePath $LogFile -Append -Encoding UTF8
}
"Completed at: $(Get-Date)" | Out-File -FilePath $LogFile -Append -Encoding UTF8
"===============================================" | Out-File -FilePath $LogFile -Append -Encoding UTF8

# Pause to see results (comment out if running automated)
Write-Host "Press any key to continue..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

# Exit with error code if any processes failed
if ($FailedCount -gt 0) {
    Write-Host "Exiting with error code 1 due to failed processes" -ForegroundColor Red
    exit 1
} else {
    Write-Host "All processes completed successfully" -ForegroundColor Green
    exit 0
}