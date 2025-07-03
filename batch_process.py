#!/usr/bin/env python3
"""Batch processing module for my_new_project."""

import time
import os
import click
import sys
from typing import Dict, Any
from datetime import datetime

# Import base_data_project components
from base_data_project.log_config import setup_logger, get_logger
from base_data_project.utils import create_components

# Import project-specific components
from src.config import CONFIG, PROJECT_NAME
from src.services.example_service import AlgoritmoGDService

# Initialize logger with configuration first
setup_logger(
    project_name=PROJECT_NAME,
    log_level=CONFIG.get('log_level', 'INFO'),
    log_dir=CONFIG.get('log_dir', 'logs'),
    console_output=CONFIG.get('console_output', True)
)

# Then get the logger instance for use throughout the file
logger = get_logger(PROJECT_NAME)

def run_batch_process(data_manager, process_manager, algorithm="example_algorithm", external_call_dict=None):
    """
    Run the process in batch mode without user interaction.
    
    Args:
        data_manager: Data manager instance
        process_manager: Process manager instance
        algorithm: Name of the algorithm to use
        external_call_dict: External call data dictionary
        
    Returns:
        True if successful, False otherwise
    """
    logger.info("Starting batch process")
    
    try:
        # Create the service with data and process managers (same as main.py)
        service = AlgoritmoGDService(
            data_manager=data_manager,
            process_manager=process_manager,
            external_call_dict=external_call_dict or {},
            config=CONFIG,
            project_name=PROJECT_NAME
        )
        
        # Initialize a new process
        process_id = service.initialize_process(
            "Batch Processing Run", 
            f"Batch process run on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        logger.info(f"Initialized process with ID: {process_id}")
        
        # Display process start
        click.echo(click.style(f"Starting batch process (ID: {process_id})", fg="green", bold=True))
        click.echo()
        
        # Run each stage (same stages as main.py)
        stages = ['data_loading', 'processing']
        for stage in stages:
            click.echo(click.style(f"Stage: {stage}", fg="blue", bold=True))
            
            # Execute stage without user interaction
            if stage == 'processing':
                # Prepare algorithm parameters if needed
                algorithm_params = CONFIG.get('algorithm_defaults', {}).get(algorithm, {})
                success = service.execute_stage(stage, algorithm_name=algorithm, algorithm_params=algorithm_params)
            else:
                success = service.execute_stage(stage)
            
            if not success:
                click.echo(click.style(f"✘ {stage} failed", fg="red", bold=True))
                return False
            else:
                click.echo(click.style(f"✓ {stage} completed successfully", fg="green"))
                click.echo()
        
        # Finalize the process
        service.finalize_process()
        
        # Display process summary
        if process_manager:
            process_summary = service.get_process_summary()
            status_counts = process_summary.get('status_counts', {})
            
            click.echo(click.style("Process Summary:", fg="blue", bold=True))
            click.echo(f"Process ID: {process_id}")
            click.echo(f"Completed stages: {status_counts.get('completed', 0)}")
            click.echo(f"Failed stages: {status_counts.get('failed', 0)}")
            click.echo(f"Skipped stages: {status_counts.get('skipped', 0)}")
            click.echo(f"Overall progress: {process_summary.get('progress', 0) * 100:.1f}%")
            click.echo()
        
        # Display output location
        output_dir = os.path.abspath(CONFIG.get('output_dir', "data/output"))
        
        click.echo(click.style("Output Files:", fg="blue", bold=True))
        click.echo(f"Results have been saved to: {output_dir}")
        click.echo()
        
        return True
        
    except Exception as e:
        logger.error(f"Error in batch process: {str(e)}", exc_info=True)
        click.echo(click.style(f"Error in batch process: {str(e)}", fg="red", bold=True))
        return False

@click.command(help="Run the process in batch mode (non-interactive)")
@click.option("--use-db/--use-csv", default=False, help="Use database instead of CSV files")
@click.option("--no-tracking/--enable-tracking", default=False, help='Disable process tracking (reduces overhead)')
@click.option("--algorithm", "-a", default="example_algorithm", help="Select which algorithm to use")
# External call data arguments
@click.option("--current-process-id", type=int, help="Current process ID")
@click.option("--api-proc-id", type=int, help="API process ID")
@click.option("--wfm-proc-id", type=int, help="WFM process ID")
@click.option("--wfm-user", type=str, help="WFM user")
@click.option("--start-date", type=str, help="Start date (YYYY-MM-DD)")
@click.option("--end-date", type=str, help="End date (YYYY-MM-DD)")
@click.option("--wfm-proc-colab", type=str, help="WFM process collaborator")
def batch_process(use_db, no_tracking, algorithm, current_process_id, api_proc_id, wfm_proc_id, wfm_user, start_date, end_date, wfm_proc_colab):
    """
    Batch process run with enhanced user experience (non-interactive)
    """
    # Display header
    click.clear()
    click.echo(click.style(f"=== {PROJECT_NAME} (Batch Mode) ===", fg="green", bold=True))
    click.echo(click.style(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", fg="green"))
    click.echo()
    
    # Display configuration
    click.echo(click.style("Configuration:", fg="blue"))
    click.echo(f"Data source: {'Database' if use_db else 'CSV files'}")
    click.echo(f"Process tracking: {'Disabled' if no_tracking else 'Enabled'}")
    click.echo(f"Algorithm: {algorithm}")
    
    # Display external call data arguments if provided
    external_args = {}
    if current_process_id is not None:
        external_args['current_process_id'] = current_process_id
        click.echo(f"Current Process ID: {current_process_id}")
    if api_proc_id is not None:
        external_args['api_proc_id'] = api_proc_id
        click.echo(f"API Process ID: {api_proc_id}")
    if wfm_proc_id is not None:
        external_args['wfm_proc_id'] = wfm_proc_id
        click.echo(f"WFM Process ID: {wfm_proc_id}")
    if wfm_user is not None:
        external_args['wfm_user'] = wfm_user
        click.echo(f"WFM User: {wfm_user}")
    if start_date is not None:
        external_args['start_date'] = start_date
        click.echo(f"Start Date: {start_date}")
    if end_date is not None:
        external_args['end_date'] = end_date
        click.echo(f"End Date: {end_date}")
    if wfm_proc_colab is not None:
        external_args['wfm_proc_colab'] = wfm_proc_colab
        click.echo(f"WFM Process Colab: {wfm_proc_colab}")
    
    click.echo()
    
    try:
        logger.info("Starting the Batch Process")
        click.echo("Initializing components...")
        
        # Create spinner for initialization
        with click.progressbar(length=100, label="Initializing") as bar:
            # Create and configure components (same as main.py)
            data_manager, process_manager = create_components(
                use_db=use_db, 
                no_tracking=no_tracking, 
                config=CONFIG, 
                project_name=PROJECT_NAME
            )

            # Debug logging for process manager (same as main.py)
            logger.debug("=== DEBUG PROCESS MANAGER ===")
            if process_manager:
                logger.debug(f"ProcessManager type: {type(process_manager)}")
                logger.debug(f"ProcessManager attributes: {[attr for attr in dir(process_manager) if not attr.startswith('_')]}")
                logger.debug(f"Has config: {hasattr(process_manager, 'config')}")
                logger.debug(f"Has core_data: {hasattr(process_manager, 'core_data')}")
                if hasattr(process_manager, 'core_data'):
                    logger.debug(f"Core data type: {type(process_manager.core_data)}")
                    if isinstance(process_manager.core_data, dict):
                        logger.debug(f"Core data keys: {list(process_manager.core_data.keys())}")
            else:
                logger.debug("No process_manager created")
            logger.debug("=== END DEBUG ===")
        
            bar.update(100)
        
        click.echo()
        click.echo(click.style("Components initialized successfully", fg="green"))
        click.echo()
        
        start_time = time.time()
        
        with data_manager:
            # Get external call data from CONFIG and override with command line arguments
            external_call_dict = CONFIG.get('external_call_data', {}).copy()
            
            # Override with command line arguments if provided
            if external_args:
                external_call_dict.update(external_args)
                logger.info(f"Updated external_call_dict with command line arguments: {external_args}")
            
            logger.debug(f"Final external call dict: {external_call_dict}")
            
            # Run the process
            success = run_batch_process(
                data_manager=data_manager, 
                process_manager=process_manager,
                algorithm=algorithm,
                external_call_dict=external_call_dict
            )

            # Log final status
            if success:
                logger.info("Process completed successfully")
                click.echo(click.style("✓ Process completed successfully", fg="green", bold=True))
            else:
                logger.warning("Process completed with errors")
                click.echo(click.style("⚠ Process completed with errors", fg="yellow", bold=True))
                return 1
                
        # Display execution time
        execution_time = time.time() - start_time
        click.echo(f"Total execution time: {execution_time:.2f} seconds")
        
        return 0

    except Exception as e:
        logger.error(f"Process failed: {str(e)}", exc_info=True)
        click.echo(click.style(f"✘ Process failed: {str(e)}", fg="red", bold=True))
        return 1

if __name__ == "__main__":
    sys.exit(batch_process())