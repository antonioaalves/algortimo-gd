#!/usr/bin/env python3
"""Batch processing module for my_new_project."""

import time
import os
import click
import sys
from typing import Dict, Any
from datetime import datetime

# Import base_data_project components
from base_data_project.log_config import setup_logger
from base_data_project.utils import create_components

# Import project-specific components
from src.config import CONFIG, PROJECT_NAME
from src.services.algoritmoGDService import ExampleService
from src.models import DataContainer

# Set up logger
logger = setup_logger(PROJECT_NAME, log_level=CONFIG.get('log_level', 'INFO'))

def run_batch_process(data_manager, process_manager, algorithm="example_algorithm"):
    """
    Run the process in batch mode without user interaction.
    
    Args:
        data_manager: Data manager instance
        process_manager: Process manager instance
        algorithm: Name of the algorithm to use
        
    Returns:
        True if successful, False otherwise
    """
    logger.info("Starting batch process")
    
    try:
        # Create the service with data and process managers
        service = ExampleService(
            data_manager=data_manager,
            process_manager=process_manager
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
        
        # Execute the data loading stage
        click.echo(click.style("Stage 1: Loading data...", fg="blue"))
        success = service.execute_stage("data_loading")
        
        if not success:
            click.echo(click.style("✘ Data loading failed", fg="red", bold=True))
            return False
        else:
            click.echo(click.style("✓ Data loading completed successfully", fg="green"))
            click.echo()
        
        # Execute the data transformation stage
        click.echo(click.style("Stage 2: Transforming data...", fg="blue"))
        success = service.execute_stage("data_transformation")
        
        if not success:
            click.echo(click.style("✘ Data transformation failed", fg="red", bold=True))
            return False
        else:
            click.echo(click.style("✓ Data transformation completed successfully", fg="green"))
            click.echo()
            
        # Execute processing using the configured algorithms
        click.echo(click.style("Stage 3: Processing...", fg="blue"))
        
        # Prepare algorithm parameters if needed
        algorithm_params = CONFIG.get('algorithm_defaults', {}).get(algorithm, {})
        
        success = service.execute_stage("processing", algorithm_name=algorithm, algorithm_params=algorithm_params)
        
        if not success:
            click.echo(click.style("✘ Processing failed", fg="red", bold=True))
            return False
        else:
            click.echo(click.style("✓ Processing completed successfully", fg="green"))
            click.echo()
        
        # Execute result analysis
        click.echo(click.style("Stage 4: Analyzing results...", fg="blue"))
        success = service.execute_stage("result_analysis")
        
        if not success:
            click.echo(click.style("✘ Result analysis failed", fg="red", bold=True))
            return False
        else:
            click.echo(click.style("✓ Result analysis completed successfully", fg="green"))
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
def batch_process(use_db, no_tracking, algorithm):
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
    click.echo()
    
    try:
        logger.info("Starting the Batch Process")
        click.echo("Initializing components...")
        
        # Create spinner for initialization
        with click.progressbar(length=100, label="Initializing") as bar:
            # Create and configure components
            data_manager, process_manager = create_components(use_db, no_tracking, CONFIG)
            bar.update(100)
        
        click.echo()
        click.echo(click.style("Components initialized successfully", fg="green"))
        click.echo()
        
        start_time = time.time()
        
        with data_manager:
            # Run the process
            success = run_batch_process(
                data_manager=data_manager, 
                process_manager=process_manager,
                algorithm=algorithm
            )

            # Log final status
            if success:
                logger.info("Process completed successfully")
                click.echo(click.style("\n✓ Process completed successfully", fg="green", bold=True))
            else:
                logger.warning("Process completed with errors")
                click.echo(click.style("\n⚠ Process completed with errors", fg="yellow", bold=True))
                
        # Display execution time
        execution_time = time.time() - start_time
        click.echo(f"Total execution time: {execution_time:.2f} seconds")

    except Exception as e:
        logger.error(f"Process failed: {str(e)}", exc_info=True)
        click.echo(click.style(f"\n✘ Process failed: {str(e)}", fg="red", bold=True))
        sys.exit(1)

if __name__ == "__main__":
    batch_process()