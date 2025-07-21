#!/usr/bin/env python3
"""Main entry point for the my_new_project project."""

import click
import logging
import sys
import os
import subprocess
import winreg
from pathlib import Path
from datetime import datetime

# Import base_data_project components
from base_data_project.log_config import setup_logger, get_logger
from base_data_project.utils import create_components
from base_data_project.process_management.manager import ProcessManager

# Import project-specific components
from src.config import CONFIG, PROJECT_NAME

# Initialize logger with configuration first
setup_logger(
    project_name=PROJECT_NAME,
    log_level=CONFIG.get('log_level', logging.INFO),
    log_dir=CONFIG.get('log_dir', 'logs'),
    console_output=CONFIG.get('console_output', True)
)

# Then get the logger instance for use throughout the file
logger = get_logger(PROJECT_NAME)

# Import components that might use logging after logger is initialized
from src.services.example_service import AlgoritmoGDService

@click.group()
def cli():
    """Interactive command-line interface for the my_new_project project."""
    pass

@cli.command(help="Run the interactive process")
@click.option("--use-db/--use-csv", prompt="Use database for data storage", default=False, 
              help="Use database instead of CSV files")
@click.option("--no-tracking/--enable-tracking", default=False, 
              help='Disable process tracking (reduces overhead)')
def run_process(use_db, no_tracking):
    """
    Interactive process run with enhanced user experience
    """
    # Display header
    click.clear()
    click.echo(click.style(f"=== {PROJECT_NAME} Interactive Mode ===", fg="green", bold=True))
    click.echo(click.style(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", fg="green"))
    click.echo()
    
    # Display configuration
    click.echo(click.style("Configuration:", fg="blue"))
    click.echo(f"Data source: {'Database' if use_db else 'CSV files'}")
    click.echo(f"Process tracking: {'Disabled' if no_tracking else 'Enabled'}")
    click.echo()
    
    try:
        logger.info("Starting the Interactive Process")
        click.echo("Initializing components...")
        
        # Create spinner for initialization
        with click.progressbar(length=100, label="Initializing") as bar:
            # Create and configure components
            data_manager, process_manager = create_components(
                use_db=use_db, 
                no_tracking=no_tracking, 
                config=CONFIG, 
                project_name=PROJECT_NAME
            )

            # Debug logging for process manager
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
        
        with data_manager:
            external_call_dict = CONFIG.get('external_call_data', {})
            logger.debug(f"External call dict: {external_call_dict}")
            # Create service with data and process managers
            service = AlgoritmoGDService(
                data_manager=data_manager,
                process_manager=process_manager,
                external_call_dict=external_call_dict,
                config=CONFIG,
                project_name=PROJECT_NAME
            )
            
            # Initialize process
            process_id = service.initialize_process(
                "Interactive Process Run", 
                f"Process run on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            
            # Run each stage with user interaction
            stages = ['data_loading', 'processing', 'result_analysis']
            for stage in stages:
                click.echo(click.style(f"Stage: {stage}", fg="blue", bold=True))
                
                # Let user decide whether to run this stage
                if click.confirm(f"Execute {stage} stage?", default=True):
                    success = service.execute_stage(stage)

                    if success:
                        click.echo(click.style(f"✓ {stage} completed successfully", fg="green"))
                    else:
                        click.echo(click.style(f"✘ {stage} failed", fg="red", bold=True))
                        if not click.confirm("Continue despite failure?", default=False):
                            click.echo("Process terminated by user")
                            break
                else:
                    click.echo(f"Skipping {stage} stage")
                
                click.echo()
            
            # Process complete
            click.echo(click.style("Process complete", fg="green", bold=True))
            
            # Get and display process summary
            if process_manager:
                process_summary = service.get_process_summary()
                status_counts = process_summary.get('status_counts', {})
                
                click.echo(click.style("Process Summary:", fg="blue", bold=True))
                click.echo(f"Process ID: {process_id}")
                click.echo(f"Completed stages: {status_counts.get('completed', 0)}")
                click.echo(f"Failed stages: {status_counts.get('failed', 0)}")
                click.echo(f"Skipped stages: {status_counts.get('skipped', 0)}")
                click.echo(f"Overall progress: {process_summary.get('progress', 0) * 100:.1f}%")
                
    except Exception as e:
        logger.error(f"Process failed: {str(e)}", exc_info=True)
        click.echo(click.style(f"✘ Process failed: {str(e)}", fg="red", bold=True))
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(cli())  # Return exit code