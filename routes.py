#!/usr/bin/env python3
"""API routes for my_new_project."""

import logging
from datetime import datetime
import os
import json

# Import Flask - we need to make this optional since it's not a required dependency
try:
    from flask import Flask, request, jsonify
except ImportError:
    raise ImportError("Flask is required for API routes. Install with: pip install flask")

# Import base_data_project components
from base_data_project.log_config import setup_logger
from base_data_project.utils import create_components

# Import project-specific components
from src.config import CONFIG, PROJECT_NAME
from src.services.example_service import ExampleService

# Set up logger
logger = setup_logger(PROJECT_NAME, log_level=logging.INFO)

# Create Flask app
app = Flask(__name__)

# Create data and process managers
data_manager, process_manager = create_components(
    use_db=CONFIG.get('use_db', False),
    no_tracking=False,
    config=CONFIG,
    project_name=PROJECT_NAME
)

# Create service
service = ExampleService(
    data_manager=data_manager,
    process_manager=process_manager
)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'project': PROJECT_NAME
    })

@app.route('/process', methods=['POST'])
def start_process():
    """Start a new process."""
    try:
        # Get request parameters
        params = request.json or {}
        
        # Get algorithm name from request or use default
        algorithm = params.get('algorithm', 'default')
        
        logger.info(f"Starting new process with algorithm: {algorithm}")
        
        # Initialize process
        with data_manager:
            process_id = service.initialize_process(
                "API Process Run",
                f"Process started via API on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            
            # Run each stage
            stages = ['data_loading', 'data_transformation', 'processing', 'result_analysis']
            results = {}
            
            for stage in stages:
                logger.info(f"Executing stage: {stage}")
                
                # Execute stage, using algorithm parameter for processing stage
                if stage == 'processing':
                    success = service.execute_stage(stage, algorithm_name=algorithm, algorithm_params=params.get('parameters'))
                else:
                    success = service.execute_stage(stage)
                
                results[stage] = success
                
                # Stop if stage fails
                if not success:
                    logger.error(f"Stage {stage} failed")
                    break
            
            # Finalize process
            service.finalize_process()
            
            # Get process summary
            summary = service.get_process_summary()
            
            # Prepare response
            response = {
                'process_id': process_id,
                'results': results,
                'status': 'completed' if all(results.values()) else 'failed',
                'completed_stages': len([s for s, r in results.items() if r]),
                'total_stages': len(stages),
                'summary': {
                    'status_counts': summary.get('status_counts', {}),
                    'progress': summary.get('progress', 0)
                }
            }
            
            return jsonify(response)
            
    except Exception as e:
        logger.error(f"Error in process: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/data/<entity>', methods=['GET'])
def get_data(entity):
    """Get data for a specific entity."""
    try:
        # Get query parameters
        limit = request.args.get('limit', type=int)
        
        with data_manager:
            # Load data
            data = data_manager.load_data(
                entity,
                limit=limit
            )
            
            # Convert to list of dictionaries
            records = data.to_dict('records')
            
            return jsonify({
                'entity': entity,
                'count': len(records),
                'data': records
            })
            
    except Exception as e:
        logger.error(f"Error getting data for {entity}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    # Get port from environment variable or use default
    port = int(os.environ.get('PORT', 5000))
    
    # Start Flask app
    logger.info(f"Starting API server on port {port}")
    app.run(host='0.0.0.0', port=port)