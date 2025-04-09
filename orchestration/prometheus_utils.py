import os
import time
import uuid
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway

def push_metrics_to_prometheus(metrics, logger):
    """Push metrics directly to Prometheus Pushgateway."""
    PUSHGATEWAY_URL = os.getenv('PUSHGATEWAY_URL', 'http://localhost:9091')
    JOB_NAME = os.getenv('JOB_NAME', 'nersc_transfer')
    INSTANCE_LABEL = os.getenv('INSTANCE_LABEL', 'data_transfer')
    
    try:
        # Create a new registry
        registry = CollectorRegistry()
        
        # Define the required metrics
        # 1. Count of requests - Gauge
        request_counter = Gauge('nersc_transfer_request_count', 
                              'Number of times the flow has been executed',
                              ['execution_id'],
                              registry=registry)
        
        bytes_counter = Gauge('nersc_transfer_total_bytes', 
                              'Number of bytes for all the executed flows',
                              ['execution_id'],
                              registry=registry)
                                 
        # 2. Total bytes transferred - Gauge
        transfer_bytes = Gauge('nersc_transfer_file_bytes', 
                             'Total size of all file transfers to NERSC',
                             ['status'],
                             registry=registry)
        
        # 3. Transfer speed - Gauge
        transfer_speed = Gauge('nersc_transfer_speed_bytes_per_second',
                              'Transfer speed for NERSC file transfers in bytes per second',
                              ['machine'],
                              registry=registry)
                              
        # 4. Transfer time - Gauge
        transfer_time = Gauge('nersc_transfer_time_seconds',
                             'Time taken for NERSC file transfers in seconds',
                             ['machine'],
                             registry=registry)
        
        # Generate a unique execution ID for this transfer
        execution_id = f"exec_{str(uuid.uuid4())}"
        
        # Set the metrics
        request_counter.labels(execution_id=execution_id).set(1)
        bytes_counter.labels(execution_id=execution_id).set(metrics['bytes_transferred'])
        transfer_bytes.labels(status=metrics['status']).set(metrics['bytes_transferred'])
        transfer_time.labels(machine=metrics['status']).set(metrics['duration_seconds'])
        transfer_speed.labels(machine=metrics['status']).set(metrics['transfer_speed'])
        
        # Log metrics for debugging
        logger.info(f"Pushing metrics: bytes_transferred={metrics['bytes_transferred']}")
        logger.info(f"Transfer speed: {metrics['transfer_speed']} bytes/second")
        
        # Push to Pushgateway with error handling
        try:
            push_to_gateway(
                PUSHGATEWAY_URL,
                job=JOB_NAME,
                registry=registry,
                grouping_key={'instance': INSTANCE_LABEL}
            )
            logger.info(f"Successfully pushed metrics to Pushgateway at {PUSHGATEWAY_URL}")
        except Exception as push_error:
            logger.error(f"Error pushing to Pushgateway at {PUSHGATEWAY_URL}: {push_error}")
                    
    except Exception as e:
        logger.error(f"Error preparing metrics for Prometheus: {e}")