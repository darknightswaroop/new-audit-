import ray
import boto3
import json
import logging
import yaml
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@ray.remote
class LogProcessor:
    """
    Processes audit logs for analysis and metrics generation
    Handles log segmentation, training data preparation, and metrics updates
    """
    
    def __init__(self):
        # Set current context
        self.current_time = "2025-07-27 14:36:29"
        self.current_user = "swaroop-thakare"
        
        # Load configurations
        self._load_configs()
        
        # Initialize components
        self.s3_client = boto3.client('s3')
        self.registry = CollectorRegistry()
        self.initialize_metrics()

    def _load_configs(self):
        """Load necessary configurations"""
        try:
            with open("configs/global_config.yaml", "r") as f:
                self.config = yaml.safe_load(f)
            
            self.bucket = self.config["audit"]["bucket"]
            self.prefix = self.config["audit"]["prefix"]
            
        except Exception as e:
            logger.error(f"Configuration loading failed: {str(e)}")
            raise

    def initialize_metrics(self):
        """Initialize processing metrics"""
        self.processing_metrics = {
            "logs_processed": Gauge(
                'audit_logs_processed',
                'Number of logs processed',
                ['processor_type', 'user'],
                registry=self.registry
            ),
            "processing_duration": Gauge(
                'audit_processing_duration',
                'Log processing duration in seconds',
                ['operation_type', 'user'],
                registry=self.registry
            )
        }

    async def process_logs(self, days_back: int = 7) -> Dict:
        """Main processing function for audit logs"""
        start_time = datetime.utcnow()
        
        try:
            # Load and process logs
            logs = await self.load_logs(days_back)
            
            # Process in parallel
            segmented_ref = self.segment_by_decision.remote(logs)
            training_ref = self.prepare_training_data.remote(logs)
            
            # Wait for results
            segmented_logs, _ = await ray.get([segmented_ref, training_ref])
            
            # Update metrics
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            await self.update_metrics(segmented_logs, processing_time)
            
            return {
                "status": "success",
                "timestamp": self.current_time,
                "logs_processed": len(logs),
                "segments": {k: len(v) for k, v in segmented_logs.items()},
                "processing_time": processing_time
            }
            
        except Exception as e:
            logger.error(f"Log processing failed: {str(e)}")
            return {
                "status": "error",
                "timestamp": self.current_time,
                "error": str(e)
            }

    @ray.method(num_returns=1)
    async def load_logs(self, days_back: int) -> List[Dict]:
        """Load audit logs from S3"""
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days_back)
            
            logs = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
                for obj in page.get('Contents', []):
                    # Get object metadata
                    response = self.s3_client.get_object(
                        Bucket=self.bucket,
                        Key=obj['Key']
                    )
                    
                    log_entry = json.loads(response['Body'].read().decode('utf-8'))
                    log_date = datetime.strptime(
                        log_entry["timestamp"], 
                        "%Y-%m-%d %H:%M:%S"
                    )
                    
                    if start_date <= log_date <= end_date:
                        logs.append(log_entry)
            
            return logs
            
        except Exception as e:
            logger.error(f"Log loading failed: {str(e)}")
            raise

    @ray.method(num_returns=1)
    async def segment_by_decision(self, logs: List[Dict]) -> Dict[str, List[Dict]]:
        """Segment logs by decision type"""
        segments = {
            "approve": [],
            "block": [],
            "review": []
        }
        
        for log in logs:
            decision = log["decision"]
            if decision in segments:
                segments[decision].append(log)
        
        return segments

    @ray.method(num_returns=1)
    async def prepare_training_data(self, logs: List[Dict]):
        """Prepare and save training data"""
        try:
            df = pd.DataFrame(logs)
            
            # Add metadata
            df['processed_at'] = self.current_time
            df['processed_by'] = self.current_user
            
            # Save to training data directory
            output_path = f"reinforcement/training_data/audit_training_{self.current_time}.csv"
            df.to_csv(output_path, index=False)
            
            logger.info(f"Training data saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Training data preparation failed: {str(e)}")
            raise

    async def update_metrics(self, segmented_logs: Dict[str, List[Dict]], 
                           processing_time: float):
        """Update processing metrics"""
        try:
            # Update logs processed metric
            total_logs = sum(len(logs) for logs in segmented_logs.values())
            self.processing_metrics["logs_processed"].labels(
                processor_type="audit",
                user=self.current_user
            ).set(total_logs)
            
            # Update processing duration metric
            self.processing_metrics["processing_duration"].labels(
                operation_type="full_processing",
                user=self.current_user
            ).set(processing_time)
            
            # Push to Prometheus
            push_to_gateway(
                'localhost:9091',
                job=f'audit_processor_{self.current_user}',
                registry=self.registry
            )
            
        except Exception as e:
            logger.error(f"Metrics update failed: {str(e)}")
            raise