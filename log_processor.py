import ray
import logging
import aioboto3
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

@ray.remote
class LogProcessor:
    """
    Ray Actor for processing audit logs.
    Responsibilities:
      - Load logs from S3 for a given time window
      - Segment logs by decision type (approve, block, review, etc.)
      - Prepare data for downstream analytics or ML training (optional)
    """

    def __init__(self, s3_bucket: str, prefix: str = "audit_traces/"):
        """
        Initialize LogProcessor.

        Args:
            s3_bucket (str): Name of the S3 bucket where audit logs are stored.
            prefix (str): S3 prefix (folder) where log files are located.
        """
        self.s3_bucket = s3_bucket
        self.prefix = prefix

    async def load_logs(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Load audit logs from S3 for the last `days_back` days.

        Args:
            days_back (int): Number of days of logs to load (default 7).

        Returns:
            List[Dict[str, Any]]: List of loaded audit log entries.
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days_back)
        logs = []

        try:
            async with aioboto3.client("s3", region_name="us-east-1") as s3:
                paginator = s3.get_paginator('list_objects_v2')
                async for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.prefix):
                    for obj in page.get('Contents', []):
                        key = obj['Key']
                        # Download and parse each log file
                        obj_data = await s3.get_object(Bucket=self.s3_bucket, Key=key)
                        content = await obj_data['Body'].read()
                        try:
                            log_entry = json.loads(content)
                            # Ensure the log is within the date range
                            log_time = datetime.strptime(log_entry["timestamp"], "%Y-%m-%d %H:%M:%S")
                            if start_date <= log_time <= end_date:
                                logs.append(log_entry)
                        except Exception as parse_e:
                            logger.warning(f"Failed to parse log file {key}: {parse_e}")
            logger.info(f"Loaded {len(logs)} logs from S3 between {start_date} and {end_date}.")
            return logs
        except Exception as e:
            logger.error(f"Failed to load logs from S3: {e}")
            return []

    async def segment_by_decision(self, logs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Segment logs by their 'decision' field.

        Args:
            logs (List[Dict[str, Any]]): List of audit log entries.

        Returns:
            Dict[str, List[Dict[str, Any]]]: Mapping from decision to list of logs.
        """
        segments = {}
        for log in logs:
            decision = log.get("decision", "unknown")
            if decision not in segments:
                segments[decision] = []
            segments[decision].append(log)
        logger.info(f"Segmented logs: { {k: len(v) for k,v in segments.items()} }")
        return segments

    async def prepare_training_data(self, logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepare and transform logs for ML training (stub/example).

        Args:
            logs (List[Dict[str, Any]]): List of audit log entries.

        Returns:
            List[Dict[str, Any]]: Processed training data.
        """
        # Example: extract only needed fields for training
        training_data = []
        for log in logs:
            training_data.append({
                "features": {
                    "agent_votes": log.get("agent_votes"),
                    "triggered_rules": log.get("triggered_rules"),
                    "confidence_score": log.get("confidence_score"),
                },
                "label": log.get("decision")
            })
        logger.info(f"Prepared {len(training_data)} training samples.")
        return training_data

    async def process_logs(self, days_back: int = 7) -> Dict[str, Any]:
        """
        End-to-end log processing pipeline:
            - Loads logs from S3
            - Segments by decision
            - Optionally prepares training data

        Args:
            days_back (int): How many days back to process.

        Returns:
            Dict[str, Any]: Summary of the processing.
        """
        logs = await self.load_logs(days_back)
        segments = await self.segment_by_decision(logs)
        training_data = await self.prepare_training_data(logs)
        return {
            "logs_processed": len(logs),
            "segments": {k: len(v) for k, v in segments.items()},
            "training_samples": len(training_data)
        }