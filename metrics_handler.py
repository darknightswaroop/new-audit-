import ray
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import Dict, Any
import os

logger = logging.getLogger(__name__)

@ray.remote
class MetricsHandler:
    """
    Ray Actor for storing audit metrics in Parquet format for analytics.
    Appends each metric row to a Parquet file for efficient batch processing.
    """

    def __init__(self, parquet_path: str = "metrics/audit_metrics.parquet"):
        """
        Initialize the MetricsHandler.

        Args:
            parquet_path (str): Path to the Parquet file for metric storage.
        """
        self.parquet_path = parquet_path
        os.makedirs(os.path.dirname(self.parquet_path), exist_ok=True)
        if not os.path.exists(self.parquet_path):
            # Initialize empty DataFrame with desired columns and write header
            df = pd.DataFrame(columns=[
                "timestamp", "txn_id", "decision", "confidence_score", "user", "dag_path"
            ])
            table = pa.Table.from_pandas(df)
            pq.write_table(table, self.parquet_path)

    async def record_decision(self, event_data: Dict[str, Any]):
        """
        Record a decision event to the Parquet metrics file.

        Args:
            event_data (Dict[str, Any]): The event data to log as metrics.
        """
        try:
            # Minimal required fields; extend as needed for your metrics
            df = pd.DataFrame([{
                "timestamp": event_data.get("timestamp", datetime.utcnow().isoformat()),
                "txn_id": event_data.get("txn_id"),
                "decision": event_data.get("decision"),
                "confidence_score": event_data.get("confidence_score"),
                "user": event_data.get("user"),
                "dag_path": event_data.get("dag_path"),
            }])
            # Convert to Arrow table
            table = pa.Table.from_pandas(df)
            # Append to Parquet file using append mode
            with pq.ParquetWriter(self.parquet_path, table.schema, use_dictionary=True, compression='snappy') as writer:
                writer.write_table(table)
        except Exception as e:
            logger.error(f"Failed to write metrics to Parquet: {e}")