import ray
import json
import jsonschema
import logging
from typing import Dict, Any
from datetime import datetime
import elasticsearch
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

@ray.remote(name="AuditLogger")
class AuditLoggerActor:
    """
    Ray Actor for audit logging with schema validation and persistent storage
    Current Context:
    - Timestamp: 2025-07-27 18:11:32
    - User: swaroop-thakare
    """
    
    def __init__(self):
        self.current_time = "2025-07-27 18:11:32"
        self.current_user = "swaroop-thakare"
        self._load_schema()
        self._initialize_storage()

    def _load_schema(self):
        """Load and parse the audit log schema"""
        try:
            with open("docs/audit_log_schema.json", "r") as f:
                self.schema = json.load(f)
            logger.info("Schema loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load schema: {e}")
            raise

    def _initialize_storage(self):
        """Initialize Elasticsearch client"""
        try:
            self.es = elasticsearch.Elasticsearch([{'host': 'localhost', 'port': 9200}])
            self.index = 'sallma_audit_logs'
            if not self.es.indices.exists(index=self.index):
                self.es.indices.create(index=self.index)
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def log_event(self, decision_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Log an audit event with schema validation
        
        Args:
            decision_data: Decision data from ControllerAgent
            
        Returns:
            Dict containing logging status and metadata
        """
        try:
            # Add current context
            decision_data.update({
                "timestamp": self.current_time,
                "user": self.current_user
            })

            # Validate against schema
            jsonschema.validate(instance=decision_data, schema=self.schema)

            # Store in Elasticsearch
            response = self.es.index(
                index=self.index,
                body=decision_data,
                id=decision_data["txn_id"]
            )

            logger.info(f"Successfully logged event: {decision_data['txn_id']}")
            
            return {
                "status": "success",
                "timestamp": self.current_time,
                "txn_id": decision_data["txn_id"],
                "storage_id": response["_id"]
            }

        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Schema validation failed: {e}")
            return {
                "status": "error",
                "timestamp": self.current_time,
                "error": "Schema validation failed",
                "details": str(e)
            }

        except Exception as e:
            logger.error(f"Logging failed: {e}")
            raise

    async def get_log(self, txn_id: str) -> Dict[str, Any]:
        """Retrieve a log entry"""
        try:
            response = self.es.get(
                index=self.index,
                id=txn_id
            )
            return response["_source"]
        except Exception as e:
            logger.error(f"Failed to retrieve log {txn_id}: {e}")
            return None