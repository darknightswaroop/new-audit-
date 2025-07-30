import ray
import json
import jsonschema
import logging
from typing import Dict, Any
from datetime import datetime
import aioboto3
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

@ray.remote
class AuditLoggerActor:
    """
    Ray Actor responsible for auditing and logging events for SALLMA.
    Stores detailed traces in S3 and searchable metadata in Redis.
    Ensures schema validation for all audit logs.
    """

    def __init__(self, s3_bucket: str, schema_path: str, redis_url: str = "redis://localhost"):
        """
        Initialize the AuditLoggerActor.

        Args:
            s3_bucket (str): Name of the S3 bucket for audit logs.
            schema_path (str): Path to the audit log JSON schema.
            redis_url (str): Redis connection URL for metadata storage.
        """
        self.s3_bucket = s3_bucket
        self.schema_path = schema_path
        self.redis_url = redis_url
        self.s3_client = None
        self.redis = None
        self.schema = None
        self._setup()

    def _setup(self):
        """Load schema and initialize S3 and Redis connections."""
        self.current_user = "swaroop-thakare"
        self._load_schema()
        self._initialize_s3()
        self._initialize_redis()

    def _load_schema(self):
        """Load and parse the audit log schema from the given path."""
        try:
            with open(self.schema_path, "r") as f:
                self.schema = json.load(f)
            logger.info("Audit log schema loaded.")
        except Exception as e:
            logger.error(f"Failed to load schema: {e}")
            raise

    def _initialize_s3(self):
        """Initialize the S3 client using aioboto3."""
        try:
            self.s3_client = aioboto3.client("s3", region_name="us-east-1")
        except Exception as e:
            logger.error(f"Failed to initialize S3: {e}")
            self.s3_client = None

    def _initialize_redis(self):
        """Initialize the Redis connection for metadata storage."""
        try:
            self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis = None

    async def log_event(self, audit_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Log a full audit event (schema validated) to S3 and metadata to Redis.

        Args:
            audit_data (Dict[str, Any]): The event data to log.

        Returns:
            Dict[str, Any]: Status, timestamp, and trace_id.
        """
        try:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            trace_id = audit_data.get("txn_id") or f"TX_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
            audit_data.update({
                "timestamp": now,
                "user": self.current_user,
                "trace_id": trace_id
            })
            # Validate audit data against schema before logging
            jsonschema.validate(instance=audit_data, schema=self.schema)

            # Write complete trace to S3 asynchronously
            if self.s3_client:
                s3_key = f"audit_traces/{trace_id}.json"
                async with self.s3_client as s3:
                    await s3.put_object(Bucket=self.s3_bucket, Key=s3_key, Body=json.dumps(audit_data))

            # Write searchable metadata to Redis for fast lookups
            if self.redis:
                await self.redis.set(f"audit:{trace_id}", json.dumps({
                    "txn_id": trace_id,
                    "decision": audit_data.get("decision"),
                    "timestamp": audit_data["timestamp"],
                    "dag_path": audit_data.get("dag_path"),
                }))

            logger.info(f"Successfully logged event: {trace_id}")
            return {
                "status": "success",
                "timestamp": audit_data["timestamp"],
                "trace_id": trace_id
            }

        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Schema validation failed: {e}")
            return {"status": "error", "error": "Schema validation failed", "details": str(e)}
        except Exception as e:
            logger.error(f"Logging failed: {e}")
            raise

    async def fetch_full_audit(self, trace_id: str) -> Dict[str, Any]:
        """
        Retrieve a full audit trace from S3 by its trace ID.

        Args:
            trace_id (str): The trace ID of the audit event.

        Returns:
            Dict[str, Any] or None: The complete audit data if found, else None.
        """
        try:
            s3_key = f"audit_traces/{trace_id}.json"
            async with self.s3_client as s3:
                obj = await s3.get_object(Bucket=self.s3_bucket, Key=s3_key)
                data = await obj['Body'].read()
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to retrieve audit {trace_id}: {e}")
            return None