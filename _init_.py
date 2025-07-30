import ray

# Import actor classes from submodules
from sallma.audit.audit_logger_actor import AuditLoggerActor
from sallma.audit.metrics_handler import MetricsHandler
from sallma.audit.log_processor import LogProcessor
from sallma.explainability.explanation_agent import ExplanationAgent

# ---- Configuration ----
AUDIT_S3_BUCKET = "your-s3-bucket"  # Replace with your S3 bucket name
SCHEMA_PATH = "docs/audit_log_schema.json"
REDIS_URL = "redis://localhost"
PARQUET_PATH = "metrics/audit_metrics.parquet"

# ---- Ray Initialization (idempotent) ----
ray.init(ignore_reinit_error=True)

# ---- Instantiate Ray actors as singletons ----
audit_logger = AuditLoggerActor.remote(AUDIT_S3_BUCKET, SCHEMA_PATH, REDIS_URL)
metrics_handler = MetricsHandler.remote(PARQUET_PATH)
log_processor = LogProcessor.remote(AUDIT_S3_BUCKET)
explanation_agent = ExplanationAgent.remote()

# ---- Expose all actors for easy import ----
__all__ = [
    "audit_logger",
    "metrics_handler",
    "log_processor",
    "explanation_agent"
]