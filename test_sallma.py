import ray
from sallma import audit_logger, metrics_handler, log_processor, explanation_agent

# Example event data
event_data = {
    "txn_id": "TX_test_001",
    "decision": "approve",
    "agent_votes": {"risk_agent": "approve"},
    "triggered_rules": ["rule_test"],
    "confidence_score": 0.98,
    "rules_version": "1.0",
    "model_version": "1.0",
    "dag_path": "root->risk_agent"
}

# Log an audit event
print("Logging audit event...")
log_result = ray.get(audit_logger.log_event.remote(event_data))
print("Log result:", log_result)

# Record metrics
print("Recording metrics...")
ray.get(metrics_handler.record_decision.remote(event_data))

# Fetch audit log and get explanation
trace_id = log_result["trace_id"]
print("Fetching full audit log...")
audit_data = ray.get(audit_logger.fetch_full_audit.remote(trace_id))
print("Audit data:", audit_data)

print("Generating explanation...")
explanation = ray.get(explanation_agent.generate_explanation.remote(audit_data))
print("Explanation:", explanation)

# Process logs from S3
print("Processing logs with LogProcessor...")
summary = ray.get(log_processor.process_logs.remote(days_back=7))
print("Processing summary:", summary)