import ray
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import yaml
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

@ray.remote
class MetricsHandler:
    """Handles metrics collection and reporting for the audit system"""

    def __init__(self):
        """Initialize metrics handler with configuration"""
        self.registry = CollectorRegistry()
        self.current_time = "2025-07-27 14:34:14"
        self.current_user = "swaroop-thakare"
        self.gauges = {}
        self.load_config()
        self.initialize_metrics()

    def load_config(self):
        """Load metrics configuration"""
        with open("backend/audit/config/metrics_config.yaml", "r") as f:
            self.config = yaml.safe_load(f)["metrics"]

    def initialize_metrics(self):
        """Initialize Prometheus metrics from config"""
        for gauge_config in self.config["gauges"]:
            self.gauges[gauge_config["name"]] = Gauge(
                f'sallma_{gauge_config["name"]}',
                gauge_config["description"],
                gauge_config["labels"],
                registry=self.registry
            )

    async def record_audit_metrics(self, event_data: Dict[str, Any], 
                                 processing_time: float) -> Dict[str, Any]:
        """Record comprehensive audit metrics"""
        try:
            # Record decision metrics
            self.gauges["decision_total"].labels(
                decision_type=event_data["decision"],
                outcome="success",
                user=self.current_user
            ).inc()

            # Record confidence score
            self.gauges["confidence_score"].labels(
                decision_type=event_data["decision"],
                user=self.current_user
            ).set(event_data["confidence_score"])

            # Record agent votes
            for agent, vote in event_data["agent_votes"].items():
                self.gauges["agent_votes"].labels(
                    agent_name=agent,
                    vote_type=vote,
                    user=self.current_user
                ).inc()

            # Record processing time
            self.gauges["processing_time"].labels(
                operation_type="audit_decision",
                user=self.current_user
            ).set(processing_time)

            # Record rules triggered
            self.gauges["rules_triggered"].labels(
                decision_type=event_data["decision"],
                user=self.current_user
            ).set(len(event_data["triggered_rules"]))

            # Push to Prometheus
            self._push_to_prometheus()

            return {
                "status": "success",
                "timestamp": self.current_time,
                "metrics_recorded": list(self.gauges.keys())
            }

        except Exception as e:
            logger.error(f"Metrics recording failed: {str(e)}")
            return {
                "status": "error",
                "timestamp": self.current_time,
                "error": str(e)
            }

    def _push_to_prometheus(self):
        """Push metrics to Prometheus gateway"""
        try:
            push_to_gateway(
                self.config["prometheus"]["gateway"],
                job=f'{self.config["prometheus"]["job_prefix"]}_{self.current_user}',
                registry=self.registry
            )
        except Exception as e:
            logger.error(f"Failed to push to Prometheus: {str(e)}")
            raise