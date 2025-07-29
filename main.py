import ray
import asyncio
import logging
from typing import Dict, Any
from datetime import datetime
from audit_logger_actor import AuditLoggerActor
from explanation_agent import ExplanationAgent
from log_processor import LogProcessor
from metrics_handler import MetricsHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuditSystem:
    """
    Main class for initializing and managing the audit system
    """
    
    def __init__(self):
        self.current_time = "2025-07-27 14:36:29"
        self.current_user = "swaroop-thakare"
        self.components = {}
        
    async def initialize(self):
        """Initialize all system components"""
        try:
            # Initialize Ray
            ray.init(
                runtime_env={
                    "working_dir": ".",
                    "pip": [
                        "prometheus_client",
                        "langchain",
                        "openai",
                        "boto3",
                        "tenacity",
                        "pandas"
                    ]
                }
            )
            
            # Initialize Ray Serve
            ray.serve.start()
            
            # Create component instances
            self.components = {
                "logger": AuditLoggerActor.remote(),
                "explanation": ExplanationAgent.deploy(),
                "processor": LogProcessor.remote(),
                "metrics": MetricsHandler.remote()
            }
            
            logger.info("Audit system initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"System initialization failed: {str(e)}")
            return False

    async def process_audit_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single audit event through the system"""
        try:
            # Add metadata
            event_data.update({
                "timestamp": self.current_time,
                "user": self.current_user
            })
            
            # Process through components
            log_ref = self.components["logger"].log_decision.remote(event_data)
            explanation_ref = self.components["explanation"].generate_explanation.remote(
                event_data["triggered_rules"]
            )
            
            # Wait for results
            log_result, explanation = await ray.get([log_ref, explanation_ref])
            
            return {
                "status": "success",
                "timestamp": self.current_time,
                "log_result": log_result,
                "explanation": explanation
            }
            
        except Exception as e:
            logger.error(f"Event processing failed: {str(e)}")
            return {
                "status": "error",
                "timestamp": self.current_time,
                "error": str(e)
            }

async def main():
    """Main entry point for the audit system"""
    # Initialize system
    system = AuditSystem()
    initialized = await system.initialize()
    
    if not initialized:
        logger.error("System initialization failed")
        return
    
    # Example audit event
    event_data = {
        "txn_id": f"TX_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        "decision": "approve",
        "agent_votes": {
            "risk_agent": "approve",
            "compliance_agent": "approve"
        },
        "triggered_rules": ["rule1", "rule2"],
        "confidence_score": 0.95,
        "rules_version": "1.0",
        "model_version": "1.0"
    }
    
    # Process event
    result = await system.process_audit_event(event_data)
    logger.info(f"Audit event processed: {result}")

if __name__ == "__main__":
    asyncio.run(main())