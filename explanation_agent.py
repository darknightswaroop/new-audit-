import ray
from ray import serve
from typing import Dict, Any
import logging
from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate

logger = logging.getLogger(__name__)

@serve.deployment
class ExplanationAgent:
    """
    Ray Serve deployment for generating human-readable explanations
    Current Context:
    - Timestamp: 2025-07-27 18:11:32
    - User: swaroop-thakare
    """
    
    def __init__(self):
        self.current_time = "2025-07-27 18:11:32"
        self.current_user = "swaroop-thakare"
        self.audit_logger = ray.get_actor("AuditLogger")
        self._initialize_llm()

    def _initialize_llm(self):
        """Initialize LangChain and templates"""
        self.llm = OpenAI(temperature=0.3)
        self.prompt = PromptTemplate(
            input_variables=["decision", "rules", "confidence", "votes"],
            template="""
            Explain the following security decision in simple terms:
            
            Decision: {decision}
            Triggered Rules: {rules}
            Confidence Score: {confidence}
            Agent Votes: {votes}
            
            Please provide a clear explanation for why this decision was made:
            """
        )

    async def explain(self, transaction_id: str) -> Dict[str, Any]:
        """
        Generate explanation for a decision
        
        Args:
            transaction_id: Transaction ID to explain
            
        Returns:
            Dict containing explanation and metadata
        """
        try:
            # Get log entry
            log_entry = await self.audit_logger.get_log.remote(transaction_id)
            
            if not log_entry:
                return {
                    "status": "error",
                    "timestamp": self.current_time,
                    "error": f"No log found for transaction {transaction_id}"
                }

            # Format prompt
            prompt_text = self.prompt.format(
                decision=log_entry["decision"],
                rules=", ".join(log_entry["triggered_rules"]),
                confidence=log_entry["confidence_score"],
                votes=log_entry["agent_votes"]
            )

            # Generate explanation
            explanation = await self.llm.agenerate([prompt_text])
            
            return {
                "status": "success",
                "timestamp": self.current_time,
                "user": self.current_user,
                "txn_id": transaction_id,
                "explanation": explanation.generations[0][0].text,
                "confidence": log_entry["confidence_score"]
            }

        except Exception as e:
            logger.error(f"Explanation generation failed: {e}")
            return {
                "status": "error",
                "timestamp": self.current_time,
                "error": str(e)
            }

    @serve.ingress
    async def __call__(self, request) -> Dict[str, Any]:
        """HTTP endpoint for explanation requests"""
        try:
            data = await request.json()
            txn_id = data.get("transaction_id")
            
            if not txn_id:
                return {
                    "status": "error",
                    "timestamp": self.current_time,
                    "error": "Missing transaction_id"
                }

            return await self.explain(txn_id)

        except Exception as e:
            logger.error(f"API request failed: {e}")
            return {
                "status": "error",
                "timestamp": self.current_time,
                "error": str(e)
            }