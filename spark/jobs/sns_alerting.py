#!/usr/bin/env python3
"""
WikiStream SNS Alerting Module
==============================
Helper functions for sending SNS alerts for pipeline events.

Supported Alerts:
- High risk user detected (risk_score >= 70)
- Data Quality gate failure
- Bronze job health check failure
- Schema drift detected
"""

import logging
import os
import boto3
import json
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


def get_sns_client(region: str = "us-east-1") -> boto3.client:
    """Get SNS client with current region."""
    return boto3.client("sns", region_name=region)


def get_alert_topic_arn() -> Optional[str]:
    """Get SNS topic ARN from environment or configuration."""
    topic_arn = os.environ.get("ALERTS_SNS_TOPIC_ARN")
    
    if topic_arn:
        return topic_arn
    
    # Default format (adjust for your environment)
    aws_account_id = os.environ.get("AWS_ACCOUNT_ID", "")
    environment = os.environ.get("ENVIRONMENT", "dev")
    
    if aws_account_id:
        return f"arn:aws:sns:us-east-1:{aws_account_id}:wikistream-{environment}-alerts"
    
    return None


def format_high_risk_alert(
    entity_id: str,
    risk_score: float,
    risk_level: str,
    evidence: Dict,
    table_name: str = "gold.risk_scores"
) -> Dict:
    """Format SNS alert for high risk user detection."""
    return {
        "alert_type": "HIGH_RISK_USER_DETECTED",
        "severity": "CRITICAL",
        "timestamp": "",
        "table": table_name,
        "details": {
            "entity_id": entity_id,
            "risk_score": risk_score,
            "risk_level": risk_level,
            "evidence": evidence,
            "action_required": "Review user activity and potential policy violations"
        }
    }


def publish_alert(
    alert_data: Dict,
    topic_arn: Optional[str] = None,
    subject_override: Optional[str] = None,
    region: str = "us-east-1"
) -> bool:
    """
    Publish alert to SNS topic.
    
    Args:
        alert_data: Formatted alert dictionary
        topic_arn: SNS topic ARN (optional, will be fetched if not provided)
        subject_override: Custom subject (optional, will be generated if not provided)
        region: AWS region
        
    Returns:
        True if successful, False otherwise
    """
    try:
        sns = get_sns_client(region)
        
        # Get topic ARN if not provided
        if not topic_arn:
            topic_arn = get_alert_topic_arn()
        
        if not topic_arn:
            logger.error("Could not determine SNS topic ARN")
            return False
        
        # Generate subject
        if not subject_override:
            alert_type = alert_data.get("alert_type", "ALERT")
            severity = alert_data.get("severity", "INFO")
            subject = f"WikiStream [{severity}] {alert_type}"
        else:
            subject = subject_override
        
        # Format message
        message = json.dumps(alert_data, indent=2, default=str)
        
        # Publish to SNS
        response = sns.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject
        )
        
        message_id = response.get("MessageId")
        logger.info(f"✅ SNS alert published: {subject} (MessageId: {message_id})")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to publish SNS alert: {e}")
        return False


def publish_high_risk_alerts(
    high_risk_users: List[Dict],
    topic_arn: Optional[str] = None,
    region: str = "us-east-1"
) -> int:
    """
    Publish alerts for high risk users.
    
    Args:
        high_risk_users: List of user dictionaries with risk data
        topic_arn: SNS topic ARN (optional)
        region: AWS region
        
    Returns:
        Number of alerts successfully published
    """
    if not high_risk_users:
        return 0
    
    success_count = 0
    
    # Limit to top 10 alerts to avoid spam
    for user_data in high_risk_users[:10]:
        alert_data = format_high_risk_alert(
            entity_id=user_data.get("entity_id", ""),
            risk_score=user_data.get("risk_score", 0),
            risk_level=user_data.get("risk_level", ""),
            evidence=user_data.get("evidence", {})
        )
        
        subject = f"WikiStream [CRITICAL] HIGH RISK USER: {user_data.get('entity_id', '')}"
        
        if publish_alert(alert_data, topic_arn, subject, region):
            success_count += 1
    
    logger.info(f"✅ Published {success_count}/{len(high_risk_users[:10])} high risk alerts")
    return success_count
