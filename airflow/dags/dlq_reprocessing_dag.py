"""
DLQ Reprocessing DAG
====================
Airflow DAG that processes messages from the Dead Letter Queue.

Schedule: Hourly
Purpose: Retry failed messages and analyze error patterns
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import json

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
# DAG CONFIGURATION
# =============================================================================

DAG_ID = "wikistream_dlq_reprocessing"
SCHEDULE_INTERVAL = "0 * * * *"  # Hourly

# EMR Configuration
EMR_CLUSTER_ID = "{{ var.value.wikistream_emr_cluster_id }}"
SPARK_SCRIPTS_PATH = "s3://{{ var.value.wikistream_scripts_bucket }}/spark/jobs/"

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "{{ var.value.wikistream_kafka_bootstrap }}"
DLQ_TOPIC = "wikistream.dlq.events"
RAW_TOPIC = "wikistream.raw.events"

# S3 Tables Configuration
ICEBERG_CATALOG = "s3tablesbucket"
BRONZE_NAMESPACE = "bronze"

# Alert Configuration
SNS_TOPIC_ARN = "{{ var.value.wikistream_alerts_sns_arn }}"

# Thresholds
MAX_RETRY_COUNT = 3
DLQ_ALERT_THRESHOLD = 100  # Alert if more than 100 messages in DLQ


# =============================================================================
# DEFAULT ARGUMENTS
# =============================================================================

default_args = {
    "owner": "wikistream",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


# =============================================================================
# EMR STEP DEFINITIONS
# =============================================================================

def get_dlq_analysis_step(execution_date: str) -> list:
    """Generate EMR step to analyze DLQ messages."""
    return [
        {
            "Name": f"DLQ Analysis - {execution_date}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                    f"{SPARK_SCRIPTS_PATH}dlq_analysis_job.py",
                    "--kafka-bootstrap", KAFKA_BOOTSTRAP_SERVERS,
                    "--dlq-topic", DLQ_TOPIC,
                    "--execution-date", execution_date,
                ],
            },
        }
    ]


def get_dlq_reprocess_step(execution_date: str, max_retries: int = 3) -> list:
    """Generate EMR step to reprocess DLQ messages."""
    return [
        {
            "Name": f"DLQ Reprocess - {execution_date}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "--conf", f"spark.sql.catalog.{ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog",
                    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
                    f"{SPARK_SCRIPTS_PATH}dlq_reprocess_job.py",
                    "--kafka-bootstrap", KAFKA_BOOTSTRAP_SERVERS,
                    "--dlq-topic", DLQ_TOPIC,
                    "--raw-topic", RAW_TOPIC,
                    "--bronze-table", f"{ICEBERG_CATALOG}.{BRONZE_NAMESPACE}.raw_events",
                    "--max-retries", str(max_retries),
                    "--execution-date", execution_date,
                ],
            },
        }
    ]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def check_dlq_volume(**context) -> Dict[str, Any]:
    """
    Check DLQ message volume and categorize error types.
    
    Returns metrics for monitoring.
    """
    # In production, this would:
    # 1. Query Kafka for DLQ topic lag
    # 2. Categorize error types from DLQ messages
    # 3. Return metrics
    
    metrics = {
        "dlq_message_count": 0,
        "error_types": {},
        "oldest_message_age_hours": 0,
        "retry_eligible_count": 0,
    }
    
    # Push metrics to XCom for downstream tasks
    context["task_instance"].xcom_push(key="dlq_metrics", value=metrics)
    
    print(f"DLQ metrics: {metrics}")
    return metrics


def evaluate_dlq_health(**context) -> None:
    """
    Evaluate DLQ health and send alerts if needed.
    """
    import boto3
    
    ti = context["task_instance"]
    metrics = ti.xcom_pull(task_ids="check_dlq_volume", key="dlq_metrics")
    
    if not metrics:
        print("No DLQ metrics available")
        return
    
    dlq_count = metrics.get("dlq_message_count", 0)
    
    # Record CloudWatch metrics
    cloudwatch = boto3.client("cloudwatch")
    
    cloudwatch.put_metric_data(
        Namespace="WikiStream/Pipeline",
        MetricData=[
            {
                "MetricName": "DLQMessageCount",
                "Value": dlq_count,
                "Unit": "Count",
            },
            {
                "MetricName": "DLQRetryEligible",
                "Value": metrics.get("retry_eligible_count", 0),
                "Unit": "Count",
            },
        ],
    )
    
    # Alert if threshold exceeded
    if dlq_count > DLQ_ALERT_THRESHOLD:
        sns = boto3.client("sns")
        
        alert = {
            "alert_type": "DLQ_THRESHOLD_EXCEEDED",
            "severity": "HIGH",
            "dlq_count": dlq_count,
            "threshold": DLQ_ALERT_THRESHOLD,
            "error_breakdown": metrics.get("error_types", {}),
            "oldest_message_hours": metrics.get("oldest_message_age_hours", 0),
            "execution_date": str(context["ds"]),
            "action_required": "Review DLQ messages and error patterns",
        }
        
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="WikiStream DLQ Threshold Exceeded",
            Message=json.dumps(alert, indent=2),
        )
        
        print(f"DLQ threshold alert sent: {dlq_count} messages")


def archive_unrecoverable_messages(**context) -> None:
    """
    Archive messages that have exceeded max retry count.
    
    These are moved to a permanent archive for investigation.
    """
    # In production, this would:
    # 1. Identify messages with retry_count >= MAX_RETRY_COUNT
    # 2. Move them to S3 archive location
    # 3. Remove from DLQ topic
    
    print(f"Archiving unrecoverable messages for {context['ds']}")


def on_failure_callback(context: Dict[str, Any]):
    """Callback function when task fails."""
    print(f"DLQ processing task failed: {context.get('exception')}")


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Process and retry messages from Dead Letter Queue",
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["wikistream", "dlq", "error-handling", "retry"],
    on_failure_callback=on_failure_callback,
) as dag:
    
    # Task: Check DLQ volume and categorize errors
    check_volume = PythonOperator(
        task_id="check_dlq_volume",
        python_callable=check_dlq_volume,
        provide_context=True,
    )
    
    # Task: Analyze DLQ messages (detailed error analysis)
    analyze_dlq = EmrAddStepsOperator(
        task_id="submit_dlq_analysis",
        job_flow_id=EMR_CLUSTER_ID,
        steps=get_dlq_analysis_step("{{ ds }}"),
        aws_conn_id="aws_default",
    )
    
    wait_analysis = EmrStepSensor(
        task_id="wait_dlq_analysis",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_dlq_analysis', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
    )
    
    # Task: Reprocess eligible messages
    reprocess_dlq = EmrAddStepsOperator(
        task_id="submit_dlq_reprocess",
        job_flow_id=EMR_CLUSTER_ID,
        steps=get_dlq_reprocess_step("{{ ds }}", MAX_RETRY_COUNT),
        aws_conn_id="aws_default",
    )
    
    wait_reprocess = EmrStepSensor(
        task_id="wait_dlq_reprocess",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_dlq_reprocess', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
    )
    
    # Task: Archive unrecoverable messages
    archive_messages = PythonOperator(
        task_id="archive_unrecoverable",
        python_callable=archive_unrecoverable_messages,
        provide_context=True,
    )
    
    # Task: Evaluate health and send alerts
    evaluate_health = PythonOperator(
        task_id="evaluate_dlq_health",
        python_callable=evaluate_dlq_health,
        provide_context=True,
    )
    
    # Task: Alert on failure
    alert_on_failure = SnsPublishOperator(
        task_id="alert_on_failure",
        target_arn=SNS_TOPIC_ARN,
        subject="WikiStream DLQ Processing Failed",
        message='{"alert_type": "DLQ_PROCESSING_FAILURE", "dag_run": "{{ run_id }}", "execution_date": "{{ ds }}"}',
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    # Define task dependencies
    check_volume >> analyze_dlq >> wait_analysis
    wait_analysis >> reprocess_dlq >> wait_reprocess
    wait_reprocess >> archive_messages >> evaluate_health
    
    # Failure alerting
    [wait_analysis, wait_reprocess] >> alert_on_failure



