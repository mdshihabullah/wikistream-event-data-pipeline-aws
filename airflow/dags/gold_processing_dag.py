"""
Gold Layer Processing DAG
=========================
Airflow DAG that orchestrates Gold layer batch processing.

Schedule: Every 5 minutes
SLA: ≤5 minutes end-to-end
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
# DAG CONFIGURATION
# =============================================================================

DAG_ID = "wikistream_gold_processing"
SCHEDULE_INTERVAL = "*/5 * * * *"  # Every 5 minutes

# EMR Configuration
EMR_CLUSTER_ID = "{{ var.value.wikistream_emr_cluster_id }}"
SPARK_SCRIPTS_PATH = "s3://{{ var.value.wikistream_scripts_bucket }}/spark/jobs/"

# S3 Tables Configuration
ICEBERG_CATALOG = "s3tablesbucket"
SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"

# Alert Configuration
SNS_TOPIC_ARN = "{{ var.value.wikistream_alerts_sns_arn }}"


# =============================================================================
# DEFAULT ARGUMENTS
# =============================================================================

default_args = {
    "owner": "wikistream",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
    "sla": timedelta(minutes=5),
}


# =============================================================================
# EMR STEP DEFINITIONS
# =============================================================================

def get_gold_processing_step(execution_date: str, processing_window_minutes: int = 10) -> list:
    """Generate EMR step for Gold processing."""
    return [
        {
            "Name": f"Gold Processing - {execution_date}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "--conf", f"spark.sql.catalog.{ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog",
                    "--conf", f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog",
                    "--conf", "spark.sql.adaptive.enabled=true",
                    "--conf", "spark.sql.shuffle.partitions=200",
                    "--conf", "spark.dynamicAllocation.enabled=true",
                    "--conf", "spark.dynamicAllocation.minExecutors=2",
                    "--conf", "spark.dynamicAllocation.maxExecutors=20",
                    "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.0",
                    f"{SPARK_SCRIPTS_PATH}gold_batch_job.py",
                    "--silver-events-table", f"{ICEBERG_CATALOG}.{SILVER_NAMESPACE}.cleaned_events",
                    "--silver-user-activity-table", f"{ICEBERG_CATALOG}.{SILVER_NAMESPACE}.user_activity",
                    "--gold-namespace", GOLD_NAMESPACE,
                    "--catalog", ICEBERG_CATALOG,
                    "--processing-window", str(processing_window_minutes),
                    "--target-date", "{{ ds }}",
                ],
            },
        }
    ]


def get_risk_alerting_step(execution_date: str) -> list:
    """Generate EMR step for risk score alerting."""
    return [
        {
            "Name": f"Risk Alerting - {execution_date}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "--conf", f"spark.sql.catalog.{ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog",
                    "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
                    f"{SPARK_SCRIPTS_PATH}risk_alerting_job.py",
                    "--risk-table", f"{ICEBERG_CATALOG}.{GOLD_NAMESPACE}.risk_scores",
                    "--target-date", "{{ ds }}",
                    "--sns-topic", SNS_TOPIC_ARN,
                ],
            },
        }
    ]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def on_failure_callback(context: Dict[str, Any]):
    """Callback function when task fails."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    
    message = {
        "alert_type": "PIPELINE_FAILURE",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": str(execution_date),
        "error": str(exception),
        "severity": "CRITICAL",
    }
    
    print(f"Gold processing task failed: {message}")


def check_high_risk_alerts(**context) -> None:
    """
    Check for high-risk alerts and send notifications.
    
    This queries the risk_scores table and sends alerts for
    critical/high risk entities.
    """
    # In production, this would query the risk_scores table
    # and send alerts via SNS for critical risks
    
    print(f"Checking risk alerts for {context['ds']}")
    
    # Placeholder - actual implementation would:
    # 1. Query risk_scores where risk_level in ('critical', 'high')
    # 2. Format alert messages with evidence fields
    # 3. Send to SNS topic for Slack notification


def update_dashboard_cache(**context) -> None:
    """
    Trigger dashboard cache refresh for QuickSight.
    
    This ensures dashboards reflect the latest Gold data.
    """
    print(f"Triggering dashboard refresh for {context['ds']}")
    
    # In production, this would:
    # 1. Call QuickSight API to refresh SPICE datasets
    # 2. Update Grafana annotations for processing completion


def record_processing_metrics(**context) -> None:
    """Record processing metrics to CloudWatch."""
    import boto3
    
    cloudwatch = boto3.client("cloudwatch")
    
    # Record processing completion metric
    cloudwatch.put_metric_data(
        Namespace="WikiStream/Pipeline",
        MetricData=[
            {
                "MetricName": "GoldProcessingCompleted",
                "Dimensions": [
                    {"Name": "DAG", "Value": DAG_ID},
                ],
                "Value": 1,
                "Unit": "Count",
            },
        ],
    )
    
    print(f"Gold processing metrics recorded for {context['ds']}")


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Process Silver layer data into Gold layer (aggregated/analytics-ready)",
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["wikistream", "gold", "medallion", "analytics"],
    on_failure_callback=on_failure_callback,
) as dag:
    
    # Task: Wait for Silver processing to complete
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_processing",
        external_dag_id="wikistream_silver_processing",
        external_task_id="record_processing_metrics",
        allowed_states=["success"],
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )
    
    # Task: Submit Gold processing EMR step
    submit_gold_step = EmrAddStepsOperator(
        task_id="submit_gold_processing_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=get_gold_processing_step("{{ ds }}"),
        aws_conn_id="aws_default",
    )
    
    # Task: Wait for Gold processing to complete
    wait_for_gold = EmrStepSensor(
        task_id="wait_for_gold_processing",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_gold_processing_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,  # 10 minutes timeout
    )
    
    # Task: Check and send high-risk alerts
    check_alerts = PythonOperator(
        task_id="check_high_risk_alerts",
        python_callable=check_high_risk_alerts,
        provide_context=True,
    )
    
    # Task: Update dashboard cache
    update_dashboards = PythonOperator(
        task_id="update_dashboard_cache",
        python_callable=update_dashboard_cache,
        provide_context=True,
    )
    
    # Task: Record metrics
    record_metrics = PythonOperator(
        task_id="record_processing_metrics",
        python_callable=record_processing_metrics,
        provide_context=True,
    )
    
    # Task: Alert on failure
    alert_on_failure = SnsPublishOperator(
        task_id="alert_on_failure",
        target_arn=SNS_TOPIC_ARN,
        subject="WikiStream Gold Processing Failed",
        message='{"alert_type": "GOLD_PROCESSING_FAILURE", "dag_run": "{{ run_id }}", "execution_date": "{{ ds }}", "severity": "CRITICAL"}',
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    # Define task dependencies
    wait_for_silver >> submit_gold_step >> wait_for_gold
    wait_for_gold >> [check_alerts, update_dashboards]
    [check_alerts, update_dashboards] >> record_metrics
    wait_for_gold >> alert_on_failure



