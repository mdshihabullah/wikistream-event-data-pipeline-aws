"""
Data Quality DAG
================
Airflow DAG that runs Deequ data quality checks across all medallion layers.

Schedule: Every 15 minutes
Alerts: SNS → Slack on quality failures
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
# DAG CONFIGURATION
# =============================================================================

DAG_ID = "wikistream_data_quality"
SCHEDULE_INTERVAL = "*/15 * * * *"  # Every 15 minutes

# EMR Configuration
EMR_CLUSTER_ID = "{{ var.value.wikistream_emr_cluster_id }}"
SPARK_SCRIPTS_PATH = "s3://{{ var.value.wikistream_scripts_bucket }}/spark/jobs/"

# S3 Tables Configuration
ICEBERG_CATALOG = "s3tablesbucket"
BRONZE_NAMESPACE = "bronze"
SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"

# Results storage
DQ_RESULTS_PATH = "s3://{{ var.value.wikistream_data_bucket }}/data_quality/results/"

# Alert Configuration
SNS_TOPIC_ARN = "{{ var.value.wikistream_alerts_sns_arn }}"

# Quality thresholds
CRITICAL_THRESHOLD = 0.95  # Below this = critical alert
WARNING_THRESHOLD = 0.98   # Below this = warning alert


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
# DATA QUALITY CHECK DEFINITIONS
# =============================================================================

BRONZE_QUALITY_CHECKS = {
    "table": f"{ICEBERG_CATALOG}.{BRONZE_NAMESPACE}.raw_events",
    "checks": [
        {"type": "completeness", "column": "event_id", "threshold": 0.999},
        {"type": "completeness", "column": "event_timestamp", "threshold": 0.999},
        {"type": "completeness", "column": "domain", "threshold": 0.999},
        {"type": "uniqueness", "column": "event_id", "threshold": 0.999},
        {"type": "non_negative", "column": "rc_id", "threshold": 0.999},
        {"type": "is_contained_in", "column": "event_type", "values": ["edit", "new", "log", "categorize"], "threshold": 1.0},
    ],
}

SILVER_QUALITY_CHECKS = {
    "table": f"{ICEBERG_CATALOG}.{SILVER_NAMESPACE}.cleaned_events",
    "checks": [
        {"type": "completeness", "column": "event_id", "threshold": 1.0},
        {"type": "completeness", "column": "event_timestamp", "threshold": 1.0},
        {"type": "completeness", "column": "domain", "threshold": 1.0},
        {"type": "completeness", "column": "is_bot", "threshold": 1.0},
        {"type": "completeness", "column": "is_anonymous", "threshold": 1.0},
        {"type": "uniqueness", "column": "event_id", "threshold": 1.0},
        {"type": "non_negative", "column": "length_delta_abs", "threshold": 0.999},
        {"type": "is_complete", "column": "is_valid", "threshold": 1.0},
    ],
}

GOLD_QUALITY_CHECKS = {
    "table": f"{ICEBERG_CATALOG}.{GOLD_NAMESPACE}.hourly_statistics",
    "checks": [
        {"type": "completeness", "column": "stat_date", "threshold": 1.0},
        {"type": "completeness", "column": "domain", "threshold": 1.0},
        {"type": "completeness", "column": "total_events", "threshold": 1.0},
        {"type": "non_negative", "column": "total_events", "threshold": 1.0},
        {"type": "non_negative", "column": "unique_users", "threshold": 1.0},
        {"type": "in_range", "column": "bot_percentage", "min": 0, "max": 100, "threshold": 1.0},
    ],
}


# =============================================================================
# EMR STEP DEFINITIONS
# =============================================================================

def get_dq_check_step(
    execution_date: str,
    layer: str,
    table: str,
    checks: List[Dict],
    results_path: str,
) -> list:
    """Generate EMR step for Deequ data quality checks."""
    checks_json = json.dumps(checks)
    
    return [
        {
            "Name": f"DQ Check - {layer} - {execution_date}",
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
                    "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,com.amazon.deequ:deequ:2.0.4-spark-3.5",
                    f"{SPARK_SCRIPTS_PATH}data_quality_job.py",
                    "--table", table,
                    "--layer", layer,
                    "--checks", checks_json,
                    "--results-path", f"{results_path}{layer}/{execution_date}/",
                    "--execution-date", execution_date,
                ],
            },
        }
    ]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def evaluate_quality_results(**context) -> str:
    """
    Evaluate data quality results and determine next action.
    
    Returns:
        Task ID to branch to based on results
    """
    # In production, this would:
    # 1. Read DQ results from S3
    # 2. Evaluate against thresholds
    # 3. Return appropriate branch
    
    # Placeholder logic
    ti = context["task_instance"]
    
    # Get results from XCom (in production, read from S3)
    bronze_passed = True
    silver_passed = True
    gold_passed = True
    
    if not (bronze_passed and silver_passed and gold_passed):
        if any([
            # Check for critical failures
            not bronze_passed,
        ]):
            return "send_critical_alert"
        return "send_warning_alert"
    
    return "record_success_metrics"


def format_quality_alert(layer: str, results: Dict, severity: str) -> Dict:
    """Format a quality alert message with evidence fields."""
    return {
        "alert_type": "DATA_QUALITY_FAILURE",
        "severity": severity,
        "layer": layer,
        "timestamp": datetime.utcnow().isoformat(),
        "failed_checks": results.get("failed_checks", []),
        "evidence": {
            "table": results.get("table"),
            "records_checked": results.get("records_checked"),
            "failure_rate": results.get("failure_rate"),
            "sample_failures": results.get("sample_failures", [])[:5],
        },
        "runbook_link": "https://wiki.example.com/wikistream/runbook#data-quality",
    }


def send_quality_alert(severity: str, **context) -> None:
    """Send quality alert to SNS."""
    import boto3
    
    sns = boto3.client("sns")
    
    alert = {
        "alert_type": "DATA_QUALITY_FAILURE",
        "severity": severity,
        "dag_run": context["run_id"],
        "execution_date": str(context["ds"]),
        "layers_checked": ["bronze", "silver", "gold"],
        "action_required": "Review data quality dashboard and failed checks",
    }
    
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"WikiStream Data Quality {severity.upper()}",
        Message=json.dumps(alert, indent=2),
    )
    
    print(f"Sent {severity} alert: {alert}")


def record_quality_metrics(**context) -> None:
    """Record data quality metrics to CloudWatch."""
    import boto3
    
    cloudwatch = boto3.client("cloudwatch")
    
    # Record quality check completion
    cloudwatch.put_metric_data(
        Namespace="WikiStream/DataQuality",
        MetricData=[
            {
                "MetricName": "QualityCheckPassed",
                "Dimensions": [
                    {"Name": "Layer", "Value": "all"},
                ],
                "Value": 1,
                "Unit": "Count",
            },
        ],
    )
    
    print(f"Data quality metrics recorded for {context['ds']}")


def on_failure_callback(context: Dict[str, Any]):
    """Callback function when task fails."""
    print(f"Data quality task failed: {context.get('exception')}")


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Run Deequ data quality checks across Bronze/Silver/Gold layers",
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["wikistream", "data-quality", "deequ", "monitoring"],
    on_failure_callback=on_failure_callback,
) as dag:
    
    # Task: Run Bronze layer quality checks
    bronze_dq_step = EmrAddStepsOperator(
        task_id="submit_bronze_dq_check",
        job_flow_id=EMR_CLUSTER_ID,
        steps=get_dq_check_step(
            "{{ ds }}",
            "bronze",
            BRONZE_QUALITY_CHECKS["table"],
            BRONZE_QUALITY_CHECKS["checks"],
            DQ_RESULTS_PATH,
        ),
        aws_conn_id="aws_default",
    )
    
    wait_bronze_dq = EmrStepSensor(
        task_id="wait_bronze_dq_check",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_bronze_dq_check', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
    )
    
    # Task: Run Silver layer quality checks
    silver_dq_step = EmrAddStepsOperator(
        task_id="submit_silver_dq_check",
        job_flow_id=EMR_CLUSTER_ID,
        steps=get_dq_check_step(
            "{{ ds }}",
            "silver",
            SILVER_QUALITY_CHECKS["table"],
            SILVER_QUALITY_CHECKS["checks"],
            DQ_RESULTS_PATH,
        ),
        aws_conn_id="aws_default",
    )
    
    wait_silver_dq = EmrStepSensor(
        task_id="wait_silver_dq_check",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_silver_dq_check', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
    )
    
    # Task: Run Gold layer quality checks
    gold_dq_step = EmrAddStepsOperator(
        task_id="submit_gold_dq_check",
        job_flow_id=EMR_CLUSTER_ID,
        steps=get_dq_check_step(
            "{{ ds }}",
            "gold",
            GOLD_QUALITY_CHECKS["table"],
            GOLD_QUALITY_CHECKS["checks"],
            DQ_RESULTS_PATH,
        ),
        aws_conn_id="aws_default",
    )
    
    wait_gold_dq = EmrStepSensor(
        task_id="wait_gold_dq_check",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_gold_dq_check', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,
    )
    
    # Task: Evaluate all results
    evaluate_results = BranchPythonOperator(
        task_id="evaluate_quality_results",
        python_callable=evaluate_quality_results,
        provide_context=True,
    )
    
    # Task: Send critical alert
    send_critical = PythonOperator(
        task_id="send_critical_alert",
        python_callable=lambda **ctx: send_quality_alert("critical", **ctx),
        provide_context=True,
    )
    
    # Task: Send warning alert
    send_warning = PythonOperator(
        task_id="send_warning_alert",
        python_callable=lambda **ctx: send_quality_alert("warning", **ctx),
        provide_context=True,
    )
    
    # Task: Record success metrics
    record_success = PythonOperator(
        task_id="record_success_metrics",
        python_callable=record_quality_metrics,
        provide_context=True,
    )
    
    # Task: Final status
    final_status = PythonOperator(
        task_id="final_status",
        python_callable=lambda: print("Data quality check completed"),
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    
    # Define task dependencies
    # Run all DQ checks in parallel
    bronze_dq_step >> wait_bronze_dq
    silver_dq_step >> wait_silver_dq
    gold_dq_step >> wait_gold_dq
    
    # Evaluate after all checks complete
    [wait_bronze_dq, wait_silver_dq, wait_gold_dq] >> evaluate_results
    
    # Branch based on evaluation
    evaluate_results >> [send_critical, send_warning, record_success]
    
    # All paths lead to final status
    [send_critical, send_warning, record_success] >> final_status



