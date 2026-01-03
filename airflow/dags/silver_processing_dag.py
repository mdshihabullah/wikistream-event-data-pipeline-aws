"""
Silver Layer Processing DAG
===========================
Airflow DAG that orchestrates Silver layer batch processing.

Schedule: Every 5 minutes
SLA: 3-5 minutes processing time
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
# DAG CONFIGURATION
# =============================================================================

DAG_ID = "wikistream_silver_processing"
SCHEDULE_INTERVAL = "*/5 * * * *"  # Every 5 minutes

# EMR Configuration
EMR_CLUSTER_ID = "{{ var.value.wikistream_emr_cluster_id }}"
SPARK_SCRIPTS_PATH = "s3://{{ var.value.wikistream_scripts_bucket }}/spark/jobs/"

# S3 Tables Configuration
ICEBERG_CATALOG = "s3tablesbucket"
BRONZE_NAMESPACE = "bronze"
SILVER_NAMESPACE = "silver"
BRONZE_TABLE = "raw_events"

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

def get_silver_processing_step(execution_date: str, processing_window_minutes: int = 10) -> list:
    """Generate EMR step for Silver processing."""
    return [
        {
            "Name": f"Silver Processing - {execution_date}",
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
                    f"{SPARK_SCRIPTS_PATH}silver_batch_job.py",
                    "--bronze-table", f"{ICEBERG_CATALOG}.{BRONZE_NAMESPACE}.{BRONZE_TABLE}",
                    "--silver-namespace", SILVER_NAMESPACE,
                    "--catalog", ICEBERG_CATALOG,
                    "--processing-window", str(processing_window_minutes),
                ],
            },
        }
    ]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def check_bronze_data_availability(**context) -> str:
    """
    Check if there's new data in Bronze layer to process.
    
    Returns:
        Task ID to branch to
    """
    from airflow.providers.amazon.aws.hooks.athena import AthenaHook
    
    # Query Bronze table for recent data
    athena = AthenaHook(aws_conn_id="aws_default")
    
    query = f"""
    SELECT COUNT(*) as record_count
    FROM "{ICEBERG_CATALOG}"."{BRONZE_NAMESPACE}"."{BRONZE_TABLE}"
    WHERE ingested_at >= current_timestamp - interval '10' minute
    """
    
    try:
        # In production, you'd execute this query
        # For now, always proceed with processing
        return "submit_silver_processing_step"
    except Exception as e:
        print(f"Error checking Bronze data: {e}")
        # Continue processing even if check fails
        return "submit_silver_processing_step"


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
        "severity": "HIGH",
    }
    
    # Log the failure
    print(f"Task failed: {message}")


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback when SLA is missed."""
    message = {
        "alert_type": "SLA_MISS",
        "dag_id": dag.dag_id,
        "tasks": [t.task_id for t in task_list],
        "severity": "HIGH",
    }
    print(f"SLA missed: {message}")


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Process Bronze layer data into Silver layer (cleaned/enriched)",
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["wikistream", "silver", "medallion", "etl"],
    on_failure_callback=on_failure_callback,
    sla_miss_callback=on_sla_miss_callback,
) as dag:
    
    # Task: Check if there's data to process
    check_data = BranchPythonOperator(
        task_id="check_bronze_data_availability",
        python_callable=check_bronze_data_availability,
        provide_context=True,
    )
    
    # Task: Submit Silver processing EMR step
    submit_silver_step = EmrAddStepsOperator(
        task_id="submit_silver_processing_step",
        job_flow_id=EMR_CLUSTER_ID,
        steps=get_silver_processing_step("{{ ds }}"),
        aws_conn_id="aws_default",
    )
    
    # Task: Wait for Silver processing to complete
    wait_for_silver = EmrStepSensor(
        task_id="wait_for_silver_processing",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='submit_silver_processing_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600,  # 10 minutes timeout
    )
    
    # Task: Skip processing (no data)
    skip_processing = PythonOperator(
        task_id="skip_processing",
        python_callable=lambda: print("No new Bronze data to process, skipping"),
    )
    
    # Task: Record metrics
    record_metrics = PythonOperator(
        task_id="record_processing_metrics",
        python_callable=lambda **ctx: print(f"Silver processing completed for {ctx['ds']}"),
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    
    # Task: Alert on failure
    alert_on_failure = SnsPublishOperator(
        task_id="alert_on_failure",
        target_arn=SNS_TOPIC_ARN,
        subject="WikiStream Silver Processing Failed",
        message='{"alert_type": "SILVER_PROCESSING_FAILURE", "dag_run": "{{ run_id }}", "execution_date": "{{ ds }}"}',
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    # Define task dependencies
    check_data >> [submit_silver_step, skip_processing]
    submit_silver_step >> wait_for_silver >> record_metrics
    skip_processing >> record_metrics
    [wait_for_silver, skip_processing] >> alert_on_failure



