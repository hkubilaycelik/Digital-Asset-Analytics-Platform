from __future__ import annotations
import pendulum
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DBT_PROJECT_PATH = Path("/usr/local/airflow/analytics")

# Define the profile mapping for dbt to connect to Snowflake
profile_config = ProfileConfig(
    profile_name = "analytics",
    target_name = "dev",
    profile_mapping = SnowflakeUserPasswordProfileMapping(
        conn_id = "snowflake_default",
        profile_args = {"schema": "PUBLIC"},
    ),
)
# Run dbt in a separate Docker container

operator_args = {
    "image": "ghcr.io/dbt-labs/dbt-snowflake:1.6.6"
}

with DAG(
    dag_id = "daap_elt_pipeline",
    start_date = pendulum.datetime(2025, 6, 18, tz = "Europe/Berlin"),
    schedule = None,
    catchup = False,
    doc_md = """ Digital Asset Analytics Pipeline """,
    tags = ["daap"],
) as dag:

    start = EmptyOperator(task_id = "start")

    # Task to trigger the Databricks notebook tasks
    trigger_databricks_job = DatabricksSubmitRunOperator(
        task_id = "trigger_databricks_job",
        databricks_conn_id = "databricks_default",
        existing_cluster_id = "0614-171939-98v2a2y3",
        notebook_task = {
            "notebook_path": "/Workspace/Users/hkubilayc@gmail.com/digital-asset-analytics-platform_raw-data-processor",
        },
    )

    # Create tasks for `dbt run` and `dbt test`
    dbt_tasks = DbtTaskGroup(
        project_config = ProjectConfig(DBT_PROJECT_PATH),
        profile_config = profile_config,
        
    )

    end = EmptyOperator(task_id="end")

    # Define the final dependency chain
    start >> trigger_databricks_job >> dbt_tasks >> end