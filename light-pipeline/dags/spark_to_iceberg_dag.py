from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os
sys.path.append('/opt/airflow/jobs')
from bronze import execute_bronze

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

# All Spark dependencies are defined here
spark_packages = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

with DAG(
    dag_id="spark_load_to_iceberg",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Iniciando pipeline..."),
    )

    Bronze = PythonOperator(
        task_id="run_bronze",
        python_callable=execute_bronze
    )

    Silver = SparkSubmitOperator(
        task_id="spark_submit_job_silver",
        application="/opt/airflow/jobs/silver.py",
        conn_id="spark_default",
        deploy_mode="client",
        packages=spark_packages,
        verbose=True
    )

    Gold = SparkSubmitOperator(
        task_id="spark_submit_job_gold",
        application="/opt/airflow/jobs/gold.py",
        conn_id="spark_default",
        deploy_mode="client",
        packages=spark_packages,
        verbose=True
    )    

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Pipeline completado exitosamente"),
    )

    start >> Bronze >>  Silver >> Gold>> end