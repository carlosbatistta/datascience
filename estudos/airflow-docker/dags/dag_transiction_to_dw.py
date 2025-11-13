from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="csv_to_postgres_datalake",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "postgres"],
    description='Executa job PySpark que carrega dados CSV para o Postgres'
) as dag:

    # Tarefa: enviar o script PySpark para execução no Spark
    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/airflow/spark/jobs/transform_to_data_lake.py',  # caminho dentro do container do Airflow
        conn_id='spark_default',
        verbose=True,
    )

    run_spark_job
