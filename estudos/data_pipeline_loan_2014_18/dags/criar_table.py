from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="criar_table",
    start_date=datetime(2025, 10, 28),
    schedule="@daily"
) as dag:
    task1 = PostgresOperator(
        task_id='create_loan_data_table',
        postgres_conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS loan_data_del (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            idade INT
        );
        """  
    )
    task1