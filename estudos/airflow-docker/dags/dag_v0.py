"""
Primeira DAG no Airflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  # <--- ALTERADO AQUI
from datetime import datetime, timedelta


with DAG(
    'first_dag',
    description='A simple ETL pipeline example',
    schedule='@daily',
    start_date=datetime(2025, 12, 12),
    catchup=False,

) as dag:

    start = EmptyOperator(task_id='start')  # <--- ALTERADO AQUI

    def print_hello():
        print("Hello World from Airflow DAG!")

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )

    end = EmptyOperator(task_id='end')  # <--- ALTERADO AQUI

    start >> hello_task >> end  # Definindo a ordem das tarefas
