from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Configuração de Log
log = logging.getLogger(__name__)

def greet_with_log(name):
    """Função Python que registra uma mensagem no log."""
    # Esta linha deve aparecer no log da tarefa!
    log.info(f"Olá, {name}! A tarefa Python foi executada com sucesso.")
    print(f"--- Fim da execução da tarefa {name} ---")

# Definição da DAG
with DAG(
    dag_id="teste_log_airflow",
    schedule='@daily',  # Execução manual
    start_date=datetime(2025, 1, 12),
    catchup=False,
    tags=["teste", "log"],
    default_args={
        "owner": "airflow",
        "retries": 0,
    }
) as dag:
    
    # 1. Tarefa Bash
    start_task = BashOperator(
        task_id="start_task",
        bash_command="echo 'Iniciando o teste de log do Airflow...'",
    )

    # 2. Tarefa Python (Onde queremos ver o log)
    python_log_task = PythonOperator(
        task_id="python_log_task",
        python_callable=greet_with_log,
        op_kwargs={"name": "Mundo Airflow"},
    )
    
    # 3. Tarefa de Finalização
    end_task = BashOperator(
        task_id="end_task",
        bash_command="echo 'Teste de log concluído.'",
    )

    # Definição da Ordem
    start_task >> python_log_task >> end_task