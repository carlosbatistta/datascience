from __future__ import annotations

import pandas as pd
import io
import tempfile
import logging
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

# Nome do arquivo CSV (crie um arquivo de teste!)
CSV_FILE_PATH = "/opt/airflow/dags/data/usuarios.csv"
TABLE_NAME = "tb_usuarios_etl"
POSTGRES_CONN_ID = "postgres_default" # ID de conexão padrão que aponta para o seu banco 'postgres'

log = logging.getLogger(__name__)

def create_table_if_not_exists():
    """Cria a tabela no PostgreSQL se ela ainda não existir."""
    sql_create_table = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(255) NOT NULL,
        idade INTEGER,
        cidade VARCHAR(100)
    );
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    log.info(f"Criando a tabela {TABLE_NAME}...")
    hook.run(sql_create_table)

def load_csv_to_postgres():
    """Lê o CSV, usa o buffer de memória e executa o COPY via cursor direto."""
    log.info(f"Iniciando o carregamento de dados do CSV para {TABLE_NAME}")
    
    # 1. Obter Conexão e Cursor (Manual)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()  # <--- Obtém a conexão bruta
    cursor = conn.cursor()  # <--- Obtém o cursor
    
    try:
        # 2. Leitura do CSV
        df = pd.read_csv(CSV_FILE_PATH)
        
        # 3. Preparação para o COPY FROM usando um buffer de memória (StringIO)
        buffer = io.StringIO()
        df.to_csv(buffer, sep=',', header=False, index=False)
        buffer.seek(0)
        
        # 4. Executa a função COPY FROM diretamente no cursor
        copy_sql = f"""
            COPY {TABLE_NAME} (nome, idade, cidade) FROM STDIN WITH (FORMAT csv, DELIMITER ',');
        """
        
        # AQUI ESTÁ A CORREÇÃO: Chamamos copy_expert NO CURSOR, não no hook.
        cursor.copy_expert(copy_sql, buffer)
        
        log.info(f"Total de {len(df)} linhas carregadas em {TABLE_NAME}.")
        
        # 5. Commit e Fechamento (Obrigatório ao usar a conexão manual)
        conn.commit()
        
    except Exception as e:
        log.error(f"Erro durante o COPY: {e}")
        conn.rollback()
        raise
    finally:
        # Garante que a conexão seja fechada
        cursor.close()
        conn.close()
        
# Definição da DAG
with DAG(
    dag_id="csv_to_postgres_pipeline",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "postgres"],
    default_args={"owner": "airflow", "retries": 1}
) as dag:
    
    # Tarefa 1: Cria a estrutura da tabela
    create_table = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table_if_not_exists,
    )

    # Tarefa 2: Carrega os dados do CSV para a tabela
    load_data = PythonOperator(
        task_id="load_csv_data",
        python_callable=load_csv_to_postgres,
    )
    
    # Tarefa 3: Confirmação
    check_loaded = BashOperator(
        task_id="check_data_loaded",
        bash_command=f"echo 'Pipeline {TABLE_NAME} concluída com sucesso!'",
    )

    create_table >> load_data >> check_loaded