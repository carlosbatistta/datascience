from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from typing import Dict

# Configuração das conexões (hostnames dos containers)
SOURCE_CONN: Dict[str, object] = {
    "host": "postgres_source",
    "port": 5432,
    "database": "loan",
    "user": "postgres",
    "password": "1234"
}

TARGET_CONN: Dict[str, object] = {
    "host": "postgres_dw",
    "port": 5432,
    "database": "loan_dw",
    "user": "postgres",
    "password": "1234"
}

def extract_and_load(**kwargs):
    import pandas as pd
    from sqlalchemy import create_engine

    src_engine = create_engine(
        f"postgresql+psycopg2://{SOURCE_CONN['user']}:{SOURCE_CONN['password']}@"
        f"{SOURCE_CONN['host']}:{SOURCE_CONN['port']}/{SOURCE_CONN['database']}"
    )
    tgt_engine = create_engine(
        f"postgresql+psycopg2://{TARGET_CONN['user']}:{TARGET_CONN['password']}@"
        f"{TARGET_CONN['host']}:{TARGET_CONN['port']}/{TARGET_CONN['database']}"
    )

    query = "SELECT * FROM client_db;"
    df = pd.read_sql(query, src_engine)

    df.to_sql("raw_clients", tgt_engine, if_exists="replace", index=False)
    print(f"Extração concluída: {len(df)} registros copiados.")


with DAG(
    dag_id="extract_postgres_to_postgres",
    start_date=datetime(2025, 10, 28),
    schedule="@daily",
    catchup=False,
    tags=["extrair", "depositar", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_load_clients",
        python_callable=extract_and_load
    )