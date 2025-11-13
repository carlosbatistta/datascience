from pyspark.sql import SparkSession
import logging as log

CSV_FILE_PATH = "/opt/airflow/dags/data/client_loan.csv"
TABLE_NAME = "data_lake_client_loan"
POSTGRES_CONN_ID = "postgres_default" # Conexão padrão para o banco 'postgres'

def create_table_if_not_exists():
    """Cria a tabela no PostgreSQL se ela ainda não existir."""
    sql_create_table = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        ID VARCHAR (80),
        Customer_ID VARCHAR (80),
        Month VARCHAR (80),
        Name VARCHAR (80),
        Age VARCHAR (80),
        SSN VARCHAR (80),
        Occupation VARCHAR (80),
        Annual_Income VARCHAR (80),
        Monthly_Inhand_Salary VARCHAR (80),
        Num_Bank_Accounts VARCHAR (80),
        Num_Credit_Card VARCHAR (80),
        Interest_Rate VARCHAR (80),
        Num_of_Loan VARCHAR (80),
        Type_of_Loan VARCHAR (80),
        Delay_from_due_date VARCHAR (80),
        Num_of_Delayed_Payment VARCHAR (80),
        Changed_Credit_Limit VARCHAR (80),
        Num_Credit_Inquiries VARCHAR (80),
        Credit_Mix VARCHAR (80),
        Outstanding_Debt VARCHAR (80),
        Credit_Utilization_Ratio VARCHAR (80),
        Credit_History_Age VARCHAR (80),
        Payment_of_Min_Amount VARCHAR (80),
        Total_EMI_per_month VARCHAR (80),
        Amount_invested_monthly VARCHAR (80),
        Payment_Behaviour VARCHAR (80),
        Monthly_Balance VARCHAR (80),
        Credit_Score VARCHAR (80)
    );
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    log.info(f"Criando a tabela {TABLE_NAME}...")
    hook.run(sql_create_table)

def load_csv_to_postgres():
    """Lê o CSV com PySpark e grava no Postgres via JDBC."""
    log.info(f"Iniciando o carregamento de dados do CSV para {TABLE_NAME}")
    
    # 1. Inicializa a sessão Spark
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("AirflowSparkJob") \
        .getOrCreate()
    
    # 2. Lê o CSV usando Spark (inferindo esquema e cabeçalho)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(CSV_FILE_PATH)
    )
    
    # 3. Pega as credenciais da conexão Airflow (via hook)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_connection(POSTGRES_CONN_ID)

    jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
    properties = {
        "user": conn.login,
        "password": conn.password,
        "driver": "org.postgresql.Driver"
    }

    # 4. Escreve o DataFrame no Postgres via JDBC
    (
        df.write
        .jdbc(
            url=jdbc_url,
            table=TABLE_NAME,
            mode="append",  # opções: append, overwrite, ignore, error
            properties=properties
        )
    )

    log.info(f"Total de {df.count()} linhas carregadas em {TABLE_NAME}.")
    spark.stop()