import pandas as pd
from sqlalchemy import create_engine

SOURCE_CONN = {
    "host": "localhost",
    "port": 5432,
    "database": "loan",
    "user": "postgres",
    "password": "1234"
}

try:
    engine = create_engine(
        f"postgresql+psycopg2://{SOURCE_CONN['user']}:{SOURCE_CONN['password']}@"
        f"{SOURCE_CONN['host']}:{SOURCE_CONN['port']}/{SOURCE_CONN['database']}"
    )
    df = pd.read_sql("SELECT 1 AS test_col;", engine)
    print(df)
except Exception as e:
    print("Erro:", e)