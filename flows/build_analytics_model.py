import os
from pathlib import Path

from sqlalchemy import create_engine, text
from prefect import flow, task, get_run_logger


POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "olist")

POSTGRES_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

def run_sql_file(sql_path:Path):
    sql_text = sql_path.read_text()

    # Split on statements on ";"
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    engine = create_engine(POSTGRES_URL)
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))

@task
def run_sql_file_task(sql_path:str):
    run_sql_file(sql_path)

@flow(name="build_analytics_models")
def build_analytics_models(sql_dir:str):
    sql_dir = Path(sql_dir)
    run_sql_file_task(sql_dir / "olist_silver.sql")
    run_sql_file_task(sql_dir / "olist_data_quality.sql") # in silver
    run_sql_file_task(sql_dir / "olist_gold.sql")

if __name__ == "__main__":
    build_analytics_models("postgres_queries/")
