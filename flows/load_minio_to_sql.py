import io
import os
import pandas as pd
import boto3
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger

# ---------- CONFIG ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-csv")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "olist")

POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "olist")

POSTGRES_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)



@task
def list_csv_files() -> list[str]:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=MINIO_PREFIX)

    contents = resp.get("Contents", [])
    csv_keys = [obj["Key"] for obj in contents if obj["Key"].lower().endswith(".csv")]
    return csv_keys


@task
def read_csv_from_minio(key: str) -> pd.DataFrame:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
    data = obj["Body"].read()

    df = pd.read_csv(io.BytesIO(data))
    return df


@task
def write_dataframe_to_postgres(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    logger = get_run_logger()
    logger.info("POSTGRES_URL (sanitized): %s", os.getenv("POSTGRES_URL"))
    logger.info("ENTER write_dataframe_to_postgres; df shape=%s", getattr(df, "shape", None))

    engine = create_engine(POSTGRES_URL)
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists=if_exists, index=False)
    logger.info("FINISHED write_dataframe_to_postgres")


@flow(name="load_raw_minio_to_postgres")
def load_minio_csvs_to_postgres():
    csv_keys = list_csv_files()
    print(csv_keys)

    for key in csv_keys:
        base_name = key.split("/")[-1]
        table_name = base_name.rsplit(".", 1)[0]

        # run tasks synchronously
        df_future = read_csv_from_minio(key)
        write_dataframe_to_postgres(df_future, table_name)


if __name__ == "__main__":
    load_minio_csvs_to_postgres()