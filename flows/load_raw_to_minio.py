import os
from pathlib import Path

import boto3
import botocore
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-csv")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "data/raw/olist")


def make_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def ensure_bucket_exists(s3, bucket: str):
    try:
        s3.create_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise

@task
def list_csv_files(raw_dir: str) -> list[str]:
    raw_path = Path(raw_dir)
    
    print('CSVs:', [str(p) for p in raw_path.glob("*.csv") if p.is_file()])
    return [str(p) for p in raw_path.glob("*.csv") if p.is_file()]


@task
def upload_one_csv(file_path:str, bucket:str, prefix:str = "raw/") -> str:
    s3 = make_minio_client()
    filename = Path(file_path).name
    key = f"{prefix}{filename}"

    ensure_bucket_exists(s3, bucket)
    s3.upload_file(file_path, bucket, key)

    return key


@flow(name="load_raw_csvs_to_minio")
def load_raw_csvs_to_minio(raw_dir: str = RAW_DATA_DIR, bucket: str = MINIO_BUCKET, prefix: str = "raw/"):
    files = list_csv_files(raw_dir)
    if not files:
        print(f"No CSV files found under {raw_dir}")
        return

    results = []
    for f in files:
        # async/parallel in Prefect
        r = upload_one_csv.submit(f, bucket, prefix)
        results.append(r)

    return results
