from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta

@task(retries= 3)
def fetch(url: str, cache_key_fn= task_input_hash, cache_expiration=timedelta(days=1)) -> pd.DataFrame:
    """Read Taxi data from web into pandas Dataframe"""

    df = pd.read_csv(url)

    return df

@task(log_prints=True)
def clean(df : pd.DataFrame, color: str) -> pd.DataFrame :
    """Fix dtype issues"""
    pickup_column = 'lpep_pickup_datetime' if color == 'green' else 'tpep_pickup_datetime'
    dropoff_column = 'lpep_dropoff_datetime' if color == 'green' else 'tpep_dropoff_datetime'

    df[pickup_column] = pd.to_datetime(df[pickup_column])
    df[dropoff_column] = pd.to_datetime(df[dropoff_column])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
     """Write Dataframe out locally as parquet file"""
     path = Path(f"data/{color}/{dataset_file}.parquet")
     df.to_parquet(f"{Path.cwd()}/{path}", compression= "gzip")

     return path

@task()
def write_gcs(path: Path) -> None :
    """Upload local parquet file to GCS"""
    gcp_bucket_block = GcsBucket.load("gcp-bucket")
    gcp_bucket_block.upload_from_path(from_path=f"{path}", to_path=path)

    return



@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    # color = "yellow"
    # year = 2021
    # month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    df_path = write_local(df_clean, color, dataset_file)
    write_gcs(df_path)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2],
    year: int = 2021,
    color: str = "yellow" 
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == '__main__':
    color="yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(year, months, color)
