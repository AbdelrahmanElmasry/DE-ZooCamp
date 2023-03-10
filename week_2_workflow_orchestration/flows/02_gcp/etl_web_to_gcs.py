from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries= 3)
def fetch(url: str) -> pd.DataFrame:
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
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    df_path = write_local(df_clean, color, dataset_file)
    write_gcs(df_path)


if __name__ == '__main__':
    etl_web_to_gcs()
