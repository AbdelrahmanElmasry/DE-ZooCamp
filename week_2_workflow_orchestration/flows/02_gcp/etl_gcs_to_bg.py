from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""

    gcs_path = f"de_data_lake_ny-rides-amer/prefect-etl/data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcp_bucket_block = GcsBucket.load("gcp-bucket")
    gcp_bucket_block.get_directory(from_path=gcs_path, local_path=f"./data")

    return Path(f"./data/{color}/{color}_tripdata_{year}-{month:02}.parquet")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data Cleaning"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count : {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace = True)
    print(f"post: missing passenger count : {df['passenger_count'].isna().sum()}")

    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write to Big query"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="trips_data_all.yellow_rides",
        project_id="ny-rides-amer",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etc_gcs_to_bg():
    """Main ETL flow to load data  into BigQuery"""

    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

if __name__ == '__main__':
    etc_gcs_to_bg()
