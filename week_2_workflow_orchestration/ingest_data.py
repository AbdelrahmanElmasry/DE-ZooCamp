#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow,task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1 ) )
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df

@task(log_prints=True, retries=3)
def ingest_data(params, df):
    user = params['username']
    password = params['password']
    host = params['host'] 
    port = params['port'] 
    db = params['db']
    table_name = params['table_name']
    url = params['url']
    connection_block = SqlAlchemyConnector.load("postgres-connector")

    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()



    # while True: 

    #     try:
    #         t_start = time()
            
    #         df = next(df_iter)

    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists='append')

    #         t_end = time()

    #         print('inserted another chunk, took %.3f second' % (t_end - t_start))

    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break

@flow(name="Ingest flow")
def main_flow():
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    params = {
        'username':"root",
        'password':"root",
        'host':"localhost",
        'port':5432,
        'db':"ny_taxi",
        'table_name':"green_taxi_trips",
        'url':"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    }
    # args = parser.parse_args()
    raw_data = extract_data(params['url'])
    df = transform_data(raw_data)
    ingest_data(params ,df)

if __name__ == '__main__':
    main_flow()

