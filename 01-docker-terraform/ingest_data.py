#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_zones_data(conn, zones_csv_url):
    csv_name = "zones.csv"
    os.system(f"wget {zones_csv_url} -O {csv_name}")
    df_zones = pd.read_csv(csv_name)
    df_zones.to_sql(name='zones', con=conn, if_exists='replace')

def ingest_trips_data(conn, url, table_name):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    print(df.head())

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=conn, if_exists='replace')
    df.to_sql(name=table_name, con=conn, if_exists='append')

    for chunk in df_iter:
        t_start = time()

        chunk.lpep_pickup_datetime = pd.to_datetime(chunk.lpep_pickup_datetime)
        chunk.lpep_dropoff_datetime = pd.to_datetime(chunk.lpep_dropoff_datetime)

        chunk.to_sql(name=table_name, con=conn, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

    print('Finished ingesting data into the postgres database')


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    zones_csv_url = params.zones_url

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    with engine.begin() as conn:
        ingest_zones_data(conn, zones_csv_url)
        ingest_trips_data(conn, url, table_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the trips csv file')
    parser.add_argument('--zones_url', required=True, help='url of the zones csv file')

    args = parser.parse_args()

    main(args)