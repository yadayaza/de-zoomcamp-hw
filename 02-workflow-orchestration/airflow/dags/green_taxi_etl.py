import os
import re

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

import sqlalchemy
from sqlalchemy import create_engine

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException



dataset_files = ["https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz",
                 "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz",
                 "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz"]

dataset_file = "output.csv"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'RatecodeID': pd.Int64Dtype(),
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'ehail_fee':  float,
        'improvement_surcharge': float,
        'total_amount': float,
        'payment_type': pd.Int64Dtype(),
        'trip_type': pd.Int64Dtype(),
        'congestion_surcharge': float 
}
parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

def load_data(dataset_urls):
    df_list = []

    for url in dataset_urls:
        df = pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        df_list.append(df)
    
    df = pd.concat(df_list, ignore_index=True)
    print(f"Shape of loaded data: {df.shape}")

    df.to_csv(f"{path_to_local_home}/{dataset_file}")

def to_snake_case(name):
    snake_case_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    return snake_case_name

def transform_data(src_file):
    df = pd.read_csv(src_file, dtype=taxi_dtypes, parse_dates=parse_dates)
    
    # Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    df = df[df['passenger_count'] > 0]
    df = df[df['trip_distance'] > 0]
    
    print(f"Dataset shape after filtering: {df.shape}")

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    df.columns = [to_snake_case(col) for col in df.columns]
    print(f"Columns after renaming: {df.columns}")
    
    # Add three assertions:
    # vendor_id is one of the existing values in the column (currently)
    valid_vendor_id_values = df['vendor_id'].unique().tolist()
    print(f"Unique vendor_id values: {valid_vendor_id_values}")

    if ~df['vendor_id'].isin(valid_vendor_id_values).all():
        raise AirflowFailException("vendor_id is not one of existing values, transform operation failed.")

    # passenger_count is greater than 0
    if df['passenger_count'].isin([0]).any():
        raise AirflowFailException("Passenger count < 1, transform operation failed.")
    
    # trip_distance is greater than 0
    if df['trip_distance'].isin([0]).any():
        raise AirflowFailException("Trip distance <= 0, transform operation failed.")

    df.to_csv(f"{path_to_local_home}/{dataset_file}")


def upload_to_gcs_partioned(src_file):
    bucket_name = os.environ.get("GCP_GCS_BUCKET")
    project_id = os.environ.get("GCP_PROJECT_ID")
    table_name = "green_taxi_data"
    root_path = f'{bucket_name}/{table_name}'

    table = pa.Table.from_pandas(pd.read_csv(src_file, dtype=taxi_dtypes, parse_dates=parse_dates))

    gcs = pa.fs.GcsFileSystem(project_id=project_id)

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )

def upload_to_postgres(src_file, table_name, schema):
    user = os.environ.get("LOCAL_POSTGRES_USER")
    password = os.environ.get("LOCAL_POSTGRES_PASSWORD")
    host = os.environ.get("LOCAL_POSTGRES_HOST")
    port = os.environ.get("LOCAL_POSTGRES_PORT")
    db = os.environ.get("LOCAL_POSTGRES_DB")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    with engine.connect() as conn:
        transaction = conn.begin()

        if not conn.dialect.has_schema(engine, schema):
            conn.execute(sqlalchemy.schema.CreateSchema(schema))
        
        df_iter = pd.read_csv(src_file, dtype=taxi_dtypes, parse_dates=parse_dates, iterator=True, chunksize=100000)

        df = next(df_iter)

        df.head(n=0).to_sql(schema=schema, name=table_name, con=engine, if_exists='replace')
        df.to_sql(schema=schema, name=table_name, con=engine, if_exists='append')

        for chunk in df_iter:
            chunk.to_sql(schema=schema, name=table_name, con=engine, if_exists='append')

        transaction.commit()


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="green_taxi_etl",
    schedule_interval="0 5 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data,
        op_kwargs={
            "dataset_urls": dataset_files,
        },
    )

    transform_data_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_data,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    upload_to_gcs_partioned_task = PythonOperator(
        task_id="upload_to_gcs_partioned_task",
        python_callable=upload_to_gcs_partioned,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    upload_to_postgres_task = PythonOperator(
        task_id="upload_to_postgres_task",
        python_callable=upload_to_postgres,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
            "table_name": "green_taxi",
            "schema": "mage",
        },
    )

    remove_file_task = BashOperator(
        task_id="remove_file_task",
        bash_command=f"rm {path_to_local_home}/{dataset_file}"
    )


    load_data_task >> transform_data_task
    transform_data_task >> upload_to_gcs_partioned_task >> remove_file_task
    transform_data_task >> upload_to_postgres_task >> remove_file_task
