import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from covid_ds_convert_to_parquet_operator import ConvertToParquetOperator

URLS_IMDB = {
   'codebook': 'owid-covid-codebook.csv',
   'covid_data': 'owid-covid-data.csv',
}


dag1 =  DAG(dag_id=f"covid_to_parquet_codebook",start_date=datetime(2021,1,1),schedule_interval="@once", catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"covid_to_parquet_download_{URLS_IMDB['codebook']}", url=URLS_IMDB['codebook'],dag=dag1
)

# TASKS
download_task


dag2 =  DAG(dag_id=f"covid_to_parquet_data",start_date=datetime(2021,1,1),schedule_interval="@once", catchup=False)
      
download_task = ConvertToParquetOperator(
   task_id=f"covid_to_parquet_download_{URLS_IMDB['covid_data']}", url=URLS_IMDB['covid_data'],dag=dag2
)

# TASKS
download_task

