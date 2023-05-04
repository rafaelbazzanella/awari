from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# def tarefa_aula_09():

import numpy as np
import pandas as pd
from io import BytesIO
# from pymongo import MongoClient
import boto3
from io import StringIO 
import os

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
# from pyspark.sql.types import ArrayType, DoubleType, BooleanType
# from pyspark.sql.functions import col,array_contains	


# {"url":"http://127.0.0.1:9000","accessKey":"bwCtbfF2J9gGGqlI","secretKey":"8cvdeQ8by2z6SeZGm2jsfYTEqHNi12Wb","api":"s3v4","path":"auto"}
client = boto3.client('s3', 
endpoint_url='http://awari-minio-nginx:9000',
aws_access_key_id='bwCtbfF2J9gGGqlI',
aws_secret_access_key='8cvdeQ8by2z6SeZGm2jsfYTEqHNi12Wb',
aws_session_token=None,
config=boto3.session.Config(signature_version='s3v4'),
verify=False,
region_name='sa-east-1'
)

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

	
def tarefa_aula_09_parte_1():
	estados = pd.read_csv(AIRFLOW_HOME + '/dags/data/exercicios/municipios-estados/csv/estados.csv')
	municipios = pd.read_csv(AIRFLOW_HOME + '/dags/data/exercicios/municipios-estados/csv/municipios.csv')

	_buffer = StringIO()
	estados.to_json(_buffer)
	client.put_object(Body=_buffer.getvalue(), Bucket='tarefaaula09', Key='estados_by_dag.json')


	_buffer = StringIO()
	municipios.to_json(_buffer)
	client.put_object(Body=_buffer.getvalue(), Bucket='tarefaaula09', Key='municipios_by_dag.json')
	print("Done!")

def tarefa_aula_09_parte_2():
	obj = client.get_object(
    Bucket='tarefaaula09', 
    Key=f'estados_by_dag.json'
	    ).get("Body")

	json_estados = pd.read_json(obj)

	obj = client.get_object(
	        Bucket='tarefaaula09', 
	        Key=f'municipios_by_dag.json'
	    ).get("Body")

	json_municipios = pd.read_json(obj)

	_buffer = StringIO()
	json_estados.to_csv(_buffer)
	client.put_object(Body=_buffer.getvalue(), Bucket='tarefaaula09', Key='municipios_by_dag.csv')

	_buffer = StringIO()
	json_municipios.to_csv(_buffer)
	client.put_object(Body=_buffer.getvalue(), Bucket='tarefaaula09', Key='municipios_by_dag.csv')


	print("Done!")

def tarefa_aula_09_parte_3():


	estados = pd.read_csv(AIRFLOW_HOME + '/dags/data/exercicios/municipios-estados/csv/estados.csv')
	municipios = pd.read_csv(AIRFLOW_HOME + '/dags/data/exercicios/municipios-estados/csv/municipios.csv')

	_buffer = BytesIO()
	estados.to_parquet(_buffer)
	client.put_object(Body=_buffer.getvalue(), Bucket='tarefaaula09', Key='estados_by_dag_parquet')

	_buffer = BytesIO()
	municipios.to_parquet(_buffer)
	client.put_object(Body=_buffer.getvalue(), Bucket='tarefaaula09', Key='municipios_by_dag_parquet')




with DAG(dag_id="dag_tarefa_aula_09",
		start_date=datetime(2023,4,10),
		schedule_interval = "@once",
		catchup = False) as dag:

	
	task1 = PythonOperator(
		task_id="task_tarefa_aula_09_parte_1",
		python_callable=tarefa_aula_09_parte_1
		)

	task2 = PythonOperator(
		task_id="task_tarefa_aula_09_parte_2",
		python_callable=tarefa_aula_09_parte_2
		)

	task3 = PythonOperator(
		task_id="task_tarefa_aula_09_parte_3",
		python_callable=tarefa_aula_09_parte_3
		)



task1

task2

# task3
# import airflow