import requests
import os
import pandas as pd
from io import StringIO, BytesIO

from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from custom_s3_hook import CustomS3Hook

class ConvertToParquetOperator(BaseOperator):
    def __init__(self, url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url
        self.custom_s3 = CustomS3Hook(bucket="imdb")
        self.current_time = datetime.now()
        self.current_date = self.current_time.strftime("%Y-%m-%d")

    def execute(self, context):
        self.process_to_parquet()
        return self.url

    def process_to_parquet(self):
        print("Fazendo download do arquivo: " + self.url)
        print(f"downloaded/{self.current_date}/{self.url}")
        csv = self.custom_s3.get_object(key=f"downloaded/{self.current_date}/{self.url}")
        df = pd.read_csv(csv, on_bad_lines='skip')
        print(os.curdir)
        # if 'isOriginalTitle' in df.columns:
        #     df['isOriginalTitle'].apply(lambda x: '0' if x not in ('0',0,1,'1') else x)
        # print(df['isOriginalTitle'].value_counts())
        # df.to_csv('validar dados.csv')
        csv_buffer = BytesIO()
        df.to_parquet(csv_buffer) 
        self.custom_s3.put_object(key=f"datalake/{self.current_date}/{self.url.replace('.csv', '.parquet')}", buffer=csv_buffer.getvalue())

