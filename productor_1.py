import datetime
import json
import random
import boto3
import pandas as pd
import csv

STREAM_NAME = "ExampleInputStream"

def get_data(df,i):
  return{'price':""+str(int(df['VALOR'][i])),'date':df['VIGENCIADESDE'][i]}

def generate(stream_name, kinesis_client):
  bucket_name = 'awskinesisdata'
  file_name = 'data_dollar/Tasa_de_cambio.csv'
  s3_client = boto3.client('s3')
  response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
  df = pd.read_csv(response['Body'])
  print(df)
  i=0
  while True:
    if(i>=df.shape[0]):
     i=0
    data = get_data(df,i)
    print(data)
    kinesis_client.put_record(StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey="partitionkey")
    i=i+1

if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis', region_name='us-east-1'))