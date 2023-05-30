import boto3
import numpy as np
import json

STREAM_NAME = "ExampleInputStream"
SYMBOLS = ["AAPL", "AMZN", "MSFT", "INTC", "TBV"]
list_prices=[]
def process_records(records):
  for record in records:
    data = json.loads(record['Data'])
    print(data)
    list_prices.append((data['price'],data['date']))
    m_movil = f_i = f_s = 0
    ##El valor promedio se toma con una medida de tiempo 
    if(len(list_prices)>=3):
      price=[int(v1) for v1,v2 in list_prices[-3:]]
      m_movil=np.mean(price)
      desv=np.std(price)
      f_i,f_s=m_movil-(desv*2),m_movil+(desv*2)
    output={'Price':data['price'],'Media_Movil':m_movil,'Franja_I':f_i,'Franja_S':f_s,'Alert':str(int(data['price'])<f_s)}
    send_to_output_stream(output)
    if(output['Alert']=='True'):
      print("_____________________________ALERTA__________________________________\n El valor del dolar sobrepasó la franja superior>")
def send_to_output_stream(output):
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    kinesis_client.put_record(
        StreamName='ExampleOutputStream',
        Data=json.dumps(output),
        PartitionKey='partitionkey')

def consume(stream_name):
  kinesis_client = boto3.client('kinesis', region_name='us-east-1')
  response = kinesis_client.describe_stream(StreamName=stream_name)
  shards = response['StreamDescription']['Shards']
  shard_id = list(shards[-1:][0].values())[0]
  shard_iterator = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='LATEST'
  )['ShardIterator']
  while True:
    records_response = kinesis_client.get_records(ShardIterator=shard_iterator,Limit=100)
    records = records_response['Records']
    if records:
      process_records(records)
    shard_iterator = records_response['NextShardIterator']

if __name__ == '__main__':
  consume(STREAM_NAME)
