import csv
import json
import boto3
import time
from datetime import datetime

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    event_params = event['Records'][0]
    
    bucket = event_params['s3']['bucket']['name']
    key = event_params['s3']['object']['key']
    
    print("-"*10,"Here","-"*10)
    
    s3_object = s3_resource.Object(bucket, key)
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    
    lines = csv.reader(data)
    print("Line: ", lines)
    headers = next(lines)
    
    print(f"headers:{headers}")
    
    list_data = list(lines)
    print("List Data: ", list_data)
    
    india, us = [], []
    [india.append(int(i[2])) if i[3] == 'India' else us.append(int(i[2])) for i in list_data]
    india_total = sum(india) 
    us_total = sum(us)

    
    print('Toatal India salary spend is: ',india_total)
    print('Total US salary spend is: ', us_total)
    print(f"""Total India, US salary spend is: {sum(india), sum(us)}""")
    
    file_content = f"""Total India, US salary spend is: {sum(india), sum(us)}"""
    if key == 'ns-de-employee.csv':
        s3_client.put_object(Body=file_content, Bucket=bucket, Key='agg')
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('HTTP_200_OK')
    }
