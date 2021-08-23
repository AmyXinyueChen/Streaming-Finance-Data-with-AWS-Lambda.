import json
import boto3
import yfinance as yf 

kinesis = boto3.client('kinesis', "us-east-2")


def lambda_handler(event, context):
    tickerlst=["FB","SHOP","BYND","NFLX","PINS","SQ","TTD","OKTA","SNAP","DDOG"]
    
    for ticker in tickerlst:
        
        stock = yf.download(ticker, start="2021-05-11", end="2021-05-12",interval="5m")
        stock.reset_index(inplace=True)
        for index, row in stock.iterrows():
            
            data = {}
            data["high"]=row["High"]
            data["low"]=row["Low"]
            data["ts"]=row["Datetime"].isoformat()
            data["name"]=ticker
            data=json.dumps(data)+"\n"
            print(data)
            kinesis.put_record(
            StreamName="STA9760S2021_project3",
            Data=data,
            PartitionKey="partitionkey")    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello fromf Lambda!')
    }




