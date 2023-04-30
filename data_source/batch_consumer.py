from kafka import KafkaConsumer
from json import loads
import json
import boto3



def consume_data():

    collected_data_storage = []

    data_batch_consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda data: loads(data),
        auto_offset_reset="earliest"
    )
    data_batch_consumer.subscribe(topics=["Pinterest_Aicore_Topic"])
    for data in data_batch_consumer:
        collected_data = data.value
        #print(collected_data)
        collected_data_storage.append(collected_data)
        print(collected_data_storage)
    
    
        #print(type(collected_data))
        #print(collected_data)
    
        with open("received_data.json", "w") as write_file:
            json.dump(collected_data_storage, write_file,indent=4,separators=(": ", " "), sort_keys=True)
            print("Writing Data to a JSON file...")
    





    '''
    with open("received_data.json", 'w') as f:
        for data in data_batch_consumer:
            records_list = data.value
            records=json.dumps(records_list,f)

        print(records)
    '''

def display_buckets():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        print(bucket.name)

def upload_to_s3():
    s3 = boto3.resource('s3')
    with open('received_data.json', 'rb') as file_tosend:
        s3.Bucket('aicorepinterestbucket').put_object(Key='received_data.json',Body=file_tosend)

def download_from_s3():
    s3 = boto3.client('s3')
    with open('s3_download_file.json', 'wb') as file_downloaded:
        s3.download_fileobj('aicorepinterestbucket','received_data.json',file_downloaded)






consume_data()
#display_buckets()
#upload_to_s3()
#download_from_s3()