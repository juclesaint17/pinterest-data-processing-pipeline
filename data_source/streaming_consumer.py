from kafka import KafkaConsumer
from json import loads



def consume_data():
        
    data_streaming_consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda data: loads(data),
        auto_offset_reset="earliest"
    )
    data_streaming_consumer.subscribe(topics=["Pinterest_Aicore_Topic"])
    for data in data_streaming_consumer:
        print(data)
