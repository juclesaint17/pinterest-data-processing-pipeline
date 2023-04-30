from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()

api_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest Data",
    value_serializer=lambda data: dumps(data).encode("ascii")
)




class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    for data in item:
        api_producer.send(topic="Pinterest_Aicore_Topic",value=data)
    return data

    #data = dict(item)
    #return item



if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
