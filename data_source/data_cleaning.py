import json


def observe_data():
    x=open('received_data.json','r') 
    data = json.loads(x.read())
    print(data)


observe_data()