import json
from pymongo import MongoClient

def read_json_file(file_name):
    with open(file_name, 'r') as f:
        json_data = json.load(f)
    return json_data

def upload_to_monogodb(json_data):
    client = MongoClient('localhost', 27017)
    db = client['big_data']
    collection = db['compras']

    for key, value in json_data.items():
        collection.insert_one({
            '_id': key,
            'compras': value['compras'],
            'recomendaciones': value['recomendaciones']
        })


compras = read_json_file('datos.json')
upload_to_monogodb(compras)