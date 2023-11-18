import json
from pymongo import MongoClient


def read_json_file(file_name):
    with open(file_name, 'r') as f:
        json_data = json.load(f)
    return json_data


def upload_to_monogodb(json_als, json_collaborative):
    client = MongoClient('localhost', 27017)
    db = client['big_data']
    collection_als = db['compras_als']
    collection_collaborative = db['compras_collaborative']

    for key, value in json_als.items():
        collection_als.insert_one({
            '_id': key,
            'compras': value['compras'],
            'recomendaciones': value['recomendaciones']
        })

    for key, value in json_collaborative.items():
        collection_collaborative.insert_one({
            '_id': key,
            'compras': value['compras'],
            'recomendaciones': value['recomendaciones']
        })


compras_als = read_json_file('datos_als.json')
compras_collaborative = read_json_file('datos_collaborative.json')
upload_to_monogodb(compras_als, compras_collaborative)
