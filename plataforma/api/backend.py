from flask import Flask, jsonify
from pymongo import MongoClient
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}})

client = MongoClient('localhost', 27017)
db = client['big_data']
collection_als = db['compras_als']
collection_collaborative = db['compras_collaborative']


@app.get('/als/<cliente>')
def obtener_info_als(cliente):
    info = collection_als.find_one({'_id': cliente})
    return jsonify(info)


@app.get('/collaborative/<cliente>')
def obtener_info_collaborative(cliente):
    info = collection_collaborative.find_one({'_id': cliente})
    return jsonify(info)


if __name__ == '__main__':
    app.run(debug=True)
