from flask import Flask, jsonify
from pymongo import MongoClient
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}})

client = MongoClient('localhost', 27017)
db = client['big_data']
collection = db['compras']

@app.get('/<cliente>')
def obtener_info(cliente):
    info = collection.find_one({'_id': cliente})
    return jsonify(info)

if __name__ == '__main__':
    app.run(debug=True)