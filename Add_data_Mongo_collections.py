import os
import json
from pymongo import MongoClient
from db_store_from_local_config import *

#MONGO_URI = 'mongodb://localhost:27017/'
MONGO_URI=f"mongodb://{MongoConfigRemoteToLocal.get('hostname')}:{MongoConfigRemoteToLocal.get('port')}/"
def insert_json_to_mongodb(json_file, collection_name, db_name):
    client = MongoClient(MONGO_URI)
    db = client[db_name]
    collection = db[collection_name]

    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            if isinstance(data, list) and not data:  
                print("#############",json.dumps(data))
                collection.insert_one({})
            elif isinstance(data, list):
                print("#############",json.dumps(data))
                collection.insert_many(data)
            else:
                print(f"Invalid JSON data format in file {json_file}. Skipping insertion.")
    except Exception as e:
        print(f"Error inserting JSON file {json_file}: {e}")

    client.close()

def get_json_files(directory):
    json_files = []
    for file in os.listdir(directory):
        print(file)
        if file.endswith('.json'):
            json_files.append(file)
    return json_files

def main(directory, db_name):
    client = MongoClient(MONGO_URI)
    db = client[db_name]
    json_files = get_json_files(directory)
    for json_file in json_files:
        collection_name = os.path.splitext(json_file)[0]
        print("Collections",collection_name)
        insert_json_to_mongodb(os.path.join(directory, json_file), collection_name, db_name)

    client.close()

#directory_path = r"D:\File_Backup\data\abg-finops_backup_20240417_164815"
directory_path = MongoConfigRemoteToLocal.directory_path.value
db_name = MongoConfigRemoteToLocal.mongo_db_name.value
main(directory=directory_path, db_name=db_name)
