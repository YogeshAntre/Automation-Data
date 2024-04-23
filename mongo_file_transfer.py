# import datetime
# from pymongo import MongoClient
# import os
# from mongo_config import *
# def backup_mongodb_collections(host, port, db_name):
#     try:
#         client = MongoClient(host, port)
#         db = client[db_name]
#         timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#         backup_folder_name = db_name + '_backup_' + timestamp
#         current_dir = os.getcwd()
#         if not os.path.exists(os.path.join(current_dir,"MOngoBackup")):
#             os.makedirs(os.path.join(current_dir,"MOngoBackup"))
#         if not os.path.exists(os.path.join("MOngoBackup", backup_folder_name)):
#             backup_folder_path = os.makedirs(os.path.join("MOngoBackup", backup_folder_name))
#         backup_folder_path = os.path.join(f"{current_dir}/MOngoBackup", backup_folder_name)
#         collections = db.list_collection_names()
                
#         for collection_name in collections:
#             collection_data = list(db[collection_name].find())
#             # print(collection_data)
#             backup_file = os.path.join(backup_folder_path, f"{collection_name}.json")
#             with open(backup_file, "w") as f:
#                 f.write(str(collection_data))
#             print(backup_file)
#         print("MongoDB collections backup successful.")
#     except Exception as e:
#         print(f"Error occurred during MongoDB collections backup: {str(e)}")

# mongo_host = MongoConfig.mongo_host.value
# mongo_port = MongoConfig.mongo_port.value
# mongo_db_name = MongoConfig.mongo_db_name.value

# backup_mongodb_collections(mongo_host, mongo_port, mongo_db_name)

import datetime
from pymongo import MongoClient
import os
import json
from bson import ObjectId

from mongo_config import *

def backup_mongodb_collections(host, port, db_name):
    try:
        client = MongoClient(host, port)
        db = client[db_name]
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_folder_name = db_name + '_backup_' + timestamp
        current_dir = '/home/revdau/BackupAutomation'  
        backup_folder_path = os.path.join(current_dir, "MongoBackup", backup_folder_name)

        if not os.path.exists(backup_folder_path):
            os.makedirs(backup_folder_path)
        
        collections = db.list_collection_names()
        
        for collection_name in collections:
            collection_data = list(db[collection_name].find())
            backup_file = os.path.join(backup_folder_path, f"{collection_name}.json")
            
            
            for document in collection_data:
                for key, value in document.items():
                    if isinstance(value, ObjectId):
                        document[key] = str(value)
            

            with open(backup_file, "w") as f:
                json.dump(collection_data, f, indent=4)
        
        print("MongoDB collections backup successful.")
    except Exception as e:
        print(f"Error occurred during MongoDB collections backup: {str(e)}")

mongo_host = MongoConfig.mongo_host.value
mongo_port = MongoConfig.mongo_port.value
mongo_db_name = MongoConfig.mongo_db_name.value

backup_mongodb_collections(mongo_host, mongo_port, mongo_db_name)
