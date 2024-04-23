import os
import shutil
import datetime
import os
import shutil

def backup_folder(source_dir, dest_dir):
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        dest_dir = os.path.join(dest_dir, f"backup_{timestamp}")
        os.makedirs(dest_dir, exist_ok=True)
        
        shutil.copytree(source_dir, dest_dir, symlinks=True)
        print("Folder backup successful.")
        
        print("Folder backup successful.")
    except Exception as e:
        print(f"Error occurred during folder backup: {str(e)}")
source_folder = r"C:\Users\user\Downloads\FlowData"
dest_folder = r"D:\backup"
backup_folder(source_folder, dest_folder)





















# Function to backup a folder from source to destination
# def backup_folder(source_dir, dest_dir):
#     try:
#         # Check if destination directory exists, if not, create it
#         if not os.path.exists(dest_dir):
#             os.makedirs(dest_dir)
        
#         # Copy contents of source directory to destination directory
#         for item in os.listdir(source_dir):
#             source_item = os.path.join(source_dir, item)
#             dest_item = os.path.join(dest_dir, item)
#             if os.path.isdir(source_item):
#                 shutil.copytree(source_item, dest_item, symlinks=True)
#             else:
#                 shutil.copy2(source_item, dest_item)
        
#         print("Folder backup successful.")
#     except Exception as e:
#         print(f"Error occurred during folder backup: {str(e)}")

# source_folder = r"C:\Users\user\Downloads\FlowData"

# dest_folder = r"D:\backup"
# backup_folder(source_folder, dest_folder)
'''
# Function to backup MongoDB collections
def backup_mongodb_collections(host, port, db_name, dest_dir):
    try:
        client = MongoClient(host, port)
        db = client[db_name]
        
        # Get list of all collections in the database
        collections = db.list_collection_names()
        
        # Backup each collection as a separate JSON file
        for collection_name in collections:
            collection_data = list(db[collection_name].find())
            backup_file = os.path.join(dest_dir, f"{collection_name}.json")
            with open(backup_file, "w") as f:
                f.write(str(collection_data))
        
        print("MongoDB collections backup successful.")
    except Exception as e:
        print(f"Error occurred during MongoDB collections backup: {str(e)}")

# Define source and destination directories for folder backup
source_folder = "/path/to/source/folder"
dest_folder = "/path/to/destination/folder"

# Define MongoDB connection parameters and destination directory for collections backup
mongo_host = "localhost"
mongo_port = 27017
mongo_db_name = "your_database_name"
mongo_backup_dir = "/path/to/mongodb_backup"
'''
# Perform folder backup


# Perform MongoDB collections backup
#backup_mongodb_collections(mongo_host, mongo_port, mongo_db_name, mongo_backup_dir)
