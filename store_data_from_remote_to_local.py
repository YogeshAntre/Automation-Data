import os
import paramiko
import hashlib
from tqdm import tqdm

hostname = "134.119.223.244"
port = 8822
username = "revdau"
password = "RevDau@123"
remote_folder_path = "/home/revdau/BackupAutomation/MongoBackup"
local_directory = r"D:\File_Backup\data"

def replace_single_quotes_with_double_quotes(file_path):
    with open(file_path, 'r') as file:
        data = file.read()
    with open(file_path, 'w') as file:
        file.write(data.replace("'", '"'))

def get_file_hash(file_path):
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            hasher.update(data)
    return hasher.hexdigest()

def download_folder_contents(folder_name, local_directory, sftp):
    remote_path = f"/home/revdau/BackupAutomation/MongoBackup/{folder_name}"
    local_path = os.path.join(local_directory, folder_name)
    os.makedirs(local_path, exist_ok=True)
    files = sftp.listdir(remote_path)
    
   
    if os.path.exists(local_path):
     
        local_files = set(os.listdir(local_path))
        for file in files:
            remote_file = f"{remote_path}/{file}"
            local_file = os.path.join(local_path, file)
            if file not in local_files:
                sftp.get(remote_file, local_file)
                replace_single_quotes_with_double_quotes(local_file)
            else:
                remote_hash = sftp.open(remote_file, "rb").read()
                local_hash = open(local_file, "rb").read()
                if hashlib.sha256(remote_hash).digest() != hashlib.sha256(local_hash).digest():
                    sftp.get(remote_file, local_file)
                    replace_single_quotes_with_double_quotes(local_file)
    else:
        
        for file in files:
            remote_file = f"{remote_path}/{file}"
            local_file = os.path.join(local_path, file)
            sftp.get(remote_file, local_file)

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

def transfer_file(hostname, port, username, password, local_dir, remote_file):
    try:
        client.connect(
            hostname=hostname, username=username, password=password, port=port
        )
        sftp = client.open_sftp()
        files = sftp.listdir(remote_file)
        for folder in tqdm(files, desc="Downloading", unit="folder", colour='green'):
            download_folder_contents(folder, local_directory, sftp)
        sftp.close()
    except Exception as e:
        print(e)
    finally:
        client.close()
transfer_file(
    hostname=hostname,
    port=port,
    username=username,
    password=password,
    local_dir=local_directory,
    remote_file=remote_folder_path,
)
