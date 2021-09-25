import argparse
import logging
import os
from pathlib import Path
from typing import Union

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")

def upload_file(file_path: Union[str, Path], file_name: str, bucket):
    """Upload a file to google cloud storage

    Parameters
    ----------
    file_name: name of file when uploading to google cloud storage
    file_path: local path of the file
    bucket: bucket object 
    
    Returns
    -------
    bool
        True if upload file successfully, otherwise False
    """
    ok = True
    try:
        with open(f"{file_path}", "rb") as f:
            blob = bucket.blob(file_name)
            blob.upload_from_file(f)
    except Exception as err:
        logging.error(err)
        ok = False
    return ok




if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', type=str, 
            default="/Users/minhdang/Desktop/DE/project/scripts/test.txt")
    parser.add_argument('--file_name', type=str,
            default="test.txt")

    args = parser.parse_args()

    file_path = args.file_path     
    file_name = args.file_name


    GCS_SERVICE_ACCOUNT = os.getenv('GCS_SERVICE_ACCOUNT')
    GCS_BUCKET_NAME =  os.getenv('GCS_BUCKET_NAME')
    logging.debug(GCS_SERVICE_ACCOUNT)
    logging.debug(GCS_BUCKET_NAME)

    credentials = service_account.Credentials.from_service_account_file(GCS_SERVICE_ACCOUNT)

    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket(GCS_BUCKET_NAME)

    
    if upload_file(file_path, file_name, bucket):
        logging.debug("Uploaded file successfully")
    else:
        logging.debug("Failed to upload file")
