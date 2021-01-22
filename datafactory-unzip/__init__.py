import logging
import azure.functions as func

import json
import os
import zipfile
import tempfile
from io import BytesIO, StringIO

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    #get file location data from request body
    if req.method == "GET":
        return func.HttpResponse(
             "This function only handles POST requests",
             status_code=200
        )

    else:
        req_body = req.get_json()
        logging.info(req_body)
        logging.info(req.method)

        #connect to storage account
        connect_str = os.environ["rawtestdata_STORAGE"]
        blob_container = req_body["FileSystem"]

        if req_body["Directory"] == None and req_body["FileName"] != None:
            blob_path_list = [req_body["FileName"]]

        elif req_body["Directory"] != None and req_body["FileName"] == None:
            blob_path_list = [req_body["Directory"]]

        else:
            blob_path_list = [req_body["Directory"], req_body["FileName"]]

        logging.info(blob_path_list)
        #the * here allows os.path.join to use a list basis as a path instead of a pathlike object
        blob_path = os.path.join(*blob_path_list).replace("\\","/")
        logging.info(blob_path)
    
        #connect to storage and download blob stream data
        in_blob = BlobClient.from_connection_string(conn_str=connect_str, container_name=blob_container, blob_name=blob_path)
        download_stream = in_blob.download_blob()
        converted_data = BytesIO(download_stream.readall()) 
        
        #create zipfile object and upload extractred files
        zip_object = zipfile.ZipFile(converted_data, "a")
        with tempfile.TemporaryDirectory() as dir_name:
            for filename in zip_object.namelist():
                directory = os.path.basename(filename)

                #skip directories
                if not directory:
                    continue
                
                zip_object.extract(filename, path=dir_name)
                file_location = os.path.join(dir_name, filename)
                logging.info("Blob path check below")
                logging.info(filename)
                with open(file_location, "rb") as f:
                    if req_body["Directory"] == None:
                        upload_path = filename
                    else:
                        upload_path = os.path.join(req_body["Directory"], filename).replace("\\","/")

                    out_blob = BlobClient.from_connection_string(conn_str=connect_str, container_name=blob_container, blob_name=upload_path)
                    out_blob.upload_blob(f, overwrite=True)
                    
        return json.dumps(req_body)
    