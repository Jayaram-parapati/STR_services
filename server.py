from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile,Response,Query
from fastapi.responses import JSONResponse

import uvicorn
from datetime import datetime
import uuid
import pandas as pd
import numpy as np
import sys,json,os
from datetime import datetime
from pymongo import MongoClient

from mongoservice import connect_to_MongoDb
from weekly_report_extraction import Weekly_extraction
from monthly_report_extraction import Monthly_extraction
from s3_service.s3_services import AWS_S3_Service 


weekly_extraction = Weekly_extraction()
monthly_extraction=Monthly_extraction()
aws_boto3 = AWS_S3_Service()

db_connection = connect_to_MongoDb()
files_collection = db_connection["files"]

app = FastAPI(
    title='STR Services'
)

@app.post("/upload_file")
def upload_file(files: list[UploadFile] = File(...)):
    try:
        file_status = []
        for path in files:
            file_content = path.file
            fname = path.filename
            file_name, file_extension = os.path.splitext(fname)
            _ts = datetime.now()
            unique_filename = f"{str(uuid.uuid4())}_{fname}"

            mime_type = aws_boto3.get_mime_type(file_extension)
            filedata = {
                "name": fname,
                "s3_key": unique_filename,
                "date": _ts,
                # "status": "Pending",
                "delete_status":0,
                # "temp_corporation_name": corporation,
                # "temp_str_id":str_id,
                # "extraction_status":True
            }

            if file_extension == '.xlsx' or file_extension == '.xls':
                content_type = mime_type
                S3_file_upload = aws_boto3.upload_to_s3_object(file_content,unique_filename,content_type)
                # print("S3_file_upload------->",S3_file_upload)
                # print("content_type------->",content_type)
                files_collection.insert_one(filedata)

            else:
                file_status.append({'name':fname, 'message':'Invalid file format. Allowed formats are .xlsx, xls', 'status':500})
                continue
    except Exception as e:
        return JSONResponse({"messege": str(e),"status":500})

# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=8000)