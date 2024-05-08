from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile,Response,Query
from fastapi.responses import JSONResponse
import uvicorn
from datetime import datetime
import uuid
import sys,json,os
from datetime import datetime
import pandas as pd
from io import BytesIO
from typing import Dict
from bson import ObjectId


from mongo_service import connect_to_MongoDb
from extraction_service.weekly_report_extraction import Weekly_extraction
from extraction_service.monthly_report_extraction import Monthly_extraction
from extraction_service.toc_extraction import str_report_type
from s3_service.s3_services import AWS_S3_Service 
from mongo_service.data_endpoints import APIendpoints


weekly_extraction = Weekly_extraction()
monthly_extraction=Monthly_extraction()
aws_boto3 = AWS_S3_Service()
report_type = str_report_type()
api = APIendpoints()

db_connection = connect_to_MongoDb()
db = db_connection.db


app = FastAPI(title='STR Services')

@app.post("/upload_file")
def upload_file(files: list[UploadFile] = File(...),corporation: str = Form(...),str_id: str = Form(...)):
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
                "file_name": fname,
                "s3_key": unique_filename,
                "upload_date": _ts,
                "delete_status":0,
                "corporation": corporation,
                "str_id":str_id,
            }
           
            if file_extension == '.xlsx' or file_extension == '.xls':
                content_type = mime_type
                S3_file_upload = aws_boto3.upload_to_s3_object(file_content,unique_filename,content_type)
                file_body,contentType = aws_boto3.get_file_obj(unique_filename)
                xl = pd.ExcelFile(file_body)          
                sheets = xl.sheet_names
                report = report_type.check_str_report_type(sheets,xl)
                if report["response"]["str_type"] == "Weekly STAR Report":
                    extraction = weekly_extraction.prepare_all_dfs(sheets,xl)
                    if extraction["status"] == 200:
                        filedata.update({"report_type":report["response"]["str_type"].split(" ")[0]})
                        db["Weekly_uploads"].insert_one(filedata)
                         
                else:
                    extraction = monthly_extraction.prepare_all_dfs_monthly(sheets,xl)
                    if extraction["status"] == 200:
                        filedata.update({"report_type":report["response"]["str_type"].split(" ")[0]})
                        db["Monthly_uploads"].insert_one(filedata)
                
                file_status.append({'name':fname,'s3_key':unique_filename, 'message':extraction['message'], 'status':extraction['status']})

            else:
                file_status.append({'name':fname,'message':'Invalid file format. Allowed formats are .xlsx, xls only', 'status':500})

        return JSONResponse({"file_status":file_status,"status":200})

    except Exception as e:
        return JSONResponse({"messege": str(e),"status":500})


@app.post('/week')
def week_data():
    pass

@app.post('/weekly')
def weekly_data():
    pass

@app.post('/month')
def month_data(data: Dict[str, str] = Body(...)):
    try:
        result = api.get_month_data(data)
        # print(result)
        return result
    except Exception as e:
        return {"error":e,status:500}
    

@app.post('/monthly')
def monthly_data(data:Dict[str,str]=Body(...)):
    try:
        result = api.get_monthly_data(data)
        # print(result)
        return result
    except Exception as e:
        return {"error":e,status:500}

@app.post('/year')
def year_data():
    pass
@app.post('/range')
def range_data():
    pass

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)