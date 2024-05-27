from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile,Response,Query
from fastapi.responses import JSONResponse
import uvicorn
from datetime import datetime
import uuid
import sys,json,os
import pandas as pd
from io import BytesIO
from typing import Dict,Optional,Union
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

@app.post("/upload_file",tags=["upload file"])
def upload_file(
                files: list[UploadFile] = File(...),
                corporation_name: str = Form(...),
                str_id: str = Form(...),
                corporation_id: str  = Form(...),
                profit_center_id: Optional[str] = Form(None),
                user_id:str = Form(...),
                client_id: Optional[str] = Form(None),
                url:str = Form(...),
                ):
    try:
        file_status = []
        for path in files:
            try:
                
                file_content = path.file
                fname = path.filename
                file_name,file_extension = os.path.splitext(fname)
                _ts = datetime.now()
                unique_filename = f"{str(uuid.uuid4())}_{fname}"
                
                filedata = {
                    "file_name": fname,
                    "s3_key": unique_filename,
                    "upload_date": _ts,
                    "delete_status":0,
                    "corporation_name": corporation_name,
                    "str_id":str_id,
                    "corporation_id":corporation_id,
                    "profit_center_id":profit_center_id,
                    "user_id":user_id,
                    "client_id":client_id,
                    "url":url
                }
                
               
                if file_extension == '.xlsx' or file_extension == '.xls':
                    content_type = aws_boto3.get_mime_type(file_extension)
                    S3_file_upload = aws_boto3.upload_to_s3_object(file_content,unique_filename,content_type)

                    if S3_file_upload['status']== 200: 
                        file_body,contentType = aws_boto3.get_file_obj(unique_filename)
                        xl = pd.ExcelFile(file_body)  
                        sheets = xl.sheet_names
                        report = report_type.check_str_report_type(sheets,xl)

                        reportType = report["response"]["str_type"].split(" ")[0]
                        reportDate = report["response"]['date']

                        check_uploadFile = api.check_upload_file(fname,str_id,reportDate,reportType)
                        if not check_uploadFile:
                            if report['response']['str_id'] == str_id:

                                if reportType == "Weekly":
                                    extraction = weekly_extraction.prepare_all_dfs(sheets,xl)
                                    if extraction["status"] == 200:
                                        filedata.update({"report_type":reportType,"date_range":reportDate})
                                        db["Weekly_uploads"].insert_one(filedata)
                                        
                                elif reportType == "Monthly": 
                                    extraction = monthly_extraction.prepare_all_dfs_monthly(sheets,xl)
                                    if extraction["status"] == 200:
                                        filedata.update({"report_type":reportType,"date_range":reportDate})
                                        db["Monthly_uploads"].insert_one(filedata)

                                else:
                                    file_status.append({'file_name':fname,'message':'Invalid file', 'status':500})

                                file_status.append(
                                    {'file_name':fname,
                                    's3_key':f"https://hgtech-str-files.s3.ap-south-1.amazonaws.com/{unique_filename}",
                                    'message':"successfully uploaded",
                                    'status':extraction['status']
                                    }
                                    )    
                            else:
                                file_status.append({'file_name':fname,"message":"str ID was missmatched please check","status":400})       

                        else:
                            file_status.append({'file_name':fname,'message':'file already exist please check', 'status':500})
                else:
                    file_status.append({'file_name':fname,'message':'Invalid file format. Allowed formats are .xlsx, .xls only', 'status':500})
            except Exception as ex:
                file_status.append({'file_name':fname,"exception": str(ex), "message":"invalid file to Extraction",'status':500})
            
        return JSONResponse({"file_status":file_status,'status':200})

    except Exception as e:
        print("error while uploading the file")
        return JSONResponse({"messege": str(e),"status":500})


@app.post('/week',tags = ["data end points"])
def week_data(data:Dict[str,str]=Body(...)):
    try:
        result = api.get_week_data(data)
        return result
    except Exception as e:
        return {"error":e,status:500}

@app.post('/weekly',tags = ["data end points"])
def weekly_data(data:Dict[str,str]=Body(...)):
    try:
        result = api.get_weekly_data(data)
        return result
    except Exception as e:
        return {"error":e,status:500}

@app.post('/month',tags = ["data end points"])
def month_data(data: Dict[str, str] = Body(...)):
    try:
        result = api.get_month_data(data)
        # print(result)
        return result
    except Exception as e:
        return {"error":e,status:500}
    

@app.post('/monthly',tags = ["data end points"])
def monthly_data(data:Dict[str,str]=Body(...)):
    try:
        result = api.get_monthly_data(data)
        # print(result)
        return result
    except Exception as e:
        return {"error":e,status:500}

@app.post('/yearly',tags = ["data end points"])
def year_data(data:Dict[str,str]=Body(...)):
    try:
        result = api.get_yearly_data(data)
        return result
    except Exception as e:
        return {"error":e,status:500}
    
@app.post('/range',tags = ["data end points"])
def range_data(data:Dict[str,str]=Body(...)):
    try:
        result = api.get_range_data(data)
        return result
    except Exception as e:
        return {"error":e,status:500}
    
@app.post('/importFilesList',tags=["import screen end points"])
def import_files(corporation:str,filetype:str,year:int,month:Optional[Union[int,str]]=None,profit_center:Optional[str]=None,):
    try:
        result = {}
        query_params = {"corporation_name":corporation,
                        "report_type":filetype,
                        "year":year,}
        if profit_center:
            query_params.update({"profit_center":profit_center,})
        if filetype == "Monthly":
            result.update({filetype:api.get_import_files_monthly(query_params)})
        elif filetype == "Weekly":
            query_params.update({"month":month})
            result.update({filetype:api.get_import_files_weekly(query_params)})
        elif filetype == "Both":
            query_params.update({"month":month})
            result.update({"Monthly":api.get_import_files_monthly(query_params)})
            result.update({"Weekly":api.get_import_files_weekly(query_params)})
            
            # result = api.get_import_files_both()
        if len(result) == 0:
            message = f"No Files found for {corporation} for {year} "
            if filetype == "Weekly":
                message += f"{month}"
            return {"message":message,"status":200}
        return result
    except Exception as e:
        return {"error":e,status:500}

# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=8000)