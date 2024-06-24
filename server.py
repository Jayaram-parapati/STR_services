from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile,Response,Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn
from datetime import datetime
import uuid
import sys,json,os
import pandas as pd
from io import BytesIO
from typing import Dict,Optional,Union,Any
from bson import ObjectId

from mongo_service import connect_to_MongoDb
from extraction_service.weekly_report_extraction import Weekly_extraction
from extraction_service.monthly_report_extraction import Monthly_extraction
from extraction_service.toc_extraction import str_report_type
from s3_service.s3_services import AWS_S3_Service 
from mongo_service.data_endpoints import APIendpoints
from mongo_service.basemodels import *

weekly_extraction = Weekly_extraction()
monthly_extraction=Monthly_extraction()
aws_boto3 = AWS_S3_Service()
report_type = str_report_type()
api = APIendpoints()

db_connection = connect_to_MongoDb()
db = db_connection.db


app = FastAPI(title='STR Services')
origins = [
    "https://localhost:8000",  
    "https://localhost:7187",
    "https://api.nimbleio.ai"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload_file",tags=["upload file"])
def upload_file(
                files: list[UploadFile] = File(...),
                corporation_name: str = Form(...),
                str_id: str = Form(...),
                corporation_id: str  = Form(...),
                profit_center_id: Optional[str] = Form(None),
                profit_center_name: Optional[str] = Form(None),
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
                    "s3_key": f"https://hgtech-str-files.s3.ap-south-1.amazonaws.com/{unique_filename}",
                    "upload_date": _ts,
                    "delete_status":0,
                    "corporation_name": corporation_name,
                    "str_id":str_id,
                    "corporation_id":corporation_id,
                    "profit_center_id":profit_center_id,
                    "profit_center_name":profit_center_name,
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

                        check_uploadFile = api.check_upload_file(corporation_id,profit_center_id,str_id,reportDate,reportType)
                        if not check_uploadFile:
                            
                            query = {"corporation_id":corporation_id,"delete_status":0}
                            
                            if profit_center_id:
                                query.update({"profit_center_id":profit_center_id})
                            
                            matchObj = db[f'{reportType}_uploads'].find_one(query)
                            if matchObj is None:
                                if reportType == 'Weekly': 
                                    matchObj = db['Monthly_uploads'].find_one(query)
                                else:
                                    matchObj = db['Weekly_uploads'].find_one(query)

                            if  matchObj is None or matchObj['str_id'] == str_id:
                            
                                str_match = db["Weekly_uploads"].find_one({"str_id":str_id})
                                if str_match is None:
                                    str_match = db["Weekly_uploads"].find_one({"str_id":str_id})
                                if str_match:
                                    str_match_pcId = str_match.get("profit_center_id",None)
                                
                                if str_match is None or  (str_match["corporation_id"] == corporation_id and str_match_pcId == profit_center_id):
                                
                                    if report['response']['str_id'] == str_id:

                                        if reportType == "Weekly":
                                            extraction = weekly_extraction.prepare_all_dfs(sheets,xl)
                                            if extraction["status"] == 200:
                                                filedata.update({"report_type":reportType,"date_range":reportDate,"extraction_report_id":extraction["report_id"]})
                                                db["Weekly_uploads"].insert_one(filedata)
                                                
                                        elif reportType == "Monthly": 
                                            extraction = monthly_extraction.prepare_all_dfs_monthly(sheets,xl)
                                            if extraction["status"] == 200:
                                                filedata.update({"report_type":reportType,"date_range":reportDate,"extraction_report_id":extraction["report_id"]})
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
                                        file_status.append({'file_name':fname,"message":"Plese check the STR Id entered","status":400})
                                else:
                                    file_status.append({'file_name':fname,"message":"given STR ID is already mapped with another Corporation","status":400})                     
                            else:
                                file_status.append({'file_name':fname,"message":"Profit centre already mapped with another STR ID","status":400})       

                        else:
                            file_status.append({'file_name':fname,'message':'file already exist please check', 'status':500})
                    else:
                        file_status.append({'file_name':fname,'message':'error in uploading to s3 bucket', 'status':500})
                else:
                    file_status.append({'file_name':fname,'message':'Invalid file format. Allowed formats are .xlsx, .xls only', 'status':500})
            except Exception as ex:
                file_status.append({'file_name':fname,"exception": str(ex), "message":"invalid file to Extraction",'status':500})
            
        return JSONResponse({"file_status":file_status,'status':200})

    except Exception as e:
        print("error while uploading the file")
        return JSONResponse({"messege": str(e),"status":500})

#End Points for single Corporation data

@app.post('/week',tags = ["data end points"])
def week_data(data:WeekData):
    try:
        data_dict = data.model_dump()
        result = api.get_week_data(data_dict)
        return result
    except HTTPException as e:
        raise e  
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/weekly',tags = ["data end points"])
def weekly_data(data:WeeklyData):
    try:
        data_dict = data.model_dump()
        result = api.get_weekly_data(data_dict)
        return result
    except HTTPException as e:
        raise e  
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/month',tags = ["data end points"])
def month_data(data:MonthData):
    try:
        data_dict = data.model_dump()
        result = api.get_month_data(data_dict)
        # print(result)
        return result
    except HTTPException as e:
        raise e  
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post('/monthly',tags = ["data end points"])
def monthly_data(data:MonthlyData):
    try:
        data_dict = data.model_dump()
        result = api.new_get_monthly_data(data_dict)
        # print(result)
        return result
    except HTTPException as e:
        raise e  
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/yearly',tags = ["data end points"])
def year_data(data:YearlyData):
    try:
        data_dict = data.model_dump()
        result = api.new_get_yearly_data(data_dict)
        return result
    except HTTPException as e:
        raise e  
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post('/range',tags = ["data end points"])
def range_data(data:WeekData):
    try:
        data_dict = data.model_dump()
        result = api.new_get_range_data(data_dict)
        return result
    except HTTPException as e:
        raise e  
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post('/importFilesList',tags=["import screen end points"])
def import_files(data:Dict[str,str]=Body(...)):
    try:
        corporation_id = data["corporation_id"]
        filetype = data["filetype"]
        year = int(data["year"])
        month = data.get("month", None)
        profit_center = data.get("profit_center", None)
        result = {}
        query_params = {"corporation_id":corporation_id,
                        "report_type":filetype,
                        "year":year}
        str_match_query = {"corporation_id": corporation_id,"delete_status":0}
        if profit_center:
            query_params.update({"profit_center":profit_center})
            str_match_query.update({"profit_center_id":profit_center})
      
        obj = db["Weekly_uploads"].find_one(str_match_query,{"_id":0,"str_id":1})
        str_id = (obj or {}).get('str_id',None)
        if obj is None:
            obj_m = db["Monthly_uploads"].find_one(str_match_query,{"_id":0,"str_id":1})
            str_id = (obj_m or {}).get('str_id',None)
        
        
        result.update({"str_id":str_id})         
        if filetype == "Monthly":
            result.update({filetype:api.get_import_files_monthly(query_params)})
        elif filetype == "Weekly":
            query_params.update({"month":month})
            result.update({filetype:api.get_import_files_weekly(query_params)})
        elif filetype == "Both":
            query_params.update({"month":month})
            result.update({"Monthly":api.get_import_files_monthly(query_params)})
            result.update({"Weekly":api.get_import_files_weekly(query_params)})

        return result
    except Exception as e:
        result = {
                "status_code":500,
                "detail":"No data found",
                "error":str(e)
            }
        return result


#End Points for All Corporations data

@app.post('/all_corporations_week',tags=["All Corporations Data Endpoints"])
def all_corps_weekData(data:AllCorpWeekData):
    try:
        data_dict = data.model_dump()
        result = api.all_corps_week_data(data_dict)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

@app.post('/all_corporations_weekly',tags=["All Corporations Data Endpoints"])
def all_corps_weeklyData(data:AllCorpWeeklyData):
    try:
        data_dict = data.model_dump()
        result = api.all_corps_weekly_data(data_dict)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

@app.post('/all_corporations_month',tags=["All Corporations Data Endpoints"])
def all_corps_monthData(data:AllCorpMonthData):
    try:
        data_dict = data.model_dump()
        result = api.all_corps_month_data(data_dict)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

@app.post('/all_corporations_monthly',tags=["All Corporations Data Endpoints"])
def all_corps_monthlyData(data:AllCorpMonthlyData):
    try:
        data_dict = data.model_dump()
        result = api.all_corps_monthly_data(data_dict)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

@app.post('/all_corporations_yearly',tags=["All Corporations Data Endpoints"])
def all_corps_yearlyData(data:AllCorpYearlyData):
    try:
        data_dict = data.model_dump()
        result = api.all_corps_yearly_data(data_dict)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

@app.post('/all_corporations_range',tags=["All Corporations Data Endpoints"])
def all_corps_rangeData(data:AllCorpWeekData):
    try:
        data_dict = data.model_dump()
        result = api.all_corps_range_data(data_dict)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))


# Endpoints for latest imported file

@app.post('/latestUploadFile',tags=["latest upload"])
def latest_upload_file(data_dict:latestUploadData):
    try:
        data = data_dict.model_dump()
        corp_id = data["corporation_id"]
        coll = data["type"]
        collection = coll+"_uploads"
        pc_id = data.get("profit_center_id",None)
        match_query = {"corporation_id":corp_id,"delete_status":0}
        if pc_id:
            match_query.update({"profit_center_id":pc_id})
        res = list(db[collection].find(match_query,{"_id": 0, "extraction_report_id": 0,"delete_status":0}).sort("date_range", -1).limit(1))
        if len(res) == 0:
            result={
                "data":[],
                "status_code":400,
                "detail":"No Data found"
            }
            return result
        result ={
            "data":res,
            "status_code":200,
            "detail":"Data retrieved successfully"
        }
        return result
    except Exception as e:
        result = {
                "status_code":500,
                "detail":"No data found",
                "error":str(e)
            }
        return result

# End point for Daily by Month

@app.post('/dailyByMonth',tags=["Daily by Month"])
def daily_data_from_month_report(data:MonthData):
    try:
        data_dict = data.model_dump()
        result = api.daily_data_from_monthreport(data_dict)
        return result
    except Exception as e:
        result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(e)
            }
        return result

#End point for STR KPI's
    
@app.post('/getKPI',tags=["KPI"])
def str_kpi(data:WeekData):
    try:
        data_dict = data.model_dump()
        result = api.get_str_kpi_data(data_dict)
        return result
    except Exception as e:
        result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(e)
            }
        return result    


@app.post('/getReportData',tags=["report data"])
def report_data(data:ReportData):
    try:
        data_dict = data.model_dump()
        result = api.get_report_data(data_dict)
        return result
    except Exception as e:
        result={
            "status_code":500,
            "detail":"No detail found",
            "error":str(e)
        }

@app.post('/delete',tags=["delete upload file"])
def delete_upload(data:deletefile):
    try:
        time_now = datetime.now()
        data_dict = data.model_dump()
        upload_id = data_dict["fileId"]
        filetype = data_dict["fileType"]
        user_id = data_dict["userId"]
        collection = filetype+"_uploads"
        res = db[collection].update_one({"_id":ObjectId(upload_id)},{"$set":{"delete_status":1,"deleted_at":time_now,"deleted_by":user_id}})
        result = {}
        if res.matched_count == 1:
            result ={
                "status_code":204,
                "message":"File deleted succesfully"
            }    
            return result
        else:
            result ={
                "status_code":400,
                "message":"File Not Found"
            } 
        return result
    except Exception as e:
        result={
            "status_code":500,
            "detail":"No detail found",
            "error":str(e)
        }
        return result
       
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)