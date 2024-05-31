from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile,Response,Query
from datetime import datetime, timedelta
import calendar
from bson import ObjectId

from mongo_service import connect_to_MongoDb


class APIendpoints(connect_to_MongoDb):
    def __init__(self):
        super().__init__()
        
    def get_week_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            
            start_query_date = data["startdate"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["enddate"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")

            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
           
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            obj = self.db.Weekly_uploads.find_one(obj_id_query)
            if obj is None:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objId = obj["extraction_report_id"]
            
            pipeline=[
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": ObjectId(str_id_objId)}},
                {"$project":{"_id":0,"metadata.str_id":0}}
            ]
            result ={}
            
            coll_names = ["adr","occupancy","revpar"]
            for collection_name in coll_names:
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
        
        except HTTPException as err:
            raise err 
        except Exception as err:
            raise HTTPException(status_code=500,detail=str(err))
        
    def get_weekly_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
         
            start_query_date = data["week_start_date"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["week_end_date"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")
            
            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
                
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            documents = list(self.db.Weekly_uploads.find(obj_id_query))
            if len(documents) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objIds = [doc["extraction_report_id"] for doc in documents]
            
            pipeline=[
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": {"$in": str_id_objIds},"tag_type":{"$eq":"Current Week"}}},
                {"$project":{"_id":0,"metadata.str_id":0}}
            ]
            result ={}
            coll_names = ["adr","occupancy","revpar"]
            for collection_name in coll_names:
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
         
        except HTTPException as err:
            raise err 
        except Exception as err:
            raise HTTPException(status_code=500,detail=str(err)) 
    
    def get_month_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            year = int(data["year"])
            month = int(data["month"])
            if month<3:
                start_month = month+10
                start_year = year-1
            else:
                start_month = month-2
                start_year = year    
            
            start_query_date = f"{start_year} {start_month}"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m")
            
            end_query_date = f"{year} {month}"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m")
            
            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
                
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            obj = self.db.Monthly_uploads.find_one(obj_id_query)
            if obj is None:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objId = obj["extraction_report_id"]

            
            pipeline = [
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": ObjectId(str_id_objId),"tag_type":{"$exists":False}}},
                {"$project":{"_id":0,"metadata.str_id":0}}
            ]
            pipeline1 = [
                {"$match":{"timestamp":{"$eq":end_ts_obj},"metadata.str_id": ObjectId(str_id_objId),"tag_type":{"$exists":True}}},
                {"$project":{"_id":0,"metadata.str_id":0}}
            ]
            result={}
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
                result.update({collection_name+"_glance":list(self.db[collection_name].aggregate(pipeline1))})
            # print(result)
            return result
        except HTTPException as err:
            raise err 
        except Exception as err:
            raise HTTPException(status_code=500,detail=str(err))
        
    def get_monthly_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            year = int(data["year"])
           
            start_query_date = f"{year} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m")
            
            end_query_date = f"{year} 12"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m")
            
            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
                
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            documents = list(self.db.Monthly_uploads.find(obj_id_query))
            if len(documents) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objIds = [doc["extraction_report_id"] for doc in documents]
            
            pipeline=[
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": {"$in": str_id_objIds},"tag_type":{"$exists":False}}},
                {"$project":{"_id":0,"metadata.str_id":0}}
            ]
            result ={}
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                result.update({collection_name.split('_')[0]:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
        except HTTPException as err:
            raise err 
        except Exception as err:
            raise HTTPException(status_code=500,detail=str(err)) 
        
    def get_yearly_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            
            num = int(data["years_selected"])
            current_year = datetime.now().year
            current_month = datetime.now().month
            start_year = current_year - num
            
            
            start_query_date = f"{start_year} {current_month} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{current_year} {current_month} 01"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            documents = list(self.db.Monthly_uploads.find(obj_id_query))
            if len(documents) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objIds = [doc["extraction_report_id"] for doc in documents]
            
            pipeline =[
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": {"$in": str_id_objIds},"tag_type":{"$exists":False}}},
                {"$group":{
                    "_id":{"year":{"$year":"$timestamp"},"label":"$metadata.label"},
                    "avg_change":{
                        "$avg":{
                            "$cond": {
                                "if": { "$ne": ["$metadata.label", "Rank"] },
                                "then": "$change",
                                "else": { "$toInt": { "$arrayElemAt": [ { "$split": ["$change", " of "] }, 0] } }}}}}},
                {"$project":{
                    "_id":0,
                    "year":"$_id.year",
                    "label":"$_id.label",
                    "change":{
                        "$cond":{
                            "if":{"$ne":["$_id.label","Rank"]},
                            "then":"$avg_change",
                            "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of 5"]}}}}},
                {"$sort":{"year":1}}
            ]
            result = {}
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                result.update({collection_name.split('_')[0]:list(self.db[collection_name].aggregate(pipeline))})
            return result
        except HTTPException as err:
            raise err 
        except Exception as err:
            raise HTTPException(status_code=500,detail=str(err)) 
        
    def get_range_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
    
            start_query_date = data["startdate"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["enddate"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")
            
            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
            
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            documents = list(self.db.Weekly_uploads.find(obj_id_query))
            if len(documents) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objIds = [doc["extraction_report_id"] for doc in documents]
            
            pipeline=[
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": {"$in": str_id_objIds},"tag_type":{"$exists":False},"metadata.label":{"$ne":"Your rank"}}},
                # {"$group":{"_id":{"label":"$metadata.label"},"avg_change":{"$avg":"$change"}}},
                # {"$project":{"_id":0,"label":"$_id.label","change":"$avg_change"}}
                {"$group": {"_id": "$metadata.label","unique_changes": {"$addToSet": {"timestamp": "$timestamp","change": "$change","change_rate":"$change_rate"}}}},
                {"$project": {"_id": 0,"label": "$_id","change": {"$avg": "$unique_changes.change"},"change_rate": {"$avg": "$unique_changes.change_rate"}}}
            ]
            result ={}
            coll_names = ["adr","occupancy","revpar"]
            for collection_name in coll_names:
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
         
        except HTTPException as err:
            raise err 
        except Exception as err:
            raise HTTPException(status_code=500,detail=str(err)) 
        
    def check_upload_file(self,fname,str_id,date,reportType):
        query = {
            "file_name": fname,
            "str_id": str_id,
            "date_range": {
                "$gte": date[0],
                "$lte": date[1]
            }
        }
        
        matchObj = self.db[f'{reportType}_uploads'].find_one(query)
        return matchObj
           
    def get_import_files_monthly(self,data):
        try:
            corporation_id = data["corporation_id"]
            profit_center = data.get("profit_center", None)
            year = data["year"]
            start_month = int(f"{1:02d}")
            end_month = 12
            if data["report_type"] == "Both" and data["month"] != "all":
                start_month = int(data["month"])
                end_month = int(data["month"])
            date = calendar.monthrange(year,end_month)[1]
            
            start_query_date = f"{year} {start_month} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{year} {end_month} {date}"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")

            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
            
            match_query = {
                            "corporation_id": corporation_id,
                            "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}
                         }
            if profit_center:
                match_query.update({"profit_center_id":profit_center,})
            
            pipeline = [
                        {
                         "$match": match_query
                        },
                        {"$addFields": {
                            "objId": {"$toString": "$_id"}
                        }},
                        {"$project": {
                            "_id": 0,
                            "delete_status":0,
                            "extraction_report_id":0
                        }},
                    ]
            result = list(self.db["Monthly_uploads"].aggregate(pipeline))
            if len(result) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corporation_id} between {start_query_date} and {end_query_date}")
            return result
        except HTTPException as err:
            raise err 
        except Exception as err:
            raise HTTPException(status_code=500,detail=str(err))
    
    def get_import_files_weekly(self,data):
        try:
            corporation_id = data["corporation_id"]
            year = data["year"]
            profit_center = data.get("profit_center", None)
            if data["month"] == "all":
                start_month = int(f"{1:02d}")
                end_month = 12
            else:
                start_month = int(data["month"])
                end_month = int(data["month"])
            
            date = calendar.monthrange(year,end_month)[1]
            
            
            start_query_date = f"{year} {start_month} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{year} {end_month} {date}"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
            
            match_query = {
                            "corporation_id": corporation_id,
                            "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}
                         }
            if profit_center:
                match_query.update({"profit_center_id":profit_center,})
                
            pipeline = [
                        {
                         "$match":match_query
                        },
                        {"$addFields": {
                            "objId": {"$toString": "$_id"}
                        }},
                        {"$project": {
                            "_id": 0,
                            "delete_status":0,
                            "extraction_report_id":0
                        }},
                    ]
            result = list(self.db["Weekly_uploads"].aggregate(pipeline))
            if len(result) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corporation_id} between {start_query_date} and {end_query_date}")
            return result
        except HTTPException as err:
            raise err 
        except Exception as err:
            raise HTTPException(status_code=500,detail=str(err))
    
