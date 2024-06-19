from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile,Response,Query
from datetime import datetime, timedelta
import calendar
from bson import ObjectId

from mongo_service import connect_to_MongoDb


class APIendpoints(connect_to_MongoDb):
    def __init__(self):
        super().__init__()
        
    def get_week_data(self,data,**kwargs):
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
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            response_data = {"corporation_name":obj["corporation_name"],
                            "corporation_id":obj["corporation_id"],
                            "profitcenter_id":obj.get("profit_center_id",None),
                            "profitcenter_name":obj.get("profit_center_name",None),
                            "str_id":obj["str_id"],
                            }
            collection = data.get("sheet",None)
            if collection:
                res = {}
                res.update(response_data)
                res.update({
                            "sheet":collection,
                            "data":list(self.db[collection].aggregate(pipeline))
                        })
                return res
          
            viewby = data.get("viewBy",None)
            if viewby:
                if viewby == 'Day':
                    pipeline[0]["$match"].update({"tag_type":{"$exists":False}})
                    pipeline[1]["$project"].pop("change_rate")
                    if kwargs["diff_days"] > 7:
                        documents = list(self.db.Weekly_uploads.find(obj_id_query))
                        str_id_objIds = [doc["extraction_report_id"] for doc in documents]
                        pipeline[0]["$match"]["metadata.str_id"] = {"$in":str_id_objIds}
                        
                    
                    
                    
                    
                    
            result ={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr","occupancy","revpar"]
            for collection_name in coll_names:
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
        
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Weekly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result
        
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
            monthly_docs = []
            if len(documents) == 0:
                
                max_days = calendar.monthrange(start_ts_obj.year,start_ts_obj.month)[1]
                temp_start_query_date = f"{start_ts_obj.year} {start_ts_obj.month} 01"
                temp_start_ts_obj = datetime.strptime(temp_start_query_date,"%Y %m %d")
                temp_end_query_date = f"{start_ts_obj.year} {start_ts_obj.month} {max_days}"
                temp_end_ts_obj = datetime.strptime(temp_end_query_date,"%Y %m %d")
                
                obj_id_query["date_range"]["$elemMatch"]["$gte"]=temp_start_ts_obj
                obj_id_query["date_range"]["$elemMatch"]["$lte"]=temp_end_ts_obj

                monthly_docs = list(self.db.Monthly_uploads.find(obj_id_query))
                if len(monthly_docs) == 0:
                    raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
                monthly_str_id_objIds = [doc["extraction_report_id"] for doc in monthly_docs]
                monthly_pipeline = [
                    {"$match":{"metadata.str_id":{"$in":monthly_str_id_objIds},"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj}}},
                    {
                        "$group": {
                            "_id": {
                                "year": {"$year": "$timestamp"},
                                "week": {"$week": "$timestamp"},
                                "label": "$metadata.label",
                            },
                            "avg_change": {"$avg": "$change"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp": {
                                "$dateFromParts": {"isoWeekYear": "$_id.year","isoWeek": "$_id.week","isoDayOfWeek": 0}
                            },
                            "metadata.label": "$_id.label",
                            "change": "$avg_change",
                            "week_range": [
                                            {"$dateToString": {"date": {"$dateFromParts": {"isoWeekYear": "$_id.year","isoWeek": "$_id.week","isoDayOfWeek": 0}}}},
                                            {"$dateToString": {"date": {"$dateFromParts": {"isoWeekYear": "$_id.year","isoWeek": "$_id.week","isoDayOfWeek": 6}}}},  
                                        ]
                            
                            
                        }
                    },
                    {"$sort": {"timestamp": 1}},
                ]
                response_data = {"corporation_name":monthly_docs[0]["corporation_name"],
                                "corporation_id":monthly_docs[0]["corporation_id"],
                                "profitcenter_id":monthly_docs[0].get("profit_center_id",None),
                                "profitcenter_name":monthly_docs[0].get("profit_center_name",None),
                                "str_id":monthly_docs[0]["str_id"],
                                }
    
            str_id_objIds = [doc["extraction_report_id"] for doc in documents]
            
            pipeline=[
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": {"$in": str_id_objIds},"tag_type":{"$eq":"Current Week"}}},
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            if documents:
                response_data = {"corporation_name":documents[0]["corporation_name"],
                                "corporation_id":documents[0]["corporation_id"],
                                "profitcenter_id":documents[0].get("profit_center_id",None),
                                "profitcenter_name":documents[0].get("profit_center_name",None),
                                "str_id":documents[0]["str_id"],
                                }
            
            collection = data.get("sheet",None)
            if collection:
                res = {}
                res.update(response_data)
                collectionName_allCorp = collection
                pipeline_to_aggregate_allCorp = pipeline
                if monthly_docs:
                    collectionName_allCorp = collection+"_DailyByMonth"
                    pipeline_to_aggregate_allCorp = monthly_pipeline
                res.update({
                            "sheet":collection,
                            "data":list(self.db[collectionName_allCorp].aggregate(pipeline_to_aggregate_allCorp))
                            })
                return res
            
            result ={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr","occupancy","revpar"]
            for collection_name in coll_names:
                collectionName = collection_name
                pipeline_to_aggregate = pipeline
                if monthly_docs:
                    collectionName = collection_name+"_DailyByMonth"
                    pipeline_to_aggregate = monthly_pipeline
                result.update({collection_name:list(self.db[collectionName].aggregate(pipeline_to_aggregate))})
            # print(result)
            return result
         
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Weekly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result 
    
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
            
            date = calendar.monthrange(year,month)[1]
            
            start_query_date = f"{year} {month} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{year} {month} {date}"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            pipeline_start_query_date = f"{start_year} {start_month} 01"
            pipeline_start_ts_obj = datetime.strptime(pipeline_start_query_date,"%Y %m %d")
            
            
            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
                
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            obj = self.db.Monthly_uploads.find_one(obj_id_query)
            if obj is None:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objId = obj["extraction_report_id"]

            
            pipeline = [
                {"$match":{"timestamp":{"$gte":pipeline_start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": ObjectId(str_id_objId),"tag_type":{"$exists":False}}},
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            pipeline1 = [
                {"$match":{"timestamp":{"$eq":start_ts_obj},"metadata.str_id": ObjectId(str_id_objId),"tag_type":{"$exists":True}}},
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            response_data = {"corporation_name":obj["corporation_name"],
                            "corporation_id":obj["corporation_id"],
                            "profitcenter_id":obj.get("profit_center_id",None),
                            "profitcenter_name":obj.get("profit_center_name",None),
                            "str_id":obj["str_id"],
                            }
            collection = data.get("sheet",None)
            if collection:
                res = {}
                coll_name = collection+"_monthlyAvgs"
                res.update(response_data)
                res.update({coll_name:{
                            "sheet":collection,
                            "data":list(self.db[coll_name].aggregate(pipeline))
                            }})
                res.update({coll_name+"_glance":{
                            "sheet":collection,
                            "data":list(self.db[coll_name].aggregate(pipeline1))
                            }})
                return res
            
            result={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
                result.update({collection_name+"_glance":list(self.db[collection_name].aggregate(pipeline1))})
            # print(result)
            return result
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result 
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result
        
    def get_monthly_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            year = int(data["year"])
           
            start_query_date = f"{year} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m")
            
            end_query_date = f"{year} 12 31"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            today = datetime.today()
            if start_ts_obj >= today:
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
            weekly_docs=[]
            if len(documents) == 0:
                
                weekly_docs = list(self.db.Weekly_uploads.find(obj_id_query))
                if len(weekly_docs) == 0:
                    raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
                weekly_str_id_objIds = [doc["extraction_report_id"] for doc in weekly_docs]
                weekly_pipeline = [
                    {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$eq":"Current Week"}}},
                    {
                        "$group": {
                            "_id": {
                                "year": {"$year": "$timestamp"},
                                "month": {"$month": "$timestamp"},
                                "label": "$metadata.label",
                            },
                            "avg_change":{
                                "$avg":{
                                    "$cond": {
                                        "if": { "$ne": ["$metadata.label", "Your rank"] },
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": ["$change", "null"] },
                                                "then": 0,
                                                "else": { "$toDouble": "$change" }
                                                }},
                                        "else": {
                                            "$cond": {
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 0] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 0] } }
                                            }
                                        }
                                    }}}
                        }
                    },
                    {
                     "$project":{
                         "_id": 0,
                        "timestamp": {
                            "$dateFromParts": {"year": "$_id.year","month": "$_id.month"}
                        },
                        "label": "$_id.label",
                        "change":{
                            "$cond":{
                                "if":{"$ne":["$_id.label","Your rank"]},
                                "then":"$avg_change",
                                "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of 5"]}}}
                     }   
                    },
                    {"$sort":{"timestamp":1}}
                ]
                response_data = {"corporation_name":weekly_docs[0]["corporation_name"],
                                "corporation_id":weekly_docs[0]["corporation_id"],
                                "profitcenter_id":weekly_docs[0].get("profit_center_id",None),
                                "profitcenter_name":weekly_docs[0].get("profit_center_name",None),
                                "str_id":weekly_docs[0]["str_id"],
                                }
                    
          
            str_id_objIds = [doc["extraction_report_id"] for doc in documents]
            uploaded_timestamps = [doc["date_range"][0] for doc in documents]
            
            pipeline=[
                {"$match":{"timestamp":{"$in":uploaded_timestamps},"metadata.str_id": {"$in": str_id_objIds},"tag_type":{"$exists":False}}},
                {"$group":{"_id":{"timestamp":"$timestamp","label":"$metadata.label"},"unique_change":{"$first":"$change"}}},
                {"$project": {"_id": 0,"timestamp": "$_id.timestamp","label":"$_id.label","change": "$unique_change"}},
                {"$sort":{"timestamp":1}}
            ]
            if documents:
                response_data = {"corporation_name":documents[0]["corporation_name"],
                                "corporation_id":documents[0]["corporation_id"],
                                "profitcenter_id":documents[0].get("profit_center_id",None),
                                "profitcenter_name":documents[0].get("profit_center_name",None),
                                "str_id":documents[0]["str_id"],
                                }
            collection = data.get("sheet",None)
            if collection:
                res = {}
                res.update(response_data)
                coll_name = collection+"_monthlyAvgs"
                res.update({
                            "sheet":collection,
                            "data":list(self.db[coll_name].aggregate(pipeline)),
                            })
                return res
            
            result ={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                collectionName = collection_name
                pipeline_to_aggregate = pipeline
                if weekly_docs:
                    collectionName = collection_name.split('_')[0]
                    pipeline_to_aggregate = weekly_pipeline
                result.update({collection_name.split('_')[0]:list(self.db[collectionName].aggregate(pipeline_to_aggregate))})
            # print(result)
            return result
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result 
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result 
        
    def new_get_monthly_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            year = int(data["year"])
           
            start_query_date = f"{year} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m")
            
            end_query_date = f"{year} 12 31"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            today = datetime.today()
            if start_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
                
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            monthly_documents = list(self.db.Monthly_uploads.find(obj_id_query))
            weekly_documents = list(self.db.Weekly_uploads.find(obj_id_query))
            
            monthly_str_id_objIds = [doc["extraction_report_id"] for doc in monthly_documents]
            weekly_str_id_objIds = [doc["extraction_report_id"] for doc in weekly_documents]
            
            uploaded_timestamps = [doc["date_range"][0] for doc in monthly_documents]
            uploaded_timestamps.extend([doc["date_range"][0] for doc in weekly_documents])
            
            if len(weekly_documents)==0 and len(monthly_documents)==0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            if len(monthly_documents) != 0:
                corp_details_obj = monthly_documents[0]
            else:corp_details_obj = weekly_documents[0]
            
            response_data = {"corporation_name":corp_details_obj["corporation_name"],
                                "corporation_id":corp_details_obj["corporation_id"],
                                "profitcenter_id":corp_details_obj.get("profit_center_id",None),
                                "profitcenter_name":corp_details_obj.get("profit_center_name",None),
                                "str_id":corp_details_obj["str_id"],
                                }
            
            weekly_docs=[]
            if len(weekly_documents) == 0:
                
                weekly_docs = list(self.db.Weekly_uploads.find(obj_id_query))
                if len(weekly_docs) == 0:
                    raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
                weekly_str_id_objIds = [doc["extraction_report_id"] for doc in weekly_docs]
                weekly_pipeline = [
                    {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$eq":"Current Week"}}},
                    {
                        "$group": {
                            "_id": {
                                "year": {"$year": "$timestamp"},
                                "month": {"$month": "$timestamp"},
                                "label": "$metadata.label",
                            },
                            "avg_change":{
                                "$avg":{
                                    "$cond": {
                                        "if": { "$ne": ["$metadata.label", "Your rank"] },
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": ["$change", "null"] },
                                                "then": 0,
                                                "else": { "$toDouble": "$change" }
                                                }},
                                        "else": {
                                            "$cond": {
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 0] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 0] } }
                                            }
                                        }
                                    }}}
                        }
                    },
                    {
                     "$project":{
                         "_id": 0,
                        "timestamp": {
                            "$dateFromParts": {"year": "$_id.year","month": "$_id.month"}
                        },
                        "label": "$_id.label",
                        "change":{
                            "$cond":{
                                "if":{"$ne":["$_id.label","Your rank"]},
                                "then":"$avg_change",
                                "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of 5"]}}}
                     }   
                    },
                    {"$sort":{"timestamp":1}}
                ]
                response_data = {"corporation_name":weekly_docs[0]["corporation_name"],
                                "corporation_id":weekly_docs[0]["corporation_id"],
                                "profitcenter_id":weekly_docs[0].get("profit_center_id",None),
                                "profitcenter_name":weekly_docs[0].get("profit_center_name",None),
                                "str_id":weekly_docs[0]["str_id"],
                                }
        
            pipeline=[
                {"$match":{"timestamp":{"$in":uploaded_timestamps},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$exists":False}}},
                {"$group":{"_id":{"timestamp":"$timestamp","label":"$metadata.label"},"unique_change":{"$first":"$change"}}},
                {"$project": {"_id": 0,"timestamp": "$_id.timestamp","label":"$_id.label","change": "$unique_change"}},
                {"$sort":{"timestamp":1}}
            ]
            
                
            collection = data.get("sheet",None)
            if collection:
                res = {}
                res.update(response_data)
                coll_name = collection+"_monthlyAvgs"
                res.update({
                            "sheet":collection,
                            "data":list(self.db[coll_name].aggregate(pipeline)),
                            })
                return res
            
            result ={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                collectionName = collection_name
                pipeline_to_aggregate = pipeline
                if weekly_docs:
                    collectionName = collection_name.split('_')[0]
                    pipeline_to_aggregate = weekly_pipeline
                result.update({collection_name.split('_')[0]:list(self.db[collectionName].aggregate(pipeline_to_aggregate))})
            # print(result)
            return result
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result 
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result 
        
    def get_yearly_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            
            num = int(data["years_selected"])-1
            current_year = datetime.now().year
            start_year = current_year - num 
            
            
            start_query_date = f"{start_year} 01 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{current_year} 12 31"
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
                                "then":{
                                    "$cond":{
                                        "if": { "$eq": ["$change", "null"] },
                                        "then": 0,
                                        "else": { "$toDouble": "$change" }
                                        }},
                                "else": {
                                    "$cond": {
                                        "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 0] }, "null"] },
                                        "then": 0,
                                        "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 0] } }
                                    }
                                }
                            }}}}},
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
            response_data = {"corporation_name":documents[0]["corporation_name"],
                            "corporation_id":documents[0]["corporation_id"],
                            "profitcenter_id":documents[0].get("profit_center_id",None),
                            "profitcenter_name":documents[0].get("profit_center_name",None),
                            "str_id":documents[0]["str_id"],
                            }
            collection = data.get("sheet",None)
            if collection:
                res = {}
                res.update(response_data)
                coll_name = collection+"_monthlyAvgs"
                res.update({
                            "sheet":collection,
                            "data":list(self.db[coll_name].aggregate(pipeline)),
                            })
                return res
            
            result = {}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                result.update({collection_name.split('_')[0]:list(self.db[collection_name].aggregate(pipeline))})
            return result
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result 
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result 
        
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
                {"$group": {"_id": "$metadata.label","unique_changes": {"$addToSet": {"timestamp": "$timestamp","change": "$change"}}}},
                {"$project": {"_id": 0,"label": "$_id","change": {"$avg": "$unique_changes.change"}}}
            ]
            response_data = {"corporation_name":documents[0]["corporation_name"],
                            "corporation_id":documents[0]["corporation_id"],
                            "profitcenter_id":documents[0].get("profit_center_id",None),
                            "profitcenter_name":documents[0].get("profit_center_name",None),
                            "str_id":documents[0]["str_id"],
                            }
            collection = data.get("sheet",None)
            if collection:
                res = {}
                res.update(response_data)
                res.update({
                            "sheet":collection,
                            "data":list(self.db[collection].aggregate(pipeline)),
                            })
                return res
            
            result ={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr","occupancy","revpar"]
            for collection_name in coll_names:
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
         
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Weekly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result 
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result 
    
    def new_get_range_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
    
            start_query_date = data["startdate"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["enddate"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")
            
            today = datetime.today()
            if start_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
            
            all_dates = []
            current_date = start_ts_obj
            while current_date <= end_ts_obj:
                all_dates.append(current_date)
                current_date += timedelta(days=1)
            
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            weekly_documents = list(self.db.Weekly_uploads.find(obj_id_query))
            monthly_documents = list(self.db.Monthly_uploads.find(obj_id_query))
            
            weekly_str_id_objIds = [doc["extraction_report_id"] for doc in weekly_documents]
            monthly_str_id_objIds = [doc["extraction_report_id"] for doc in monthly_documents]
            
            if len(weekly_documents)==0 and len(monthly_documents)==0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            if len(weekly_documents) != 0:
                corp_details_obj = weekly_documents[0]
            else:corp_details_obj = monthly_documents[0]
                
            response_data = {"corporation_name":corp_details_obj["corporation_name"],
                            "corporation_id":corp_details_obj["corporation_id"],
                            "profitcenter_id":corp_details_obj.get("profit_center_id",None),
                            "profitcenter_name":corp_details_obj.get("profit_center_name",None),
                            "str_id":corp_details_obj["str_id"],
                            }
             
            result ={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr","occupancy","revpar"]
            collection = data.get("sheet",None)
            if collection:
                coll_names = [collection]
            for collection_name in coll_names:
                matched_objs = list(self.db[collection_name].find({"timestamp":{"$in":all_dates},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$exists":False},"metadata.label":{"$ne":"Your rank"}},))
                found_dates = [obj["timestamp"] for obj in matched_objs]
                missing_dates_from_weekly = [date for date in all_dates if not date in found_dates]
                missed_objs = list(self.db[collection_name+"_DailyByMonth"].find({"timestamp":{"$in":missing_dates_from_weekly},"metadata.str_id": {"$in": monthly_str_id_objIds}}))
                matched_objs.extend(missed_objs)
                pipeline = [
                    {"$match":{"$or":matched_objs}},
                    {"$group": {"_id": "$metadata.label","unique_changes": {"$addToSet": {"timestamp": "$timestamp","change": "$change"}}}},
                    {"$project": {"_id": 0,"label": "$_id","change": {"$avg": "$unique_changes.change"}}}
                ]
                if collection:
                    res = {}
                    res.update(response_data)
                    res.update({
                                "sheet":collection,
                                "data":list(self.db[collection].aggregate(pipeline)),
                                })
                    return res
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
            return result
         
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Weekly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result 
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result  
        
    def check_upload_file(self,corp_id,pc_id,str_id,date,reportType):
        query = {
            "corporation_id":corp_id,
            "str_id": str_id,
            "date_range": {
                "$gte": date[0],
                "$lte": date[1]
            },"delete_status":0
        }
        if pc_id:
            query.update({"profit_center_id":pc_id})
        
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
            if start_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
            
            match_query = {
                            "corporation_id": corporation_id,
                            "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                            "delete_status":0
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
            res = list(self.db["Monthly_uploads"].aggregate(pipeline))
            if len(res) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corporation_id} between {start_query_date} and {end_query_date}")
            result={
                    "data":res,
                    "status_code":200,
                    "detail":"Data retrieved successfully"
                    }    
            return result
        except HTTPException as err:
            result = {
                "data":[],
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corporation_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result 
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result
    
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
            if start_ts_obj >= today:
                raise HTTPException(status_code=400,
                                    detail="searching dates are invalid")
            
            match_query = {
                            "corporation_id": corporation_id,
                            "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                            "delete_status":0
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
            res = list(self.db["Weekly_uploads"].aggregate(pipeline))
            if len(res) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corporation_id} between {start_query_date} and {end_query_date}")
            
            result={
                    "data":res,
                    "status_code":200,
                    "detail":"Data retrieved successfully"
                    }    
            return result
        except HTTPException as err:
            result = {
                "data":[],
                "status_code":400
            }
            res = self.db.Weekly_uploads.find_one({"corporation_id":corporation_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result 
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result
    
    def all_corps_week_data(self,data):
        try:
            all_corp_res = []
            corp_data = {
                "startdate":data["startdate"],
                "enddate":data["enddate"],
                "sheet":data["sheet"]
            }
            
            start_query_date = data["startdate"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["enddate"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")

            # today = datetime.today()
            # if start_ts_obj >= today or end_ts_obj >= today:
            #     raise HTTPException(status_code=400,
            #                         detail="searching dates are invalid")
            
            corporations = data["corporations"]
            for corp in corporations:
                try:
                    corp_data.update({"corporation_id":corp})
                    multi_pc_data = list(self.db.Weekly_uploads.find({"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}},
                                                                     {"_id":0,"extraction_report_id":0}))
                    
                    if len(multi_pc_data) != 0:
                        for upload in multi_pc_data:
                            pc_id = upload.get("profit_center_id",None)
                            corp_data.update({"profit_center_id":pc_id})

                            corp_res = self.get_week_data(corp_data)
                            data_list = corp_res.get("data",None)
                            if data_list:
                                all_corp_res.append(corp_res)
                except Exception as err:
                    pass
                   
            if len(all_corp_res) == 0:
                empty_res = {
                    "data":[],
                    "detail":"No data found",
                    "status_code":400
                }
                return empty_res
            result = {
                "data":all_corp_res,
                "detail":"Data retrieved successfully",
                "status_code":200
            }
            return result
        except Exception as err :
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result
        
    def all_corps_weekly_data(self,data):
        try:
            all_corp_res = []
            corp_data = {
                "week_start_date":data["week_start_date"],
                "week_end_date":data["week_end_date"],
                "sheet":data["sheet"]
            }
            
            start_query_date = data["week_start_date"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["week_end_date"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")
            
            corporations = data["corporations"]
            for corp in corporations:
                try:
                    corp_data.update({"corporation_id":corp})
                    multipc_match_query = {"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}}
                    multi_pc_data = list(self.db.Weekly_uploads.find(multipc_match_query,{"_id":0,"extraction_report_id":0}))
                    if len(multi_pc_data) == 0:
                        max_days = calendar.monthrange(start_ts_obj.year,start_ts_obj.month)[1]
                        temp_start_query_date = f"{start_ts_obj.year} {start_ts_obj.month} 01"
                        temp_start_ts_obj = datetime.strptime(temp_start_query_date,"%Y %m %d")
                        temp_end_query_date = f"{start_ts_obj.year} {start_ts_obj.month} {max_days}"
                        temp_end_ts_obj = datetime.strptime(temp_end_query_date,"%Y %m %d")
                        
                        multipc_match_query["date_range"]["$elemMatch"]["$gte"]=temp_start_ts_obj
                        multipc_match_query["date_range"]["$elemMatch"]["$lte"]=temp_end_ts_obj
                        multi_pc_data = list(self.db.Monthly_uploads.find(multipc_match_query,{"_id":0,"extraction_report_id":0}))
                        
                    
                    if len(multi_pc_data) != 0:
                        pc_set = set()
                        for obj in multi_pc_data:
                            pc_id = obj.get("profit_center_id",None)
                            pc_set.add(pc_id)
                        for pc_id in pc_set:
                            corp_data.update({"profit_center_id":pc_id})
                            corp_res = self.get_weekly_data(corp_data)
                            data_list = corp_res.get("data",None)
                            if data_list:
                                all_corp_res.append(corp_res)
                
                except Exception as err:
                    pass
            if len(all_corp_res) == 0:
                empty_res = {
                    "data":[],
                    "detail":"No data found",
                    "status_code":400
                }
                return empty_res
            result = {
                "data":all_corp_res,
                "detail":"Data retrieved successfully",
                "status_code":200
            }
            return result
        except Exception as err :
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result   
     
    def all_corps_month_data(self,data):
        try:
            all_corp_res = []
            corp_data = {
                "year":data["year"],
                "month":data["month"],
                "sheet":data["sheet"]
            }
            
            year = int(data["year"])
            month = int(data["month"])
            
            date = calendar.monthrange(year,month)[1]
            
            start_query_date = f"{year} {month} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{year} {month} {date}"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            corporations = data["corporations"]
            for corp in corporations:
                try:
                    corp_data.update({"corporation_id":corp})
                    multi_pc_data = list(self.db.Monthly_uploads.find({"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}},
                                                                     {"_id":0,"extraction_report_id":0}))
                    
                    if len(multi_pc_data) != 0:
                        pc_set = set()
                        for obj in multi_pc_data:
                            pc_id = obj.get("profit_center_id",None)
                            pc_set.add(pc_id)
                        for pc_id in pc_set:
                            corp_data.update({"profit_center_id":pc_id})
                            corp_res = self.get_month_data(corp_data)
                            obj = data["sheet"]+"_monthlyAvgs"
                            data_obj = corp_res.get(obj,None)
                            if data_obj:
                                data_list = data_obj.get("data",None)
                                if data_list:
                                    all_corp_res.append(corp_res)
                except Exception as err:
                    pass
            if len(all_corp_res) == 0:
                empty_res = {
                    "data":[],
                    "detail":"No data found",
                    "status_code":400
                }
                return empty_res
            result = {
                "data":all_corp_res,
                "detail":"Data retrieved successfully",
                "status_code":200
            }
            return result
        except Exception as err :
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result    
        
    def all_corps_monthly_data(self,data):
        try:
            all_corp_res = []
            corp_data = {
                "year":data["year"],
                "sheet":data["sheet"]
            }
            
            year = int(data["year"])
           
            start_query_date = f"{year} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m")
            
            end_query_date = f"{year} 12 31"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            corporations = data["corporations"]
            for corp in corporations:
                try:
                    corp_data.update({"corporation_id":corp})
                    multipc_match_query = {"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}}
                    multi_pc_data = list(self.db.Monthly_uploads.find(multipc_match_query,{"_id":0,"extraction_report_id":0}))
                    if len(multi_pc_data)==0:
                        multi_pc_data = list(self.db.Weekly_uploads.find(multipc_match_query,{"_id":0,"extraction_report_id":0}))
                   
                    if len(multi_pc_data) != 0:
                        pc_set = set()
                        for obj in multi_pc_data:
                            pc_id = obj.get("profit_center_id",None)
                            pc_set.add(pc_id)
                        for pc_id in pc_set:
                            corp_data.update({"profit_center_id":pc_id})
                            corp_res = self.get_monthly_data(corp_data)
                            data_list = corp_res.get("data",None)
                            if data_list:
                                all_corp_res.append(corp_res)
                except Exception as err:
                    pass
            if len(all_corp_res) == 0:
                empty_res = {
                    "data":[],
                    "detail":"No data found",
                    "status_code":400
                }
                return empty_res
            result = {
                "data":all_corp_res,
                "detail":"Data retrieved successfully",
                "status_code":200
            }
            return result
        except Exception as err :
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result    
        
    def all_corps_yearly_data(self,data):
        try:
            all_corp_res = []
            corp_data = {
                "years_selected":data["years_selected"],
                "sheet":data["sheet"]
            }
            
            num = int(data["years_selected"])
            current_year = datetime.now().year
            start_year = current_year - num
            
            
            start_query_date = f"{start_year} 01 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{current_year} 12 31"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            corporations = data["corporations"]
            for corp in corporations:
                try:
                    corp_data.update({"corporation_id":corp})
                    
                    multi_pc_data = list(self.db.Monthly_uploads.find({"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}},
                                                                     {"_id":0,"extraction_report_id":0}))
                    if len(multi_pc_data) != 0:
                        pc_set = set()
                        for obj in multi_pc_data:
                            pc_id = obj.get("profit_center_id",None)
                            pc_set.add(pc_id)
                        for pc_id in pc_set:
                            corp_data.update({"profit_center_id":pc_id})
                            corp_res = self.get_yearly_data(corp_data)
                            data_list = corp_res.get("data",None)
                            if data_list:
                                all_corp_res.append(corp_res)
                except Exception as err:
                    pass   
            if len(all_corp_res) == 0:
                empty_res = {
                    "data":[],
                    "detail":"No data found",
                    "status_code":400
                }
                return empty_res
            result = {
                "data":all_corp_res,
                "detail":"Data retrieved successfully",
                "status_code":200
            }
            return result
        except Exception as err :
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result  
    
    def all_corps_range_data(self,data):
        try:
            all_corp_res = []
            corp_data = {
                "startdate":data["startdate"],
                "enddate":data["enddate"],
                "sheet":data["sheet"]
            }
            
            start_query_date = data["startdate"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["enddate"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")
            
            corporations = data["corporations"]
            for corp in corporations:
                try:
                    corp_data.update({"corporation_id":corp})
                    multi_pc_data = list(self.db.Weekly_uploads.find({"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}},
                                                                     {"_id":0,"extraction_report_id":0}))
                    if len(multi_pc_data) != 0:
                        pc_set = set()
                        for obj in multi_pc_data:
                            pc_id = obj.get("profit_center_id",None)
                            pc_set.add(pc_id)
                        for pc_id in pc_set:
                            corp_data.update({"profit_center_id":pc_id})
                            corp_res = self.new_get_range_data(corp_data)
                            data_list = corp_res.get("data",None)
                            if data_list:
                                all_corp_res.append(corp_res)
                except Exception as err:
                    pass   
            if len(all_corp_res) == 0:
                empty_res = {
                    "data":[],
                    "detail":"No data found",
                    "status_code":400
                }
                return empty_res
            result = {
                "data":all_corp_res,
                "detail":"Data retrieved successfully",
                "status_code":200
            }
            return result
        except Exception as err :
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result
    
    
    def fillMissingData(self,result,start_date,end_date):

        all_dates = []
        current_date = start_date
        while current_date <= end_date:
            all_dates.append(current_date)
            current_date += timedelta(days=1)
            
        result_dict = {res['timestamp']: res for res in result}

        final_result = []
        for date in all_dates:
            if date in result_dict.keys():
                final_result.append(result_dict[date])
            else:
                final_result.append({
                    'timestamp': date,
                    'metadata':{'label': 'Your rank'},
                    'change': None
                })    
        return final_result
        
    def daily_data_from_monthreport(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            year = int(data["year"])
            month = int(data["month"])
            
            date = calendar.monthrange(year,month)[1]
            
            start_query_date = f"{year} {month} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{year} {month} {date}"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            obj = self.db.Monthly_uploads.find_one(obj_id_query)
            
            if obj is None:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objId = obj["extraction_report_id"]
            
            pipeline = [
                {"$match":{"metadata.str_id":ObjectId(str_id_objId),"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj}}},
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            
            documents = list(self.db.Weekly_uploads.find(obj_id_query))
            if len(documents) == 0:
                raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
            str_id_objIds = [doc["extraction_report_id"] for doc in documents]
            
            pipeline1 = [
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id":{"$in":str_id_objIds},"metadata.label":"Your rank","tag_type":{"$exists":False}}},
                {"$group":{"_id":{"timestamp":"$timestamp"},"label":{"$first":"$metadata.label"},"uniqueRank":{"$first":"$change"}}},
                {"$project":{"_id":0,"timestamp": "$_id.timestamp","metadata.label":"$label","change": "$uniqueRank"}},
                {"$sort":{"timestamp":1}}   
            ]
            
            response_data = {"corporation_name":obj["corporation_name"],
                            "corporation_id":obj["corporation_id"],
                            "profitcenter_id":obj["profit_center_id"],
                            "status_code":200,
                            "detail":"Data retrieved successfully"
                            }
            result={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr_DailyByMonth","occupancy_DailyByMonth","revpar_DailyByMonth"]
            data ={}
            for collection_name in coll_names:
                data.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
                
                coll = collection_name.split('_')[0]
                ranks_data = list(self.db[coll].aggregate(pipeline1))
                ranks_with_nulldata = self.fillMissingData(ranks_data,start_ts_obj,end_ts_obj)
                data[collection_name].extend(ranks_with_nulldata)
            result.update({"data":data})    
            return result
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result
        
    def fillMissingKPIdata(self,result,start_date,end_date,label,tag):

        all_dates = []
        current_date = start_date
        while current_date <= end_date:
            all_dates.append(current_date)
            if tag == "monthly":
                max_days = calendar.monthrange(current_date.year,current_date.month)[1]
                current_date += timedelta(days=max_days)
                continue
            current_date += timedelta(days=1)
            
        result_dict = {res['timestamp']: res for res in result}

        final_result = []
        for date in all_dates:
            if date in result_dict.keys():
                final_result.append(result_dict[date])
            else:
                final_result.append({
                    'timestamp': date,
                    'label': label,
                    'change': None
                })    
        return final_result
    
        
    def get_str_kpi_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            
            start_query_date = data["startdate"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["enddate"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")

            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,detail="searching dates are invalid")
            
            dates_difference = end_ts_obj - start_ts_obj
            difference_days = dates_difference.days
            max_days = calendar.monthrange(start_ts_obj.year,start_ts_obj.month)[1]
            
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            
            response_data = {}
                
            if difference_days <= 7:    
                obj = self.db.Weekly_uploads.find_one(obj_id_query)
                str_id_objIds = [obj["extraction_report_id"]]
                if obj is None:
                    raise HTTPException(status_code=400,detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
      
                response_data.update({"corporation_name":obj["corporation_name"],
                                "corporation_id":obj["corporation_id"],
                                "profitcenter_id":obj["profit_center_id"],
                                "str_id":obj["str_id"],
                                })
                
            elif difference_days > 7 and difference_days <= max_days:    
                documents = list(self.db.Weekly_uploads.find(obj_id_query))
                if len(documents) == 0:
                    raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
                str_id_objIds = [doc["extraction_report_id"] for doc in documents]
                response_data.update({"corporation_name":documents[0]["corporation_name"],
                            "corporation_id":documents[0]["corporation_id"],
                            "profitcenter_id":documents[0]["profit_center_id"],
                            "str_id":documents[0]["str_id"],
                            })
            else:   
                documents = list(self.db.Monthly_uploads.find(obj_id_query))
                if len(documents) == 0:
                    raise HTTPException(status_code=400,
                                    detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
                str_id_objIds = [doc["extraction_report_id"] for doc in documents]
                response_data.update({"corporation_name":documents[0]["corporation_name"],
                            "corporation_id":documents[0]["corporation_id"],
                            "profitcenter_id":documents[0]["profit_center_id"],
                            "str_id":documents[0]["str_id"],
                            })
                
            result ={}
            
            index_dict = {
                "adr":"Index (ARI)",
                "occupancy":"Index (MPI)",
                "revpar":"Index (RGI)"
            }
            
            coll_names = ["adr","occupancy","revpar"]
            data = {}
            for collection_name in coll_names:
                lables_list = ["Competitive Set"]
                if difference_days <= max_days:
                    lables_list = ["Comp Set"]
                    
                lables_list.append(index_dict[collection_name])
                for label in lables_list:
                    pipeline = [
                                    {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},
                                               "metadata.str_id": {"$in": str_id_objIds},
                                               "metadata.label":label,
                                               "tag_type":{"$exists":False}}},
                                    {"$group":{"_id":{"timestamp":"$timestamp","label":"$metadata.label"},"unique_change":{"$first":"$change"}}},
                                    {"$project": {"_id": 0,"timestamp": "$_id.timestamp","label":"$_id.label","change": "$unique_change"}},
                                    {"$sort":{"timestamp":1}}
                                ]
                    obj_key = f"{collection_name}_{label}"
                    if difference_days <= max_days:
                        kpi_data = list(self.db[collection_name].aggregate(pipeline))
                        kpi_data_filledwithnulls = self.fillMissingKPIdata(kpi_data,start_ts_obj,end_ts_obj,label,"weekly")
                        data.update({obj_key:kpi_data_filledwithnulls})
                    else:
                        month_collection = collection_name+"_monthlyAvgs"
                        kpi_data = list(self.db[month_collection].aggregate(pipeline))
                        kpi_data_filledwithnulls = self.fillMissingKPIdata(kpi_data,start_ts_obj,end_ts_obj,label,"monthly")
                        data.update({obj_key:kpi_data_filledwithnulls}) 
            result.update(response_data)
            result.update({"status_code":200,"detail":"Data retrieved successfully"})               
            result.update({"data":data})        
            return result 
            
                    
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id},{"_id":0,"extraction_report_id":0})
            if res:
                corp_name = res["corporation_name"]
                result.update({
                    "corporation_name":corp_name,
                    "detail": f"No data found for {corp_name}",
                })
            else:
                result.update({"detail":"No data found"})    
            return result
        except Exception as err:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(err)
            }
            return result
        
   
        
    def get_report_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            
            start_query_date = data["startdate"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["enddate"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")
            
            today = datetime.today()
            if start_ts_obj >= today or end_ts_obj >= today:
                raise HTTPException(status_code=400,detail="searching dates are invalid")
            
            dates_difference = end_ts_obj - start_ts_obj
            difference_days = dates_difference.days
            max_days = calendar.monthrange(start_ts_obj.year,start_ts_obj.month)[1]
            
            viewby = data.get("viewBy",None)
            
            if viewby == 'Day' :
                result = self.get_week_data(data,diff_days=difference_days)
                return result
            
            
              
        except Exception as e:
            pass