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
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            
            widget = data.get("widget",None)
            if widget:
                pc_list = data.get("profit_centers",[])
                if len(pc_list) != 0:
                    obj_id_query.update({"profit_center_id":{"$in":pc_list}})
                multi_pc_objs = list(self.db.Weekly_uploads.find(obj_id_query))
                weekly_str_id_objIds = [doc["extraction_report_id"] for doc in multi_pc_objs]
                pipeline=[
                {"$match":{"timestamp":{"$eq":end_ts_obj},"metadata.str_id":{"$in":weekly_str_id_objIds},"tag_type":{"$eq":"Current Week"} }},
                {"$group":{"_id":"$metadata.label",
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
                                    }}},
                            "denominator":{
                                "$avg":{
                                    "$cond":{
                                        "if":{"$eq":["$metadata.label", "Your rank"]},
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] } }
                                            }
                                        },
                                        "else":1
                                    }
                                }
                            }
                           }},
                {"$project":{"_id":0,"label":"$_id",
                             "change":{
                                "$cond":{
                                    "if":{"$ne":["$_id","Your rank"]},
                                    "then":"$avg_change",
                                    "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of ",{"$toString":{"$round":"$denominator"}}]}}},
                             }}
                ]
                obj = multi_pc_objs[0]
    
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
            res = self.db.Weekly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
    
    def fillMissingWeeklyData(self,result):
        
        all_dates = set()
        for obj in result:
            all_dates.add(obj['timestamp'])
        for date in all_dates:
            week_start_date = date-timedelta(days=6)
            objs = [obj for obj in result if obj['timestamp'] == date]
            if not any ([obj for obj in objs if obj['metadata']['label'] == 'Your rank']):
                result.append({
                        'timestamp': date,
                        'metadata':{'label': 'Your rank'},
                        'change': None,
                        'week_range':[week_start_date,date]
                    })  
                
        return result
        
    def new_get_weekly_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
         
            start_query_date = data["week_start_date"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["week_end_date"]
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
            
            start_ts_obj_for_search = datetime.strptime(f"{start_ts_obj.year} {start_ts_obj.month} 01","%Y %m %d") 
            max_days = calendar.monthrange(end_ts_obj.year,end_ts_obj.month)[1]
            end_ts_obj_for_search = datetime.strptime(f"{end_ts_obj.year} {end_ts_obj.month} {max_days}","%Y %m %d")    
            
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj_for_search,"$lte": end_ts_obj_for_search}},
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
                matched_objs_query = {"timestamp":{"$in":all_dates},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$eq":"Current Week"}}
                matched_objs = list(self.db[collection_name].find(matched_objs_query))
                # found_dates = [obj["timestamp"] for obj in matched_objs]
                ts_from_weeks = [timestamp for doc in weekly_documents for timestamp in self.generate_timestamps(doc["date_range"][0], doc["date_range"][1]) if timestamp in all_dates]

                missing_dates_from_weekly = [date for date in all_dates if not date in ts_from_weeks]
                matched_ids = [obj['_id'] for obj in matched_objs]
                monthly_pipeline = [
                    {"$match":{"metadata.str_id":{"$in":monthly_str_id_objIds},"timestamp":{"$in":missing_dates_from_weekly}}},
                    {
                        "$group": {
                            "_id": {
                                "year": {"$year": "$timestamp"},
                                "week": {"$week": "$timestamp"},
                                "label": "$metadata.label",
                            },
                            "avg_change": {"$avg": "$change"},
                            "first_TS":{"$first":"$timestamp"},
                            "last_TS":{"$last":"$timestamp"},
                        }
                    },
                    {
                        "$addFields": {
                            "dayOfWeekNumber_first": { "$dayOfWeek": "$first_TS" },
                            "dayOfWeekNumber_last": { "$dayOfWeek": "$last_TS" }
                        }
                    },
                    {
                        "$addFields":{
                            "saturday":{
                                "$cond":{
                                    "if":{"$eq":["$dayOfWeekNumber_last",7]},
                                    "then":"$last_TS",
                                    "else":{
                                        "$cond":{
                                            "if":{"$eq":["$dayOfWeekNumber_first",1]},
                                            "then":{"$dateAdd":{
                                                        "startDate":"$first_TS",
                                                        "unit":"day",
                                                        "amount":6
                                                        }},
                                            "else":None
                                        }
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            # "first_TS":"$first_TS",
                            # "last_TS":"$last_TS",
                            "timestamp":"$saturday",
                            "metadata.label": "$_id.label",
                            "change": "$avg_change",
                            "week_range":[
                                {"$dateSubtract":{
                                    "startDate":"$saturday",
                                    "unit":"day",
                                    "amount":6
                                }},
                                "$saturday"
                            ]  
                           
                        }
                    },
                    {"$sort": {"timestamp": 1}},
                ]
                pipeline = [
                    {"$match": {"_id": {"$in": matched_ids}}},
                    {"$project":{"_id":0,"change_rate":0,"metadata.str_id":0,"tag_type":0}},
                    {"$unionWith":{
                        "coll":collection_name+'_DailyByMonth',
                        "pipeline":monthly_pipeline
                    }},
                    {"$set":{"metadata.label":{"$cond":{
                        "if": { "$eq": ["$metadata.label", "Competitive Set"] },
                        "then": "Comp Set",
                        "else": "$metadata.label"
                    }}}},
                    {"$group":{
                        "_id":{"timestamp":"$timestamp","label":"$metadata.label"},
                        "change":{"$first":"$change"},
                        "week_range":{"$first":"$week_range"}
                    }},
                    {"$project":{
                        "_id":0,
                        "timestamp":"$_id.timestamp",
                        "metadata.label":"$_id.label",
                        "change":"$change",
                        "week_range":"$week_range"
                    }},
                    {"$sort":{"timestamp":1}} 
                ]
                
                if collection:
                    res = {}
                    res.update(response_data)
                    res.update({
                                "sheet":collection,
                                "data":list(self.db[collection_name].aggregate(pipeline))
                                })
                    if any(obj for obj in res['data'] if obj['metadata']['label'] == 'Your rank'):
                        res["data"] = self.fillMissingWeeklyData(res["data"])
                    return res
                single_res = list(self.db[collection_name].aggregate(pipeline))
                if any(obj for obj in single_res if obj['metadata']['label'] == 'Your rank'):
                    single_res = self.fillMissingWeeklyData(single_res)
                result.update({collection_name:single_res})
            # print(result)
            return result
         
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Weekly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
            
            widget = data.get("widget",None)
            if widget:
                pc_list = data.get("profit_centers",[])
                if len(pc_list) != 0:
                    obj_id_query.update({"profit_center_id":{"$in":pc_list}})
                multi_pc_objs = list(self.db.Monthly_uploads.find(obj_id_query))
                monthlyy_str_id_objIds = [doc["extraction_report_id"] for doc in multi_pc_objs]
                pipeline=[
                {"$match":{"timestamp":{"$eq":start_ts_obj},"metadata.str_id": {"$in":monthlyy_str_id_objIds},"tag_type":{"$exists":False}}},
                {"$set":{"metadata.label":{"$cond":{
                        "if": { "$eq": ["$metadata.label", "Competitive Set"] },
                        "then": "Comp Set",
                        "else": "$metadata.label"
                }}}},
                {"$set":{"metadata.label":{"$cond":{
                    "if": { "$eq": ["$metadata.label", "Rank"] },
                    "then": "Your rank",
                    "else": "$metadata.label"
                }}}},
                {"$group":{"_id":"$metadata.label",
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
                                    }}},
                            "denominator":{
                                "$avg":{
                                    "$cond":{
                                        "if":{"$eq":["$metadata.label", "Your rank"]},
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] } }
                                            }
                                        },
                                        "else":1
                                    }
                                }
                            }
                           }},
                {"$project":{"_id":0,"label":"$_id",
                             "change":{
                             "$cond":{
                                "if":{"$ne":["$_id","Your rank"]},
                                "then":"$avg_change",
                                "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of ",{"$toString":{"$round":"$denominator"}}]}}},
                            }}
                ]
                obj = multi_pc_objs[0]
            
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
                if widget:
                    result.update({collection_name.split('_')[0]:list(self.db[collection_name].aggregate(pipeline))})
                    continue 
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
                result.update({collection_name+"_glance":list(self.db[collection_name].aggregate(pipeline1))})
            # print(result)
            return result
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
    
    def generate_timestamps(self,start_date, end_date):
        timestamps = []
        current_date = start_date
        while current_date <= end_date:
            timestamps.append(current_date)
            current_date += timedelta(days=1)
        return timestamps
        
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
            
            start_ts = data.get("start_obj_from_report",None)
            end_ts = data.get("end_obj_from_report",None)
            if start_ts and end_ts:
                start_ts_obj = datetime.strptime(f"{start_ts.year} {start_ts.month} 01","%Y %m %d") 
                max_days = calendar.monthrange(end_ts.year,end_ts.month)[1]
                end_ts_obj = datetime.strptime(f"{end_ts.year} {end_ts.month} {max_days}","%Y %m %d")
                
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
            ts_from_months = [timestamp for doc in monthly_documents for timestamp in self.generate_timestamps(doc["date_range"][0], doc["date_range"][1])]
            ts_to_search_in_weekly = [timestamp for doc in weekly_documents for timestamp in self.generate_timestamps(doc["date_range"][0], doc["date_range"][1]) if timestamp not in ts_from_months and timestamp.year==year]

            # uploaded_timestamps.extend([doc["date_range"][1] for doc in weekly_documents])
            
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
              
            result ={}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            collection = data.get("sheet",None)
            if collection:
                coll_names = [collection+"_monthlyAvgs"]
                
            viewby = data.get("viewBy",None)
              
            for collection_name in coll_names:
                weekly_collection_name = collection_name.split('_')[0]
                
                matched_objs = list(self.db[collection_name].find({"timestamp":{"$in":uploaded_timestamps},"metadata.str_id": {"$in": monthly_str_id_objIds},"tag_type":{"$exists":False}}))
                found_dates = [obj["timestamp"] for obj in matched_objs]
                missing_dates_from_monthly = [date for date in uploaded_timestamps if not date in found_dates]
                matched_ids = [obj['_id'] for obj in matched_objs]
                weekly_pipeline = [
                    {"$match":{"timestamp":{"$in": ts_to_search_in_weekly},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$eq":"Current Week"}}},
                    {"$unionWith": {
                                                "coll": weekly_collection_name + "_ss",
                                                "pipeline": [{"$match":{"timestamp":{"$in":missing_dates_from_monthly},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$eq":"Current Week"},"metadata.label":{"$ne":"Index"}}}]
                                            }},
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
                                    }}},
                            "avg_change_rate":{
                                "$avg":{
                                    "$cond": {
                                        "if": { "$ne": ["$metadata.label", "Your rank"] },
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": ["$change_rate", "null"] },
                                                "then": 0,
                                                "else": { "$toDouble": "$change_rate" }
                                                }},
                                        "else": {
                                            "$cond": {
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change_rate", " of "] }, 0] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change_rate", " of "] }, 0] } }
                                            }
                                        }
                                    }}},
                            "denominator":{
                                "$avg":{
                                    "$cond":{
                                        "if":{"$eq":["$metadata.label", "Your rank"]},
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] } }
                                            }
                                        },
                                        "else":1
                                    }
                                }
                            }
                        }
                    },
                    {
                     "$project":{
                         "_id": 0,
                        "timestamp": {
                            "$dateFromParts": {"year": "$_id.year","month": "$_id.month"}
                        },
                        "metadata.label": "$_id.label",
                        "change":{
                            "$cond":{
                                "if":{"$ne":["$_id.label","Your rank"]},
                                "then":"$avg_change",
                                "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of ",{"$toString":{"$round":"$denominator"}}]}}},
                        "change_rate":{
                            "$cond":{
                                "if":{"$ne":["$_id.label","Your rank"]},
                                "then":"$avg_change_rate",
                                "else":{"$concat":[{"$toString":{"$round":"$avg_change_rate"}}," of ",{"$toString":{"$round":"$denominator"}}]}}}
                     }   
                    },
                ]
                pipeline = [
                    {"$match": {"_id": {"$in": matched_ids}}},
                    {"$set":{"metadata.label":{"$cond":{
                        "if": { "$eq": ["$metadata.label", "Competitive Set"] },
                        "then": "Comp Set",
                        "else": "$metadata.label"
                    }}}},
                    {"$set":{"metadata.label":{"$cond":{
                        "if": { "$eq": ["$metadata.label", "Rank"] },
                        "then": "Your rank",
                        "else": "$metadata.label"
                    }}}},
                    {"$unionWith": {
                        "coll":weekly_collection_name,
                        "pipeline": weekly_pipeline
                    }}, 
                    {"$group":{"_id":{"timestamp":"$timestamp","label":"$metadata.label"},"unique_change":{"$first":"$change"},"unique_change_rate":{"$first":"$change_rate"}}},
                    {"$project": {"_id": 0,"timestamp": "$_id.timestamp","label":"$_id.label","change": "$unique_change","change_rate": "$unique_change_rate"}},
                    {"$sort":{"timestamp":1}}
                ]
                
                if viewby is None:
                    weekly_pipeline[0]["$match"].update({"tag_type":{"$exists":False}})
                    weekly_pipeline.pop(1)
                    weekly_pipeline[1]["$group"].pop("avg_change_rate")
                    weekly_pipeline[2]["$project"].pop("change_rate")
                    pipeline[4]["$group"].pop("unique_change_rate")
                    pipeline[5]["$project"].pop("change_rate")
                    
                
                if collection:
                    res = {}
                    res.update(response_data)
                    res.update({
                                "sheet":collection,
                                "data":list(self.db[collection_name].aggregate(pipeline)),
                                })
                    return res    
       
                result.update({weekly_collection_name:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
        
    def new_get_yearly_data(self,data):
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
            
            monthly_documents = list(self.db.Monthly_uploads.find(obj_id_query))
            weekly_documents = list(self.db.Weekly_uploads.find(obj_id_query))
            
            monthly_str_id_objIds = [doc["extraction_report_id"] for doc in monthly_documents]
            weekly_str_id_objIds = [doc["extraction_report_id"] for doc in weekly_documents]
            
            uploaded_timestamps = [doc["date_range"][0] for doc in monthly_documents]
            uploaded_timestamps.extend([doc["date_range"][1] for doc in weekly_documents])
            
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
       
            result = {}
            response_data.update({"status_code":200,"detail":"Data retrieved successfully"})
            result.update(response_data)
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            collection = data.get("sheet",None)
            if collection:
                coll_names = [collection+"_monthlyAvgs"]
            for collection_name in coll_names:
                weekly_collection_name = collection_name.split('_')[0]
                
                matched_objs = list(self.db[collection_name].find({"timestamp":{"$in":uploaded_timestamps},"metadata.str_id": {"$in": monthly_str_id_objIds},"tag_type":{"$exists":False}}))
                found_dates = [obj["timestamp"] for obj in matched_objs]
                missing_dates_from_monthly = [date for date in uploaded_timestamps if not date in found_dates]
                matched_ids = [obj['_id'] for obj in matched_objs]
                weekly_pipeline = [
                    {"$match":{"timestamp":{"$in": missing_dates_from_monthly},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$eq":"Current Week"}}},
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
                                    }}},
                            "denominator":{
                                "$avg":{
                                    "$cond":{
                                        "if":{"$eq":["$metadata.label", "Your rank"]},
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] } }
                                            }
                                        },
                                        "else":1
                                    }
                                }
                            }
                        }
                    },
                    {
                     "$project":{
                         "_id": 0,
                        "timestamp": {
                            "$dateFromParts": {"year": "$_id.year","month": "$_id.month"}
                        },
                        "metadata.label": "$_id.label",
                        "change":{
                            "$cond":{
                                "if":{"$ne":["$_id.label","Your rank"]},
                                "then":"$avg_change",
                                "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of ",{"$toString":{"$round":"$denominator"}}]}}}
                     }   
                    },
                ]
                pipeline = [
                    {"$match": {"_id": {"$in": matched_ids}}},
                    {"$set":{"metadata.label":{"$cond":{
                        "if": { "$eq": ["$metadata.label", "Competitive Set"] },
                        "then": "Comp Set",
                        "else": "$metadata.label"
                    }}}},
                    {"$set":{"metadata.label":{"$cond":{
                        "if": { "$eq": ["$metadata.label", "Rank"] },
                        "then": "Your rank",
                        "else": "$metadata.label"
                    }}}},
                    {"$unionWith": {
                        "coll":weekly_collection_name,
                        "pipeline": weekly_pipeline
                    }}, 
                    {"$group":{
                        "_id":{"year":{"$year":"$timestamp"},"label":"$metadata.label"},
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
                                }}},
                        "denominator":{
                                "$avg":{
                                    "$cond":{
                                        "if":{"$eq":["$metadata.label", "Your rank"]},
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] } }
                                            }
                                        },
                                        "else":1
                                    }
                                }
                            }
                        }},
                    {"$project":{
                        "_id":0,
                        "year":"$_id.year",
                        "label":"$_id.label",
                        "change":{
                            "$cond":{
                                "if":{"$ne":["$_id.label","Your rank"]},
                                "then":"$avg_change",
                                "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of ",{"$toString":{"$round":"$denominator"}}]}}}}},
                    {"$sort":{"year":1}}
                ]
                if collection:
                    res = {}
                    res.update(response_data)
                    res.update({
                                "sheet":collection_name,
                                "data":list(self.db[collection_name].aggregate(pipeline)),
                                })
                    return res    
       
                result.update({weekly_collection_name:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
            
            start_ts_obj_for_search = datetime.strptime(f"{start_ts_obj.year} {start_ts_obj.month} 01","%Y %m %d") 
            max_days = calendar.monthrange(end_ts_obj.year,end_ts_obj.month)[1]
            end_ts_obj_for_search = datetime.strptime(f"{end_ts_obj.year} {end_ts_obj.month} {max_days}","%Y %m %d")
            
            obj_id_query = {
                "corporation_id":corp_id,
                "date_range":{"$elemMatch": {"$gte": start_ts_obj_for_search,"$lte": end_ts_obj_for_search}},
                "delete_status":0,
            }
            if pc_id:
                obj_id_query.update({"profit_center_id":pc_id})
            widget = data.get("widget",None)
            if widget:
                pc_list = data.get("profit_centers",[])
                if len(pc_list) != 0:
                    obj_id_query.update({"profit_center_id":{"$in":pc_list}})
                    
                
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
                matched_objs_query = {"timestamp":{"$in":all_dates},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$exists":False},"metadata.label":{"$ne":"Your rank"}}
                viewby = data.get("viewBy",None)
                if viewby or widget:
                    matched_objs_query.pop("metadata.label")
                matched_objs = list(self.db[collection_name].find(matched_objs_query))
                found_dates = [obj["timestamp"] for obj in matched_objs]
                missing_dates_from_weekly = [date for date in all_dates if not date in found_dates]
                matched_ids = [obj['_id'] for obj in matched_objs]
                pipeline = [
                    {"$match": {"_id": {"$in": matched_ids}}},
                    {"$unionWith": {
                        "coll": collection_name + "_DailyByMonth",
                        "pipeline": [
                            {"$match": {"timestamp": {"$in": missing_dates_from_weekly}, "metadata.str_id": {"$in": monthly_str_id_objIds}}}
                        ]
                    }},
                    {"$set":{"metadata.label":{"$cond":{
                        "if": { "$eq": ["$metadata.label", "Competitive Set"] },
                        "then": "Comp Set",
                        "else": "$metadata.label"
                    }}}},
                    
                    {"$group": {"_id": "$metadata.label","unique_changes": {"$addToSet": {"timestamp": "$timestamp","change": "$change"}}}},
                    {"$project": {"_id": 0,"label": "$_id","change": {"$avg": "$unique_changes.change"}}}
                ]
                if collection:
                    res = {}
                    res.update(response_data)
                    res.update({
                                "sheet":collection_name,
                                "data":list(self.db[collection_name].aggregate(pipeline)),
                                })
                    return res
              
                if viewby or widget:
                    if viewby == 'Day':
                        marketscale_stage = {"$unionWith": {
                                                "coll": collection_name + "_ss",
                                                "pipeline": [{"$match":{"timestamp":{"$in":all_dates},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$exists":False},"metadata.label":{"$ne":"Index"}}}]
                                            }}
                        pipeline.insert(1,marketscale_stage)
                        groupstage_query ={"_id":{"timestamp":"$timestamp","label":"$metadata.label"},"change":{"$first":"$change"},"change_rate":{"$first":"$change_rate"}}
                        pipeline[4]["$group"].update(groupstage_query)
                        projectstage_query ={"_id":0,"timestamp":"$_id.timestamp","label":"$_id.label","change":"$change","change_rate":"$change_rate"}
                        pipeline[5]["$project"].update(projectstage_query)
                        pipeline.insert(6,{"$sort":{"timestamp":1}})
                    if viewby == 'Week' or viewby == 'Year' or widget:
                        marketscale_stage = {"$unionWith": {
                                                "coll": collection_name + "_ss",
                                                "pipeline": [{"$match":{"timestamp":{"$in":all_dates},"metadata.str_id": {"$in": weekly_str_id_objIds},"tag_type":{"$exists":False},"metadata.label":{"$ne":"Index"}}}]
                                            }}
                        pipeline.insert(1,marketscale_stage)
                        groupstage_query ={
                            "_id": {
                                "year": {"$year": "$timestamp"},
                                "week": {"$week": "$timestamp"},
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
                                    }}},
                            "avg_change_rate":{
                                "$avg":{
                                    "$cond": {
                                        "if": { "$ne": ["$metadata.label", "Your rank"] },
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": ["$change_rate", "null"] },
                                                "then": 0,
                                                "else": { "$toDouble": "$change_rate" }
                                                }},
                                        "else": {
                                            "$cond": {
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change_rate", " of "] }, 0] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change_rate", " of "] }, 0] } }
                                            }
                                        }
                                    }}},
                            "denominator":{
                                "$avg":{
                                    "$cond":{
                                        "if":{"$eq":["$metadata.label", "Your rank"]},
                                        "then":{
                                            "$cond":{
                                                "if": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] }, "null"] },
                                                "then": 0,
                                                "else": { "$toInt": { "$arrayElemAt": [{ "$split": ["$change", " of "] }, 1] } }
                                            }
                                        },
                                        "else":1
                                    }
                                }
                            }
                        }
                        pipeline[4]["$group"].update(groupstage_query)
                        projectstage_query ={
                            "_id": 0,
                            "timestamp": {
                                "$dateFromParts": {"isoWeekYear": "$_id.year","isoWeek": "$_id.week","isoDayOfWeek": 0}
                            },
                            "label": "$_id.label",
                            "change":{
                                "$cond":{
                                    "if":{"$ne":["$_id.label","Your rank"]},
                                    "then":"$avg_change",
                                    "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of ",{"$toString":{"$round":"$denominator"}}]}}},
                            "change_rate":{
                                "$cond":{
                                    "if":{"$ne":["$_id.label","Your rank"]},
                                    "then":"$avg_change_rate",
                                    "else":{"$concat":[{"$toString":{"$round":"$avg_change_rate"}}," of ",{"$toString":{"$round":"$denominator"}}]}}},
                            # "week_range": [
                            #                 {"$dateToString": {"date": {"$dateFromParts": {"isoWeekYear": "$_id.year","isoWeek": "$_id.week","isoDayOfWeek": 0}}}},
                            #                 {"$dateToString": {"date": {"$dateFromParts": {"isoWeekYear": "$_id.year","isoWeek": "$_id.week","isoDayOfWeek": 6}}}},  
                            #             ] 
                        }
                        pipeline[5]["$project"].update(projectstage_query)
                        pipeline.insert(6,{"$sort":{"timestamp":1}})
                        
                        if widget:
                            pipeline.pop(1)
                            pipeline[3]["$group"].update({"_id":"$metadata.label"})
                            pipeline[3]["$group"].pop("avg_change_rate")
                            pipeline[4]["$project"].pop("timestamp")
                            pipeline[4]["$project"].pop("change_rate")
                            project_query ={
                                "_id":0,
                                "label":"$_id",
                                "change":{
                                    "$cond":{
                                        "if":{"$ne":["$_id","Your rank"]},
                                        "then":"$avg_change",
                                        "else":{"$concat":[{"$toString":{"$round":"$avg_change"}}," of ",{"$toString":{"$round":"$denominator"}}]}}},
                            }
                            pipeline[4]["$project"].update(project_query)
                        
                        if viewby == 'Year':
                            pipeline[4]["$group"]["_id"].pop("week")
                            pipeline[5]["$project"]["timestamp"].update({"$dateFromParts":{"year": "$_id.year","month":1,"day":1}})
              
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
            if not any(result[key] for key in ['adr','occupancy','revpar']):
                raise HTTPException(status_code=400,
                                    detail="No data found")   
            return result

        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Weekly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
            
            start_ts_obj_for_search = datetime.strptime(f"{start_ts_obj.year} {start_ts_obj.month} 01","%Y %m %d") 
            max_days = calendar.monthrange(end_ts_obj.year,end_ts_obj.month)[1]
            end_ts_obj_for_search = datetime.strptime(f"{end_ts_obj.year} {end_ts_obj.month} {max_days}","%Y %m %d")
            
            corporations = data["corporations"]
            for corp in corporations:
                try:
                    corp_data.update({"corporation_id":corp})
                    multipc_match_query = {"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}}
                    multipc_match_query_for_month = {"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj_for_search,"$lte": end_ts_obj_for_search}}}
                    multi_pc_data = list(self.db.Weekly_uploads.find(multipc_match_query,{"_id":0,"extraction_report_id":0}))
                    multi_pc_data.extend(self.db.Monthly_uploads.find(multipc_match_query_for_month,{"_id":0,"extraction_report_id":0}))
          
                    if len(multi_pc_data) != 0:
                        pc_set = set()
                        for obj in multi_pc_data:
                            pc_id = obj.get("profit_center_id",None)
                            pc_set.add(pc_id)
                        for pc_id in pc_set:
                            corp_data.update({"profit_center_id":pc_id})
                            corp_res = self.new_get_weekly_data(corp_data)
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
                    multi_pc_data.extend(self.db.Weekly_uploads.find(multipc_match_query,{"_id":0,"extraction_report_id":0}))
                   
                    if len(multi_pc_data) != 0:
                        pc_set = set()
                        for obj in multi_pc_data:
                            pc_id = obj.get("profit_center_id",None)
                            pc_set.add(pc_id)
                        for pc_id in pc_set:
                            corp_data.update({"profit_center_id":pc_id})
                            corp_res = self.new_get_monthly_data(corp_data)
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
                    multi_pc_match_query = {"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj,"$lte": end_ts_obj}}}
                    multi_pc_data = list(self.db.Monthly_uploads.find(multi_pc_match_query,{"_id":0,"extraction_report_id":0}))
                    multi_pc_data.extend(self.db.Weekly_uploads.find(multi_pc_match_query,{"_id":0,"extraction_report_id":0}))
                    
                    if len(multi_pc_data) != 0:
                        pc_set = set()
                        for obj in multi_pc_data:
                            pc_id = obj.get("profit_center_id",None)
                            pc_set.add(pc_id)
                        for pc_id in pc_set:
                            corp_data.update({"profit_center_id":pc_id})
                            corp_res = self.new_get_yearly_data(corp_data)
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
            
            start_ts_obj_for_search = datetime.strptime(f"{start_ts_obj.year} {start_ts_obj.month} 01","%Y %m %d") 
            max_days = calendar.monthrange(end_ts_obj.year,end_ts_obj.month)[1]
            end_ts_obj_for_search = datetime.strptime(f"{end_ts_obj.year} {end_ts_obj.month} {max_days}","%Y %m %d")
            
            corporations = data["corporations"]
            for corp in corporations:
                try:
                    corp_data.update({"corporation_id":corp})
                    multi_pc_match_query = {"corporation_id":corp,"delete_status":0,"date_range":{"$elemMatch": {"$gte": start_ts_obj_for_search,"$lte": end_ts_obj_for_search}}}
                    multi_pc_data = list(self.db.Weekly_uploads.find(multi_pc_match_query,{"_id":0,"extraction_report_id":0}))
                    multi_pc_data.extend(self.db.Monthly_uploads.find(multi_pc_match_query,{"_id":0,"extraction_report_id":0}))
 
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
            # if len(documents) == 0:
            #     raise HTTPException(status_code=400,
            #                         detail=f"No data found for {corp_id} between {start_query_date} and {end_query_date}")
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
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
        
    def fillMissingReportData(self,data,startDate,endDate,type):
        try:
            all_dates = []
            current_date = startDate
            last_date = endDate
            
            if type == "Week":
                if current_date.isoweekday() != 7:
                    current_date -=timedelta(days=current_date.isoweekday())
                if endDate.isoweekday() != 6:
                    endDate +=timedelta(days=(6-endDate.isoweekday()))
                    
            if type == "Month":
                current_date = datetime.strptime(f"{current_date.year} {current_date.month} 01","%Y %m %d")
                max_days = calendar.monthrange(last_date.year,last_date.month)[1]
                last_date = datetime.strptime(f"{last_date.year} {last_date.month} {max_days}","%Y %m %d")
            
            if type == "Year":
                current_date = datetime.strptime(f"{current_date.year} 01 01","%Y %m %d")
                max_days = calendar.monthrange(last_date.year,last_date.month)[1]
                last_date = datetime.strptime(f"{last_date.year} 01 01","%Y %m %d")
           
            while current_date <= last_date:
                all_dates.append(current_date)
                if type == "Day":
                    current_date += timedelta(days=1)
                if type == "Week":
                    current_date += timedelta(days=7)
                if type == "Month":
                    if current_date.month == 12:
                        current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 1, day=1)
                if type == "Year":
                    current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
                  
            sheets = ["adr","occupancy","revpar"]
            index_dict = {
                "adr":"Index (ARI)",
                "occupancy":"Index (MPI)",
                "revpar":"Index (RGI)"
            }
            marketscale_dict = {
                "adr":"ADR",
                "occupancy":"Occ",
                "revpar":"RevPAR"
            }
            for sheet in sheets:
                sheet_dict = data[sheet]
                dicttt = {(res["timestamp"],res["label"]):res for res in sheet_dict}
                sheet_result = []
                labels = ["My Property","Comp Set","Your rank"]
                labels.append(index_dict[sheet])
                labels.append(marketscale_dict[sheet])
                for date in all_dates:
                    for label in labels:
                        if (date,label) in dicttt:
                            sheet_result.append(dicttt[(date,label)])
                        else:
                            sheet_result.append({
                                "timestamp":date,
                                "label":label,
                                "change":None,
                                "change_rate":None
                            })
                data[sheet] = (sheet_result)
            return data    
    
        except Exception as e:
            result = {
                "status_code":500,
                "detail":"No detail found",
                "error":str(e)
            }
            return result
      
    def get_report_data(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_id = data.get("profit_center_id",None)
            viewby = data.get("viewBy",None)
            
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

            if viewby=='Day' or viewby=='Week':
                result = self.new_get_range_data(data)
                response = self.fillMissingReportData(result,start_ts_obj,end_ts_obj,viewby)
                return response
            
            if viewby=='Month':
                data["start_obj_from_report"]=start_ts_obj 
                data["end_obj_from_report"]=end_ts_obj
                data["year"]=start_ts_obj.year 
                result = self.new_get_monthly_data(data)
                response = self.fillMissingReportData(result,start_ts_obj,end_ts_obj,viewby)
                return response
            
            if viewby=='Year':
                data["start_obj_from_report"]=start_ts_obj 
                data["end_obj_from_report"]=end_ts_obj
                data["years_selected"]=2 
                result = self.new_get_range_data(data)
                response = self.fillMissingReportData(result,start_ts_obj,end_ts_obj,viewby)
                return response
                
    
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
        
    def widget_glance(self,data):
        try:
            corp_id = data["corporation_id"]
            pc_ids_list = data.get("profit_centers",None)
            
            start_query_date = data["startdate"]
            start_ts_obj = datetime.strptime(start_query_date,"%Y-%m-%d")
            
            end_query_date = data["enddate"]
            end_ts_obj = datetime.strptime(end_query_date,"%Y-%m-%d")
            
            today = datetime.today()
            if start_ts_obj >= today:
                raise HTTPException(status_code=400,detail="searching dates are invalid")
            
            data.update({"widget":"widget"})
            dates_difference = end_ts_obj - start_ts_obj
            difference_days = dates_difference.days
            max_days = calendar.monthrange(start_ts_obj.year,start_ts_obj.month)[1]

            
            if difference_days == 6:
                result = self.get_week_data(data)
                if result['status_code'] != 400 :
                    return result
                    
            if difference_days == max_days-1:
                data["year"] = start_ts_obj.year
                data["month"] = start_ts_obj.month
                result = self.get_month_data(data)
                if result['status_code'] != 400 :
                    return result
                
            result = self.new_get_range_data(data)
            return result        
      
        except HTTPException as err:
            result = {
                "status_code":400
            }
            res = self.db.Monthly_uploads.find_one({"corporation_id":corp_id,"delete_status":0},{"_id":0,"extraction_report_id":0})
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
        