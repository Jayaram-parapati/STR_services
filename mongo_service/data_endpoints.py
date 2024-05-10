from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile,Response,Query
from datetime import datetime, timedelta
from bson import ObjectId

from mongo_service import connect_to_MongoDb


class APIendpoints(connect_to_MongoDb):
    def __init__(self):
        super().__init__()
        
    def get_week_data(self):
        pass
    def get_weekly_data(self):
        pass
    def get_month_data(self,data):
        try:
            str_id = data["str_id"]
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
            
            obj = self.db.str_reports.find_one({"str_id": str_id})
            str_id_objId = obj["_id"]

            
            pipeline = [
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": ObjectId(str_id_objId),"tag_type":{"$exists":False}}},
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            pipeline1 = [
                {"$match":{"timestamp":{"$eq":end_ts_obj},"metadata.str_id": ObjectId(str_id_objId),"tag_type":{"$exists":True}}},
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            result={}
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                result.update({collection_name:list(self.db[collection_name].aggregate(pipeline))})
                result[collection_name].append(list(self.db[collection_name].aggregate(pipeline1)))
            # print(result)
            return result
        except Exception as e:
            return {"error":e,status:500}
        
    def get_monthly_data(self,data):
        try:
            str_id = data["str_id"]
            year = int(data["year"])
            
            obj = self.db.str_reports.find_one({"str_id": str_id})
            str_id_objId = obj["_id"]
            
            start_query_date = f"{year} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m")
            
            end_query_date = f"{year} 12"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m")
            
            pipeline=[
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": ObjectId(str_id_objId),"tag_type":{"$exists":False}}},
                {"$project":{"_id":0,"metadata.str_id":0,"change_rate":0}}
            ]
            result ={}
            coll_names = ["adr_monthlyAvgs","occupancy_monthlyAvgs","revpar_monthlyAvgs"]
            for collection_name in coll_names:
                result.update({collection_name.split('_')[0]:list(self.db[collection_name].aggregate(pipeline))})
            # print(result)
            return result
        except Exception as e :
            return{"error":e,status:500} 
        
    def get_yearly_data(self,data):
        try:
            num = int(data["years_selected"])
            current_year = datetime.now().year
            current_month = datetime.now().month
            start_year = current_year - num
            
            str_id = data["str_id"]
            obj = self.db.str_reports.find_one({"str_id": str_id})
            str_id_objId = obj["_id"]
            
            start_query_date = f"{start_year} {current_month} 01"
            start_ts_obj = datetime.strptime(start_query_date,"%Y %m %d")
            
            end_query_date = f"{current_year} {current_month} 01"
            end_ts_obj = datetime.strptime(end_query_date,"%Y %m %d")
            
            pipeline =[
                {"$match":{"timestamp":{"$gte":start_ts_obj,"$lte":end_ts_obj},"metadata.str_id": ObjectId(str_id_objId),"tag_type":{"$exists":False}}},
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
        except Exception as e :
            return{"error":e,status:500}
        
    def get_range_data(self):
        pass