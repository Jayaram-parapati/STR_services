from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile,Response,Query
from datetime import datetime, timedelta
from bson import ObjectId

from mongo_service import connect_to_MongoDb


class APIendpoints(connect_to_MongoDb):
    def __init__(self):
        super().__init__()
        
    def get_week_data(self):
        pass
    def get_wekly_data(self):
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
    def get_year_data(self):
        pass
    def get_range_data(self):
        pass