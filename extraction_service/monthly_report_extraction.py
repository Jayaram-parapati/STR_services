import pandas as pd
import sys
import numpy as np
from datetime import datetime
import json
from pymongo import MongoClient
import os
import warnings
import pdb
from mongo_service import connect_to_MongoDb


# Ignore all warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

#! todo: read year from first sheet
year = 2022
month = 'Jan'

weekdays = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
]


config = {
    "required_sheets": ["Table of Contents","Comp","Daily by Month"],
    "meta_rows": {
        "Comp": [range(4)],
        "Daily by Month": [range(4)]
    },
    "split_dfs": {"Comp": True,"Daily by Month": True},
    "save_to_db": True,
}

class Monthly_extraction(connect_to_MongoDb):
    def __init__(self):
        super().__init__()
    

    def consecs(self,ll):
        ol = []
        # cfound = False
        for i, l in enumerate(ll):
            if i < len(ll) - 1 and l != ll[i + 1] - 1:
                ol.append(l)    
        return ol


    def boundaries(self,a, start, end):
        ranges = []
        for i in a:
            ranges.append([start, i])
            start = i
        if start < end:
            ranges.append([start, end])
        return ranges


    def prepare_dfs(self,sheets,xl):
        dfs = []
        # meta_rows = [list() for x in sheets]
        for index, sheet in enumerate(sheets):
            
            if sheet not in config["required_sheets"]:
                continue

            df = xl.parse(sheet, header=None)
            if config["meta_rows"] != None:
                if sheet in config["meta_rows"].keys():
                    for rows in config["meta_rows"][sheet]:
                        # meta_rows[index].append(df.iloc[rows])
                        dfs.append(
                            {"sheet": f"meta-{sheet}", "index": index, "df": df.iloc[rows]}
                        )

                        df.drop(df.index[rows], inplace=True)
                        

            df.reset_index(drop=True, inplace=True)
            
            df.columns = range(df.columns.size)
            

            if sheet in config["split_dfs"].keys():
                df["nancnt"] = df.isnull().sum(axis=1)
                
                maxcnt = max(df["nancnt"])
                maxrows = df.index[df["nancnt"] >= maxcnt].to_list()
                df.drop(columns=["nancnt"], inplace=True)
                

                splitindex = self.consecs(maxrows)
                boxes = self.boundaries(splitindex, 0, df.shape[0])

                for b in boxes:
                    tdf = df.iloc[b[0] : b[1], :]
                    if tdf.size > 1:
                        # print(tdf.size)
                        dfs.append({"sheet": sheet, "index": index, "df": tdf})

                # input("Enter any key...")
            else:
                dfs.append({"sheet": sheet, "index": index, "df": df})
        return dfs

    def prepare_comp_sheet(self,dfo, str_id):
        global year
        global month
        
        df = dfo["df"]
        df.dropna(axis="columns", how="all", inplace=True)
        df.dropna(axis="rows", how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)
    
        if df.shape[0] >= 9:
            collection_name = df.iloc[0, 0]
            if pd.isnull(collection_name):
                df.drop(index=df.index[0], axis=0, inplace=True)
                df.reset_index(drop=True, inplace=True)
                collection_name = df.iloc[0, 0]
            collection_name = "".join([c for c in collection_name if c.isalpha()]).lower()
            collection_name = f"{collection_name}_monthlyAvgs"

            # extract daily data, exclude last 3 columns
            df.iloc[[0]] = df.iloc[[0]].ffill(axis=1)
            df_year_month = df.iloc[0:2,:]
            df_change = df.iloc[2:6, :]
            df_rchange = df.iloc[7:, :]

            

            # shapes should match
            if df_change.shape == df_rchange.shape:
                # print(len(df_change.columns), df.shape[1])
                df_change.columns = range(df_change.shape[1])
                df_rchange.columns = range(df_rchange.shape[1])
                df_year_month.columns = range(df_rchange.shape[1])
                df_change.reset_index(drop=True, inplace=True)
                df_rchange.reset_index(drop=True, inplace=True)
                df_year_month.reset_index(drop=True, inplace=True)
            
                if (
                    collection_name not in self.db.list_collection_names()
                    and config["save_to_db"] == True
                ):
                    # print(f"Creating Collection {collection_name}")
                    self.db.create_collection(
                        collection_name,
                        timeseries={"timeField": "timestamp", "metaField": "metadata"},
                    )
                    # db[collection_name].create_index(
                    #     [("timestamp", 1), ("metadata.label", -1)], unique=True
                    # )
                    # create index if neccessary
                    
                #monthly avg extraction to and save to db    
                for r in range(df_change.shape[0]):
                    label_name =df_change.iloc[r,0]
                    for c in range(1,df_change.shape[1]):
                        record={}
                        record.update({"metadata": {"label": label_name, "str_id": str_id}})
                        if not pd.isna(df_change.iloc[r,c]):
                            change = df_change.iloc[r,c]
                        else: change = 'null'   
                        record.update({"change":change})
                        if not pd.isna(df_rchange.iloc[r,c]):
                            change_rate = df_rchange.iloc[r,c]
                        else:change_rate = 'null'
                        record.update({"change_rate":change_rate})
                        if(type(df_year_month.iloc[0,c]) == int): 
                            date = f"{df_year_month.iloc[0,c]} {(df_year_month.iloc[1,c])}"
                            timestamp = datetime.strptime(date.strip(),"%Y %b")
                            record.update({"timestamp":timestamp})
                        else:
                            date = f"{year} {month}"
                            timestamp = datetime.strptime(date.strip(),"%Y %B")
                            tag_type = df_year_month.iloc[0,c]
                            tag_year = df_year_month.iloc[1,c]
                            record.update({"timestamp":timestamp,"tag_type":tag_type,"tag_year":tag_year})
                        try:
                            if config["save_to_db"] == True:
                                # db[collection_name].insert_one(record)
                                q = {"timestamp":{"$eq":record['timestamp']},
                                    "metadata.str_id":str_id,
                                    "metadata.label":label_name,
                                    "tag_type":{"$exists":False}}
                                if 'tag_type' in record:
                                    q.pop("tag_type") 
                                    q.update({'tag_type':record['tag_type'],'tag_year':record['tag_year']})
                            
                                match_obj = self.db[collection_name].find_one(q)
                                if match_obj:
                                    self.db[collection_name].delete_one({"_id":match_obj["_id"]}) 
                                    self.db[collection_name].insert_one(record)
                                else:self.db[collection_name].insert_one(record)                           
                        except Exception as e:
                            print(e)
                # print(records)

    def prepare_daily_sheet(self,dfo, str_id):
        global year
        df = dfo["df"]
        df.dropna(axis="columns", how="all", inplace=True)
        df.dropna(axis="rows", how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        if df.shape[0] >= 9:
            collection_name = df.iloc[0, 0]
            if pd.isnull(collection_name):
                df.drop(index=df.index[0], axis=0, inplace=True)
                df.reset_index(drop=True, inplace=True)
                collection_name = df.iloc[0, 0]
            collection_name = "".join([c for c in collection_name if c.isalpha()]).lower() 
            collection_name = f"{collection_name}_DailyByMonth"
            
            # extract daily data, exclude last 3 columns
            df.iloc[[0]] = df.iloc[[0]].ffill(axis=1)
            df_change = df.iloc[2:5, :]
            df_rchange = df.iloc[6:9, :]

            

            # shapes should match
            if df_change.shape == df_rchange.shape:
                # print(len(df_change.columns), df.shape[1])
                df_change.columns = range(df_change.shape[1])
                df_rchange.columns = range(df_rchange.shape[1])
                df_change.reset_index(drop=True, inplace=True)
                df_rchange.reset_index(drop=True, inplace=True)
            
                if (
                    collection_name not in self.db.list_collection_names()
                    and config["save_to_db"] == True
                ):
                    # print(f"Creating Collection {collection_name}")
                    self.db.create_collection(
                        collection_name,
                        timeseries={"timeField": "timestamp", "metaField": "metadata"},
                    )
                    # db[collection_name].create_index(
                    #     [("timestamp", 1), ("metadata.label", -1)], unique=True
                    # )
                    # create index if neccessary
                for r in range(df_change.shape[0]):
                    label_name = df_change.iloc[r, 0]
                    for d in range(1, df_change.shape[1]):
                        if not pd.isna(df_change.iloc[r, d]):
                            change = df_change.iloc[r, d]
                        else:change = 'null'
                        if not pd.isna(df_rchange.iloc[r, d]):
                            change_rate = df_rchange.iloc[r, d]
                        else:change_rate = 'null'
                        date = f"{year} {df.iloc[0,d]} {(df.iloc[1,d])}"
                        timestamp_obj = datetime.strptime(date.strip(),"%Y %B %d")
                        record = {
                            "metadata": {"label": label_name, "str_id": str_id},
                            "timestamp": timestamp_obj,
                            "change": change,
                            "change_rate": change_rate,
                        }
                        # print(record)
                        try:
                            if config["save_to_db"] == True:
                                q = {"timestamp":{"$eq":record['timestamp']},
                                    "metadata.str_id":str_id,
                                    "metadata.label":label_name}
                                match_obj = self.db[collection_name].find_one(q)
                                
                                if match_obj:
                                    self.db[collection_name].delete_one({"_id":match_obj["_id"]}) 
                                    self.db[collection_name].insert_one(record)
                                else:self.db[collection_name].insert_one(record)                           
                        except Exception as e:
                            print(e)
                
    def prepare_toc_sheet(self,dfo):
        # print(dfo)
        global year
        global month
        
        collection_name = "Monthly_str_reports"
        df = dfo["df"]

        df.dropna(axis="columns", how="all", inplace=True)
        df.dropna(axis="rows", how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.columns = range(df.shape[1])
        # df.to_csv("output/toc.csv")
        strinfo = {}
        idc = df.iloc[0, 0].split("/")
        strinfo["str_id"] = idc[0].split("#")[1].strip()
        prop = df.iloc[1, 0].split(":")
        dtr = df.iloc[2, 0].split(":")
        dates = dtr[1].strip()
        drange = datetime.strptime(dates.strip(),"%B %Y")
        strinfo["str_property"] = prop[1].strip()
        strinfo["str_date_range"] = drange
        year = drange.year
        month = dates.split(' ')[0]
        if (
            collection_name not in self.db.list_collection_names()
            and config["save_to_db"] == True
        ):
            # print(f"Creating Collection {collection_name}")
            self.db.create_collection(
                collection_name,
                # timeseries={"timeField": "timestamp", "metaField": "metadata"},
            )
        if config["save_to_db"] == True:
            result = self.db[collection_name].insert_one(strinfo)
            return result.inserted_id
        


    def prepare_all_dfs_monthly(self,sheets,xl):
        dfs = self.prepare_dfs(sheets,xl)
        try:
            for dfo in dfs:
                if dfo["sheet"] == "Table of Contents":
                    str_id = self.prepare_toc_sheet(dfo)
                    break
                
            for index, dfo in enumerate(dfs):
                # print(dfo["sheet"])
                if dfo["sheet"] == "Comp":
                    self.prepare_comp_sheet(dfo, str_id)
                if dfo["sheet"] == "Daily by Month":
                    self.prepare_daily_sheet(dfo, str_id)  
            return {'message':'Extraction Success', 'status':200,"report_id":str_id}
        except Exception as e:
            return {'message':e, 'status':500}




