import pandas as pd
import sys
import numpy as np
from datetime import datetime
import json
import os
import pdb
from mongo_service import connect_to_MongoDb


class str_report_type(connect_to_MongoDb):

    def __init__(self):
        super().__init__()
        

    def prepare_toc_sheet(self,df):
        df.dropna(axis="columns", how="all", inplace=True)
        df.dropna(axis="rows", how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.columns = range(df.shape[1])
        # df.to_csv("output/toc.csv")
        strinfo = {}
        idc = df.iloc[0, 0].split("/")
        strinfo["str_id"] = idc[0].split("#")[1].strip()
        prop = df.iloc[1, 0].split(":")
        strinfo["str_type"] = prop[0].strip()
        strinfo["str_property"] = prop[1].strip()
        return strinfo

    def check_str_report_type(self,sheets,xl):
        sheet = sheets[0]
        df = xl.parse(sheet, header=None)
        
        try:
            str_info = self.prepare_toc_sheet(df)
            print(str_info)
            return {'response':str_info, 'status':200}
        except Exception as e:
            return {'message':e, 'status':500}


