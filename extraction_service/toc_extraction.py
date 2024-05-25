import pandas as pd
import sys
import numpy as np
from datetime import datetime
import json
import os
import pdb
from mongo_service import connect_to_MongoDb

import calendar


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
        strinfo["corporation"] = prop[1].strip()
        
        date= df.iloc[2, 0].split(":")[1].strip()

        if strinfo["str_type"] == 'Weekly STAR Report':
            dateRange = date.split("-")
            drange = [datetime.strptime(d.strip(), "%B %d, %Y") for d in dateRange]
            strinfo["date"] = drange
        else:
            drange = datetime.strptime(date.strip(),"%B %Y")

            start_date = drange.replace(day=1)
            last_day = calendar.monthrange(drange.year, drange.month)[1]
            end_date = drange.replace(day=last_day)
            datemrange = [start_date,end_date]

            strinfo["date"] = datemrange

        return strinfo

    def check_str_report_type(self,sheets,xl):
        sheet = sheets[0]
        df = xl.parse(sheet, header=None)

        try:
            str_info = self.prepare_toc_sheet(df)
            return {'response':str_info, 'status':200}
        except Exception as e:
            return {'message':e, 'status':500}


