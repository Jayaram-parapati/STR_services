from pymongo import MongoClient
from dotenv import load_dotenv
import os



class connect_to_MongoDb:
    def __init__(self):
        load_dotenv()
        db_url = os.getenv("db_url")
        db_name = os.getenv("db_name")
        mongo_cluster = MongoClient(db_url)
        self.db = mongo_cluster[db_name]
        print(f"you are now connected to ---------> {self.db.name} database ")
    
       