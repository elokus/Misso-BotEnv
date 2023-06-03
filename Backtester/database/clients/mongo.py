from Backtester.config import CONFIGS
import pymongo

def get_client():
    connection_string = f"mongodb+srv://elMongo-admin:{CONFIGS['db']['password']}@awssharedcluster.0ekoew2.mongodb.net/?retryWrites=true&w=majority"
    client = pymongo.MongoClient(connection_string)
    return client

def get_database(db_name: str):
    return get_client()[db_name]


