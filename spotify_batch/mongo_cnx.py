from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

load_dotenv()


def connect_to_mongo():
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    connection_string = f"mongodb+srv://{username}:{password}@projetbigdata.bg6dron.mongodb.net/?retryWrites=true&w=majority&appName=ProjetBigData"
    return MongoClient(connection_string, server_api=ServerApi("1"))

def save_to_mongo(records,collection_name):
    client = connect_to_mongo()
    db = client.ProjetBigDataDB
    collection = db[collection_name]  
    collection.drop()
    collection.insert_many(records)
    print("✅ Top streamed songs saved to MongoDB.")

if __name__ == "__main__":
    try:
        client= connect_to_mongo()
        db = client.ProjetBigDataDB # You can choose any DB name
        collection = db["top_song_2020"]
        count = collection.count_documents({})
        print(f"Successfully accessed collection 'top_song_2020'. Document count: {count}")
    except Exception as e:
        print(e)