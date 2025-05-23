from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
load_dotenv()  # Load variables from .env

username = os.getenv("MONGO_USERNAME")
password = os.getenv("MONGO_PASSWORD")

uri = f"mongodb+srv://{username}:{password}@projetbigdata.bg6dron.mongodb.net/?retryWrites=true&w=majority&appName=ProjetBigData"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:

    db = client.ProjetBigDataDB # You can choose any DB name
    collection = db["top_song_2020"]
    count = collection.count_documents({})
    print(f"Successfully accessed collection 'top_song_2020'. Document count: {count}")
except Exception as e:
    print(e)
