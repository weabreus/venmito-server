from pymongo import MongoClient
import certifi
import os

ca = certifi.where()

def get_database():
    # Replace these with your server's details
    CONNECTION_STRING = os.getenv("MONGO_CONNECTION_URI")
    
    # Create a connection using MongoClient
    client = MongoClient(CONNECTION_STRING, tlsCAFile=ca)

    # Create and return the database instance
    return client['venmito']
