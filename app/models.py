from app.utils import serialize_mongo_document

class Analysis:
    collection_name = 'analysis'

    def __init__(self, db):
        self.db = db
    
    def get_all(self):
        collection = self.db.get_collection(self.collection_name)
        documents_cursor = collection.find()

        return [serialize_mongo_document(doc) for doc in documents_cursor]

    def insert_one(self, document):
        collection = self.db.get_collection(self.collection_name)
        return collection.insert_one(document)

class People:
    collection_name = 'people'

    def __init__(self, db):
        self.db = db
    
    def insert_many(self, documents):
        collection = self.db.get_collection(self.collection_name)
        return collection.insert_many(documents)

class Transfer:
    collection_name = 'transfers'

    def __init__(self, db):
        self.db = db

    def get_all(self):
        collection = self.db.get_collection(self.collection_name)
        documents_cursor = collection.find()

        return [serialize_mongo_document(doc) for doc in documents_cursor]
    
    def insert_many(self, documents):
        collection = self.db.get_collection(self.collection_name)
        return collection.insert_many(documents)

class Promotion:
    collection_name = 'promotions'

    def __init__(self, db):
        self.db = db
    
    def insert_many(self, documents):
        collection = self.db.get_collection(self.collection_name)
        return collection.insert_many(documents)
    
class Transaction:
    collection_name = 'transactions'

    def __init__(self, db):
        self.db = db
    
    def insert_many(self, documents):
        collection = self.db.get_collection(self.collection_name)
        return collection.insert_many(documents)