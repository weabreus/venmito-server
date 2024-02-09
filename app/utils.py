from bson.objectid import ObjectId

def check_devices(devices_list, device_name):
    return device_name in devices_list

def serialize_mongo_document(document):
    serialized_doc = {}
    
    for key, value in document.items():
        if isinstance(value, ObjectId):
            
            serialized_doc[key] = str(value)
        else:
            serialized_doc[key] = value
    return serialized_doc

