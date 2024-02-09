from flask import Blueprint, jsonify, request
import pandas as pd
from pandas import json_normalize
import xml.etree.ElementTree as ET
from yaml import safe_load
from app.utils import check_devices
from database.mongo_conection import get_database
from app.models import Analysis, People, Transfer, Promotion, Transaction
from datetime import datetime
from flask_cors import cross_origin
from app.utils import serialize_mongo_document
from bson.objectid import ObjectId


bp = Blueprint('api', __name__, url_prefix='/api')

@bp.route('/form/create-new-analysis', methods=['POST'])
@cross_origin()
def create_new_analysis():
    
    peopleYAML, peopleJson, transfers, promotions, transactions = None, None, None, None, None

    for key, file in request.files.items():
        
        content_type = file.content_type
        filename = file.filename

        if content_type == 'application/x-yaml' or filename.endswith('.yml') or filename.endswith('.yaml'):
            yamlFile = safe_load(file.read())
            if 'people' in yamlFile:
                peopleYAML = pd.json_normalize(yamlFile['people'])
                if 'city' in peopleYAML.columns:
                    peopleYAML[['city', 'country']] = peopleYAML['city'].str.split(', ', expand=True)
        elif content_type == 'text/csv' or filename.endswith('.csv'):
            if key == 'promotion':
                promotions = pd.read_csv(file)
                promotions.rename(columns={'client_email': 'email', 'telephone': 'phone'}, inplace=True)
            elif key == 'transfer':
                transfers = pd.read_csv(file)
                transfers['date'] = pd.to_datetime(transfers['date'])
        elif content_type == 'application/json' or filename.endswith('.json'):
            peopleJson = pd.read_json(file)
            peopleJson = json_normalize(peopleJson['people'])

            peopleJson['iPhone'] = peopleJson['devices'].apply(check_devices, device_name='iPhone')
            peopleJson['Android'] = peopleJson['devices'].apply(check_devices, device_name='Android')
            peopleJson['Desktop'] = peopleJson['devices'].apply(check_devices, device_name='Desktop')

            peopleJson = peopleJson.drop(['devices'], axis=1)

            peopleJson['id'] = peopleJson['id'].str.lstrip('0')

            peopleJson['name'] = peopleJson['firstName'] + ' ' + peopleJson['surname']
            peopleJson = peopleJson.drop(['firstName', 'surname'], axis=1)

            cols = list(peopleJson.columns)
            name_index = cols.index('name')
            cols.insert(1, cols.pop(name_index))
            peopleJson = peopleJson[cols]

            peopleJson.rename(columns={'telephone': 'phone', 'location.city': 'city', 'location.country': 'country'}, inplace=True)

            peopleJson['id'] = peopleJson['id'].astype(peopleYAML['id'].dtype)
        elif content_type == 'text/xml' or filename.endswith('.xml'):
            tree = ET.parse(file)
            root = tree.getroot()

            ids = []
            buyer_names = []
            items = []
            prices = []
            stores = []
            transaction_dates = []

            for transaction in root.findall('transaction'):
                id = transaction.attrib['id']
                buyer_name = transaction.find('buyer_name').text
                item = transaction.find('item').text
                price = float(transaction.find('price').text)
                store = transaction.find('store').text
                transaction_date = transaction.find('transactionDate').text
    

                ids.append(id)
                buyer_names.append(buyer_name)
                items.append(item)
                prices.append(price)
                stores.append(store)
                transaction_dates.append(transaction_date)

            transactions = pd.DataFrame({
                'id': ids,
                'buyer_name': buyer_names,
                'item': items,
                'price': prices,
                'store': stores,
                'transaction_date': transaction_dates
            })

            transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'])
          
    
    peopleMerged = pd.concat([peopleJson, peopleYAML], ignore_index=True)
    peopleMerged.drop_duplicates(keep="first", inplace=True)
    peopleMerged = peopleMerged.sort_values(by='id')
    peopleMerged[['first_name', 'surname']] = peopleMerged['name'].str.split(' ', n=1, expand=True)
    peopleMerged['buyer_name'] = peopleMerged['first_name'].str[0] + '. ' + peopleMerged['surname']
    peopleMerged.reset_index(drop=True, inplace=True)
  
    
    db = get_database()
   
    # create an analysis record
    analysis = Analysis(db)
    analysisCreationResult = analysis.insert_one({'date': datetime.now()})
    print(analysisCreationResult.inserted_id)

    # create the peoples records
    peopleMerged['analysis_id'] = analysisCreationResult.inserted_id
    peopleDict = peopleMerged.to_dict(orient="records")
    people = People(db)
    peopleCreationResult = people.insert_many(peopleDict)
    print(peopleCreationResult)

    # create the transfers
    transfers['analysis_id'] = analysisCreationResult.inserted_id
    transferDict = transfers.to_dict(orient="records")
    transfer = Transfer(db)
    transferCreationResult = transfer.insert_many(transferDict)
    print(transferCreationResult)

    # create the promotions
    promotions['analysis_id'] = analysisCreationResult.inserted_id
    promotionsDict = promotions.to_dict(orient="records")
    promotion = Promotion(db)
    promotionCreationResult = promotion.insert_many(promotionsDict)
    print(promotionCreationResult)

    # create the transactions
    transactions['analysis_id'] = analysisCreationResult.inserted_id
    transactionsDict = transactions.to_dict(orient="records")
    transaction = Transaction(db)
    transactionsCreationResult = transaction.insert_many(transactionsDict)
    print(transactionsCreationResult)

    return jsonify('Success'), 200

@bp.route('/analysis', methods=['GET'])
@cross_origin()
def get_all_analysis():
    db = get_database()
    analysis = Analysis(db)

    analysisResponse = analysis.get_all()

    return jsonify(analysisResponse), 200

@bp.route('/analysis/promotions/<analysis_id>', methods=['GET'])
@cross_origin()
def get_promotions_analysis(analysis_id):
    db = get_database()

    try:
        analysis_oid = ObjectId(analysis_id)
    except:
        return jsonify({"error": "Invalid analysis_id format."}), 400
    

    # Aggregation pipeline
    pipeline = [
        {
            "$match": {"analysis_id": analysis_oid}
        },
        {
            "$lookup": {
                "from": "promotions",
                "let": {"personEmail": "$email", "personPhone": "$phone"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$email", "$$personEmail"]},
                                    {"$eq": ["$phone", "$$personPhone"]}
                                ]
                            }
                        }
                    }
                ],
                "as": "matchedPromotions"
            }
        },
        {"$unwind": "$matchedPromotions"},
        {
            "$group": {
                "_id": "$id",
                "name": {"$first": "$name"},
                "email": {"$first": "$email"},
                "phone": {"$first": "$phone"},
                "city": {"$first": "$city"},
                "country": {"$first": "$country"},
                "iPhone": {"$first": "$iPhone"},
                "Android": {"$first": "$Android"},
                "Desktop": {"$first": "$Desktop"},
                "first_name": {"$first": "$first_name"},
                "surname": {"$first": "$surname"},
                "buyer_name": {"$first": "$buyer_name"},
                "analysis_id": {"$first": "$analysis_id"},
                "promotions": {"$addToSet": {"name":"$matchedPromotions.promotion", "responded": "$matchedPromotions.responded"}},
               
            }
        },
        {
            "$project": {
                "_id": 1,
                "name": 1,
                "email": 1,
                "analysis_id": 1,
                "promotions": 1
            }
        }
        
    ]

    result_cursor = db.people.aggregate(pipeline)
    results = [serialize_mongo_document(doc) for doc in result_cursor]
   
    return jsonify(results), 200

@bp.route('/analysis/stores/<analysis_id>', methods=['GET'])
@cross_origin()
def get_stores_analysis(analysis_id):
    db = get_database()

    try:
        analysis_oid = ObjectId(analysis_id)
    except:
        return jsonify({"error": "Invalid analysis_id format."}), 400

    transactions_products_sales_cursor = db.transactions.aggregate([
        {
            "$match": {"analysis_id": analysis_oid}
        },
        {
            "$group": {
                "_id": "$item",
                "count": {
                    "$sum": 1
                },
                "totalPrice": {"$sum": "$price"}
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ])

    transactions_stores_sales_cursor = db.transactions.aggregate(
        [
        {
            "$match": {"analysis_id": analysis_oid}
        },
        {
            "$group": {
                "_id": "$store",
                "transactionCount": {"$sum": 1},
                "totalPrice": {"$sum": "$price"}
            }
        },
        {
            "$sort": {
                "totalPrice": -1
            }
        }
    ]
    )

    transactions_with_people_cursor = db.transactions.aggregate([
        {
            "$match": {"analysis_id": analysis_oid}
        },
        {
        "$lookup": {
            "from": "people",
            "let": {
                "transaction_buyer_name": "$buyer_name",
                "transaction_analysis_id": "$analysis_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$buyer_name", "$$transaction_buyer_name"]},
                                {"$eq": ["$analysis_id", "$$transaction_analysis_id"]}
                            ]
                        }
                    }
                }
            ],
            "as": "person_details"
            }
        },
        {
            "$match": {
                "person_details": {"$ne": []}
            }
        },
        {
            "$unwind": "$person_details"
        },
        {
            "$project": {
    
                "transaction_id": "$id",
                "buyer_name": 1,
                "item": 1,
                "price": 1,
                "store": 1,
                "transaction_date": 1,
                "analysis_id": 1,
                "customer_id": "$person_details.id",
                "name": "$person_details.name",
                "email": "$person_details.email",
                "city": "$person_details.city",
                "country": "$person_details.country",
                "iPhone": "$person_details.iPhone",
                "Android": "$person_details.Android",
                "Desktop": "$person_details.Desktop",
                "first_name": "$person_details.first_name",
                "surname": "$person_details.surname",
                
            }
        }
    ])

   
 
    
    return jsonify({
        "itemSales": [doc for doc in transactions_products_sales_cursor],
        "storeSales": [doc for doc in transactions_stores_sales_cursor],
        "peopleTransactions": [serialize_mongo_document(doc) for doc in transactions_with_people_cursor]

        }), 200