import json
import os
from pymongo import MongoClient

# MongoDB configuration
# MONGO_HOST = 'localhost'
# MONGO_PORT = 27017
# DATABASE_NAME = 'amazon_data'
# COLLECTION_NAME = 'filtered_data'

def batch_process_file(input_path, output_path, batch_size=10000):
    with open(input_path, 'r') as input_file:
        batch = []
        i=0
        for line in input_file:
            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                filtered_data = filter_data(batch)
                # output(output_path, filtered_data)
                batch.clear() 
        if batch:
            filtered_data = filter_data(batch)
            # output(output_path, filtered_data)
            batch.clear()
            
    # MongoDB connection
    # client = MongoClient(MONGO_HOST, MONGO_PORT)
    # db = client[DATABASE_NAME]
    # collection = db[COLLECTION_NAME]
    # for data_item in filtered_data:
    #     collection.insert_one(data_item)
    # client.close()


def filter_data(data):
    filtered_data = []
    for item in data:
        filtered_item = {
            "asin": item.get("asin", None),
            "also_buy": item.get("also_buy", None),
            "also_view": item.get("also_view", None)
        }
        filtered_data.append(filtered_item)
    return filtered_data
    

def output(output_path, filtered_data):
    mode = 'w' if not os.path.exists(output_path) else 'a'
    with open(output_path, mode) as output_file:
        for item in filtered_data:
            json.dump(item, output_file)
            output_file.write('\n')


batch_process_file('Sampled_Amazon_Meta.json', 'pre_processed_data.json')
