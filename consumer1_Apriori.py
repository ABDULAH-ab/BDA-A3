from kafka import KafkaConsumer
import json
import itertools
import pymongo

# MongoDB connection details
mongo_host = 'localhost'
mongo_port = 27017
mongo_db = 'your_database'
mongo_collection = 'your_collection'

bootstrap_servers = ['localhost:9092']
topic = 'testtopic8'
local_path = '/home/hoop/kafka/apriori.txt'

# Connect to MongoDB
client = pymongo.MongoClient(mongo_host, mongo_port)
db = client[mongo_db]
collection = db[mongo_collection]

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
data = []
item_counts = {}
single = []


for message in consumer:
    price_data = message.value
    price_data2 = price_data['also_buy']
    data.append(tuple(price_data2))  # Convert list to tuple
    for item in data:
        for i in item:
            item_counts[i] = item_counts.get(i, 0) + 1
        
    for item, count in item_counts.items():

        if count > 10:
            print(f"{item}: {count}")
            single.append(item)
    if(single):
        subsets=[]
        for subset in itertools.combinations(single, 0+2):
            subsets.append(subset)
        item_counts2={}
        for subset in subsets:
            item_counts2[subset] = item_counts2.get(subset, 0) + 1
        single=[]
        for item_set, count in item_counts2.items():
            if count >= 10:
                print(f"{item_set}\t{count}")
                single.append(item_set)
        
    if(single):
        subsets=[]
        for subset in itertools.combinations(single, 0+3):
            subsets.append(subset)
        item_counts2={}
        for subset in subsets:
            item_counts2[subset] = item_counts2.get(subset, 0) + 1
        shingle=[]
        for item_set, count in item_counts2.items():
            if count >= 10:
                print(f"{item_set}\t{count}")
    collection.insert_one(price_data)


        
            

