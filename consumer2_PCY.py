from collections import defaultdict, Counter
import json
from kafka import KafkaConsumer
import pymongo

# MongoDB connection details
mongo_host = 'localhost'
mongo_port = 27017
mongo_db = 'your_database'
mongo_collection = 'your_collection'

bootstrap_servers = ['localhost:9092']
topic = 'testtopic8'
client = pymongo.MongoClient(mongo_host, mongo_port)
db = client[mongo_db]
collection = db[mongo_collection]

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def pcy(Data, hash_bucket_size, T,price_data):
    item_counts = defaultdict(int)
    hash_bucket_counts = defaultdict(int)
    pair_counts = Counter()

    for data in Data:
        for i in data:
            item_counts[i] += 1

    frequent_items = {item for item, count in item_counts.items() if count >= T}

    for data in Data:
        items_in_basket = set(data)
        for i, item1 in enumerate(items_in_basket):
            if item1 not in frequent_items:
                continue
            for item2 in list(items_in_basket)[i+1:]:
                if item2 not in frequent_items:
                    continue
                pair_counts[(item1, item2)] += 1

    for pair, count in pair_counts.items():
        hash_bucket = hash(pair) % hash_bucket_size
        hash_bucket_counts[hash_bucket] += 1

    with open('pcy.txt', 'a') as f:
        for pair, count in pair_counts.items():
            hash_bucket = hash(pair) % hash_bucket_size
            if hash_bucket_counts[hash_bucket] >= T:
                f.write(f"{pair}\n")
                print(f"{pair}")
    collection.insert_one(price_data)


f_data = []
i = 0
for message in consumer:
    price_data = message.value
    f_data.append(message.value.get('also_buy', []))
    if i == 100:
        pcy(f_data, 10, 30,price_data)
        f_data.clear()
        i = 0
    i += 1
