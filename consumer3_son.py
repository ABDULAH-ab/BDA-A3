from kafka import KafkaConsumer
import json
import itertools
from time import sleep
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

data=[]

for message in consumer:
    price_data = message.value
    price_data2 = price_data['also_buy']
    data.append(tuple(price_data2))  # Convert list to tuple
    count=0
    for item in data:
        for i in item:
            count+=1
    #print(count)
        
    #dividing the threshold
    divided_threshold=8//2
    count1=count//2
    counter=0
    first_chunk=[]
    second_chunk=[]
    third_chunk=[]
    #print(count1)
    #print(count2)
    #print(count3)
    for item in data:
        if(counter<=count1):
            first_chunk.append(item)
        elif(counter>count1 and counter<=count):
            second_chunk.append(i)
        counter+=1
    #print(first_chunk)
    item_counts={}
    item_con={}
    single=[]
    single2=[]
    counter=0
    for item in data:
        for i in item:
            if(counter<=count1):
                item_counts[i] = item_counts.get(i, 0) + 1
            elif(counter>count1 and counter<=count):
                item_con[i] = item_con.get(i, 0) + 1
            counter+=1
    #print (item_counts)
    for item, count in item_counts.items():
        if count >= 2:
            single.append(item)

    for item in data:
        subsets=[]
        for subset in itertools.combinations(item, 0+2):
            subsets.append(subset)
    item_counts2={}
    for subset in subsets:
        item_counts2[subset] = item_counts2.get(subset, 0) + 1
    for item_set, count in item_counts2.items():
        if count > 1:
            print(f"{item_set}\t{count}")
            single.append(item_set)

    #if single:
        #print("Frequent Items:")
        #print(single)

    sleep(5)

    for item, count in item_con.items():
        if count >= 2:
            single2.append(item)
    for item in data:
        subsets=[]
        for subset in itertools.combinations(item, 0+2):
            subsets.append(subset)
    item_counts3={}
    for subset in subsets:
        item_counts3[subset] = item_counts3.get(subset, 0) + 1
    for item_set, count in item_counts3.items():
        if count > 1:
            print(f"{item_set}\t{count}")
            single2.append(item_set)

    #if single2:
        #print("Frequent Items:")
        #print(single2)

    data2=single
    for i in single2:
        data2.append(i)

    main_item_counts={}
    for i in data2:
        main_item_counts[i] = main_item_counts.get(i, 0) + 1

    frequentitems=[]
    for item, count in main_item_counts.items():
        if count >= 2:
            frequentitems.append(item)
    
    if frequentitems:
        print("Frequent Items:")
        print(frequentitems)

    collection.insert_one(main_item_counts)


        

