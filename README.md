# BDA-A3


Kafka to MongoDB Integration Using Apriori , PCY & SON Algorithm

This repository contains source code for a project that utilizes Apache Kafka for data ingestion and MongoDB for storing results processed by the Apriori algorithm. The project serves as an exemplary implementation for real-time data processing and frequent itemset mining in a distributed environment.

### Dependencies
- Python 3.x (install from [official site](https://www.python.org/downloads/))
- Kafka (setup instructions available on [Apache Kafka](https://kafka.apache.org/documentation/))
- MongoDB (installation guide at [MongoDB](https://www.mongodb.com/try/download/community))

---

### Introduction
The project is designed to demonstrate a seamless flow of data from Kafka to MongoDB through the implementation of the Apriori algorithm to mine frequent itemsets. It highlights the capability of handling large-scale data in real-time and efficiently storing the computed results for further analysis.

---

### Files & DB
- `pre_processor.py`: Preprocesses the initial data.
- `Producer.py`: Ingests data into Kafka topics from a JSON formatted file.
- `consumer1_Apriori.py`: Processes the streamed data to compute frequent itemsets using the sliding apriori algorithm
- `consumer2_PCY.py`: Processes the streamed data to compute frequent itemsets using the sliding pcy algorithm
- `consumer3_son.py`: Processes the streamed data to compute frequent itemsets.
- *MongoDB*: Stores the results of the data processing, allowing for efficient retrieval and storage of large volumes of itemset data.

---

### Usage

- `producer.py`: Reads data from dataset.json and sends each entry to the Kafka topic.

- `consumer1_Apriori.py`: Consumes messages from Kafka, applies the Apriori algorithm to find frequent itemsets, and stores the sets, set number and the frequency count as results in MongoDB.
- `consumer2_PCY.py`: Consumes messages from Kafka, applies the Sliding PCY algorithm to find frequent itemsets, and stores the results in MongoDB.
- `consumer3_son.py`: Consumes messages from Kafka, applies SON algorithm to find frequent itemsets, and stores the results in MongoDB.
- `bash.sh`: A simple start up file automatically starts the kafka servers and runs producer and consumers.

---

### Execution Instructions (Manually)

1. *Start the Kafka Producer*:  
   Run python producer.py to begin data ingestion into Kafka.
2. *Execute the Kafka Consumer*:  
   Run python consumer1.py to start the consumer process that applies the Apriori algorithm and writes the output to MongoDB.
   Run python consumer2.py to start the consumer process that applies the pcy algorithm and writes the output to MongoDB.
   Run python consumer3.py to start the consumer process that applies the custom algorithm and writes the output to MongoDB.
3. *Activate MongoDB & Open Mongosh*:  
   Activate Mongo by typing sudo systemctl start mongod terminal
   Run Mongosh Terminal by typing mongosh in the terminal
   Run use mydatabase to select the database 
4. *View the Database for the PCY consumer*:  
   Run db.frequent_itemsets.deleteMany({}) to delete the previous content for the PCY consumer database
   Run db.frequent_itemsets.find().pretty() to view the database for the PCY consumer
5. *View the Database for the Apriori consumer*:  
   Run db.apriori.deleteMany({}) to delete the previous content for the Apriori consumer database
   Run db.apriori.find().pretty() to view the database for the Apriori consumer
6. *View the Database for the custom consumer*:  
   Run db.custom.deleteMany({}) to delete the previous content for the custom consumer database
   Run db.custom.find().pretty() to view the database for the custom consumer


### Contributors
- *Abdullah* || i221879
- *Saim Nadeem* || i221884
- *Shazer Nadeem* || i222043
