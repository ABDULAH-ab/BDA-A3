from kafka import KafkaProducer
import json
from time import sleep

def read_json_file(file_path, producer):
    with open(file_path, 'r') as file:
        for line in file:
            try:
                sleep(4)
                data = json.loads(line.strip())  # Load JSON from each line
                
                # Concatenate the content in "also_buy" and "also_view" lists along with "asin" and "similar"
                concatenated_data = {
                    "also_buy": data["also_buy"]
                    }
                
                
                # Convert concatenated_data to JSON and send it to Kafka
                producer.send('testtopic8', value=json.dumps(concatenated_data).encode('utf-8'))
                
                print('Data sent to Kafka:', concatenated_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line: {e}")

def main():
    bootstrap_servers = 'localhost:9092'  # Update this with your Kafka broker address
    json_file_path = 'pre_processed_data.json'

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: v)

    read_json_file(json_file_path, producer)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()

