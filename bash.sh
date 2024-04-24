#!/bin/bash

# Start Zookeeper in a new terminal
gnome-terminal -- bash -c "bin/zookeeper-server-start.sh config/zookeeper.properties" &

# Wait for 2 seconds before opening the next terminal window
sleep 2

# Start Kafka Server in a new terminal
gnome-terminal -- bash -c "bin/kafka-server-start.sh config/server.properties" &

# Wait for 2 seconds before opening the next terminal window
sleep 5

# Start Kafka Producer in a new terminal
gnome-terminal -- python3 -i -c 'import producer' &

# Wait for 2 seconds before opening the next terminal window
sleep 5

# Start Kafka Consumer in a new terminal
gnome-terminal -- python3 consumer1.py &

sleep 5

# Start Kafka Consumer in a new terminal
gnome-terminal -- python3 consumer2.py &

sleep 5

# Start Kafka Consumer in a new terminal
gnome-terminal -- python3 consumer3.py &

sleep 5

# Keep the script running until the user decides to exit
while true; do
    sleep 10
#done
