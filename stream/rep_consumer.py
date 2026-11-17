import json
from pathlib import Path
import networkx as nx
import pandas as pd
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
# we need to install openmaps later osmnx
# 1. Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
ROOT_DIR = Path(__file__).resolve().parent.parent
SENSORS_SUBSET = ROOT_DIR / "data" / "processing" / "pems-bay-sensors-subset.csv"
# Same sensor subset replayed by stream/producer.py
SELECTED_SENSORS = pd.read_csv(SENSORS_SUBSET)['sensor_id'].tolist()
# Topics written by sensor_simulator.sensors.TrafficSensor: traffic.<sensor_id>.speed
SENSOR_TOPICS = [f"traffic.{s}.speed" for s in SELECTED_SENSORS]

# 2. Step 3 Representation Model: Initialize the Spatial Graph Topology
G = nx.DiGraph()

# One node per sensor; coordinates (lat, lon) are filled in later by the OpenMaps layer
# We also attach an initial real-time state dictionary to each node
for sensor_id in SELECTED_SENSORS:
    lat, lon = None, None
    G.add_node(sensor_id, 
               lat=lat, 
               lon=lon, 
               current_speed=65.0,        # Default baseline
               traffic_state="FREE_FLOW"  # High-level inferred state
    )

# Establish directed edges (representing the flow of traffic from one node to the next)
# For now, we assume a sequential linear chain layout: 400001 -> 400017 -> 400030 ...
for i in range(len(SELECTED_SENSORS) - 1):
    G.add_edge(SELECTED_SENSORS[i], SELECTED_SENSORS[i+1])

print("Representation Graph Model Initialized.")
print(f"Nodes (Sensors): {list(G.nodes)}")
print(f"Edges (Road Connections): {list(G.edges)}")

# 3. Setup Kafka Consumer
consumer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'traffic-representation-group-v2',
    'auto.offset.reset': 'earliest' # 
}
consumer = Consumer(consumer_config)
# wasnt working with automatic partions so did it manually, i.e assigned the first partition

partitions = [
    TopicPartition(topic, 0, OFFSET_BEGINNING)
    for topic in SENSOR_TOPICS
]

consumer.assign(partitions)

print("\nListening for live streaming traffic updates from Kafka...")

try:
    while True:
        msg = consumer.poll(1.0) # Wait up to 1 second for a message
        
        if msg is None:
            print("No message received in the last second...")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Kafka Error: {msg.error()}")
                break
                
        # Parse the incoming sensor message
        data = json.loads(msg.value().decode('utf-8'))
        run_id = data.get("run_id", "unknown")
        # TrafficSensor payload: sensor_id is a string, the reading is in 'value', time in 'ts'
        sensor_id = int(data['sensor_id'])
        timestamp = data['ts']
        speed = data['value']
        
        # 4. Update the Representation Model State
        # Derive higher-level context from raw numbers as required by Step 3
        derived_state = "CONGESTED" if speed < 45.0 else "FREE_FLOW"
        
        # Inject the new metrics directly into the active graph architecture
        G.nodes[sensor_id]['current_speed'] = speed
        G.nodes[sensor_id]['traffic_state'] = derived_state
        
        # 5. Print Verification Log
        print(f"[Run: {run_id[:8]}][{timestamp}] Topic: {msg.topic()} -> Sensor {sensor_id} updated: {speed} MPH | State: {derived_state}")




except KeyboardInterrupt:
    print("\nStopping Consumer...")
finally:
    consumer.close()
