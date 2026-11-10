import json
import networkx as nx
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
# we need to install openmaps later osmnx
# 1. Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
SELECTED_SENSORS = [400001, 400017, 400030, 400040, 400045]
SENSOR_TOPICS = [f"sensor-{s}" for s in SELECTED_SENSORS]

# 2. Step 3 Representation Model: Initialize the Spatial Graph Topology
G = nx.DiGraph()

# Add our 5 nodes with placeholder coordinates (lat, lon) for the OpenMaps layer later
# We also attach an initial real-time state dictionary to each node
placeholder_coords = {
    400001: (37.7749, -122.4194),  # Placeholder San Francisco coords
    400017: (37.7750, -122.4180),
    400030: (37.7760, -122.4170),
    400040: (37.7770, -122.4160),
    400045: (37.7780, -122.4150),
}

for sensor_id in SELECTED_SENSORS:
    lat, lon = placeholder_coords[sensor_id]
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
        sensor_id = data['sensor_id']
        timestamp = data['timestamp']
        speed = data['speed']
        
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
