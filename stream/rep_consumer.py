import json
import networkx as nx
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING
import pandas as pd
from pathlib import Path
import osmnx as ox

ROOT_DIR = Path(__file__).resolve().parent.parent
SENSOR_LOCATIONS_FILE = ROOT_DIR / "data" / "graph_sensor_locations_bay.csv"

# we need to install openmaps later osmnx
# 1. Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
SELECTED_SENSORS = [400001, 400017, 400030, 400040, 400045]
SENSOR_TOPICS = [f"sensor-{s}" for s in SELECTED_SENSORS]

sensor_locations = pd.read_csv(
    SENSOR_LOCATIONS_FILE,
    header=None,
    names=["sensor_id", "lat", "lon"]
)

north = sensor_locations['lat'].max()
south = sensor_locations['lat'].min()
east = sensor_locations['lon'].max()
west = sensor_locations['lon'].min()

print ("sensor boundries:")
print(f"north: {north}, south: {south}, east: {east}, west: {west}")

# Add approximately 1 km of padding around the sensor boundaries
padding = 0.01

bbox = (
    west - padding,
    south - padding,
    east + padding,
    north + padding
)

print("Downloading road network...")

road_graph = ox.graph_from_bbox(
    bbox,
    network_type="drive",
    retain_all=True
)

print("Road network downloaded.")
print(f"Road intersections: {len(road_graph.nodes)}")
print(f"Road segments: {len(road_graph.edges)}")


# combine the individual road networks   

print("\nroad network summary:")
print(f"road intersections: {len(road_graph.nodes)}")
print(f"road segments: {len(road_graph.edges)}")

# Only use the five selected sensors
selected_locations = sensor_locations[
    sensor_locations["sensor_id"].isin(SELECTED_SENSORS)
]

#finds the closest intersection
sensor_to_road_node = {}

for row in selected_locations.itertuples(index=False):
    nearest_node = ox.distance.nearest_nodes(
        road_graph,
        X=row.lon,
        Y=row.lat
    )

    sensor_to_road_node[row.sensor_id] = nearest_node

print("\nSensor to road node mapping:")
for sensor_id, road_node in sensor_to_road_node.items():
    print(f"{sensor_id} -> {road_node}")

print("\nUnique road nodes:", len(set(sensor_to_road_node.values())))

print("\nDistances between sensor road nodes:")

for sensor_id, road_node in sensor_to_road_node.items():
    for other_sensor_id, other_road_node in sensor_to_road_node.items():
        if sensor_id < other_sensor_id:
            distance = nx.shortest_path_length(
                road_graph,
                source=road_node,
                target=other_road_node,
                weight="length"
            )

            print(f"{sensor_id} -> {other_sensor_id}: {distance:.2f} metres")

print("\nShortest road path between 400001 and 400045:")

path = nx.shortest_path(
    road_graph,
    source=sensor_to_road_node[400001],
    target=sensor_to_road_node[400045],
    weight="length"
)

print(f"Road nodes in path: {len(path)}")
print(f"Path: {path}")

#finds the closest road segment.
sensor_to_road_edge = {}

for row in selected_locations.itertuples(index=False):
    nearest_edge = ox.distance.nearest_edges(
        road_graph,
        X=row.lon,
        Y=row.lat
    )

    sensor_to_road_edge[row.sensor_id] = nearest_edge

print("\nSensor to road edge mapping:")

for sensor_id, road_edge in sensor_to_road_edge.items():
    print(f"{sensor_id} -> {road_edge}")

print("\nMapped road segment information:")

for sensor_id, (u, v, key) in sensor_to_road_edge.items():
    edge_data = road_graph.edges[u, v, key]

    print(
        f"Sensor {sensor_id}: "
        f"Road length={edge_data['length']:.2f} metres, "
        f"Road name={edge_data.get('name', 'Unknown')}, "
        f"Highway type={edge_data.get('highway', 'Unknown')}"
    )

# Attach traffic sensor information to the corresponding road edges
for sensor_id, (u, v, key) in sensor_to_road_edge.items():

    road_graph.edges[u, v, key]["sensor_id"] = sensor_id
    road_graph.edges[u, v, key]["current_speed"] = 65.0
    road_graph.edges[u, v, key]["traffic_state"] = "FREE_FLOW"

print("\nTraffic sensors attached to road network:")

for sensor_id, (u, v, key) in sensor_to_road_edge.items():
    edge_data = road_graph.edges[u, v, key]

    print(
        f"Sensor {sensor_id}: "
        f"Speed={edge_data['current_speed']} MPH, "
        f"State={edge_data['traffic_state']}"
    )
# 2. Step 3 Representation Model: Initialize the Spatial Graph Topology
# The OpenStreetMap road graph is our representation graph
G = road_graph

print("Representation Graph Model Initialized.")
print(f"Road intersections: {G.number_of_nodes()}")
print(f"Road segments: {G.number_of_edges()}")

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

        u, v, key = sensor_to_road_edge[sensor_id]
        # Update the traffic state of that road segment
        G.edges[u, v, key]['current_speed'] = speed
        G.edges[u, v, key]['traffic_state'] = derived_state
        
        # 5. Print Verification Log
        print(f"[Run: {run_id[:8]}][{timestamp}] Topic: {msg.topic()} -> Sensor {sensor_id} updated: {speed} MPH | State: {derived_state}")




except KeyboardInterrupt:
    print("\nStopping Consumer...")
finally:
    consumer.close()


