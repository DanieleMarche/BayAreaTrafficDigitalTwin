import json
from pathlib import Path

import networkx as nx
import osmnx as ox
import pandas as pd
from confluent_kafka import Consumer, KafkaError, TopicPartition, OFFSET_BEGINNING

# 1. Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
ROOT_DIR = Path(__file__).resolve().parent.parent
SENSORS_SUBSET = ROOT_DIR / "data" / "processing" / "pems-bay-sensors-subset.csv"
# PEMS-BAY sensor coordinates (no header: sensor_id, lat, lon)
SENSOR_LOCATIONS_FILE = ROOT_DIR / "data" / "graph_sensor_locations_bay.csv"
# Local cache of the OSM road network, so it is downloaded only once
ROAD_GRAPH_CACHE = ROOT_DIR / "data" / "processing" / "bay_road_network.graphml"
# Set to True to print the road distance between every pair of sensors (slow)
PRINT_SENSOR_DISTANCES = False

# Same sensor subset replayed by stream/producer.py
SELECTED_SENSORS = pd.read_csv(SENSORS_SUBSET)['sensor_id'].tolist()
# Topics written by sensor_simulator.sensors.TrafficSensor: traffic.<sensor_id>.speed
SENSOR_TOPICS = [f"traffic.{s}.speed" for s in SELECTED_SENSORS]

# Weather topics written by sensor_simulator.sensors.WeatherSensor: weather.<station>.<variable>
# (the producer replaces '_' with '-' in variable names)
WEATHER_STATION = "SJC"
WEATHER_VARIABLES = ["tmpc", "dwpc", "relh", "drct", "sped", "p01m", "vsby", "gust", "peak_wind_gust"]
WEATHER_TOPICS = [f"weather.{WEATHER_STATION}.{v.replace('_', '-')}" for v in WEATHER_VARIABLES]

# 2. Sensor locations
sensor_locations = pd.read_csv(
    SENSOR_LOCATIONS_FILE,
    header=None,
    names=["sensor_id", "lat", "lon"]
)

# Only use the selected sensors
selected_locations = sensor_locations[
    sensor_locations["sensor_id"].isin(SELECTED_SENSORS)
].reset_index(drop=True)

missing = set(SELECTED_SENSORS) - set(selected_locations["sensor_id"])
if missing:
    print(f"Warning: no location found for sensors {sorted(missing)}; their messages will be ignored.")

# Bounding box around the selected sensors only
north = selected_locations['lat'].max()
south = selected_locations['lat'].min()
east = selected_locations['lon'].max()
west = selected_locations['lon'].min()

print("Sensor boundaries:")
print(f"north: {north}, south: {south}, east: {east}, west: {west}")

# Add approximately 1 km of padding around the sensor boundaries
padding = 0.01
bbox = (west - padding, south - padding, east + padding, north + padding)  # (left, bottom, right, top)

# 3. Road network (OpenStreetMap)
if ROAD_GRAPH_CACHE.exists():
    print(f"Loading cached road network from {ROAD_GRAPH_CACHE.name}...")
    road_graph = ox.load_graphml(ROAD_GRAPH_CACHE)
else:
    print("Downloading road network...")
    road_graph = ox.graph_from_bbox(bbox, network_type="drive", retain_all=True)
    ox.save_graphml(road_graph, ROAD_GRAPH_CACHE)
    print("Road network downloaded and cached.")

print(f"Road intersections: {len(road_graph.nodes)}")
print(f"Road segments: {len(road_graph.edges)}")

# 4. Map each sensor to its closest intersection and road segment
lons = selected_locations["lon"].tolist()
lats = selected_locations["lat"].tolist()
nearest_nodes = ox.distance.nearest_nodes(road_graph, X=lons, Y=lats)
nearest_edges = ox.distance.nearest_edges(road_graph, X=lons, Y=lats)

sensor_ids = selected_locations["sensor_id"].tolist()
sensor_to_road_node = dict(zip(sensor_ids, nearest_nodes))
sensor_to_road_edge = {s: tuple(e) for s, e in zip(sensor_ids, nearest_edges)}

# 5. Build sensor connectivity graph
sensor_graph = nx.Graph()
sensor_graph.add_nodes_from(sensor_to_road_edge.keys())

for i, sensor_a in enumerate(sensor_ids):
    u1, v1, key1 = sensor_to_road_edge[sensor_a]

    for sensor_b in sensor_ids[i + 1:]:
        u2, v2, key2 = sensor_to_road_edge[sensor_b]

        # Connect sensors if their road segments share an endpoint
        if {u1, v1} & {u2, v2}:
            sensor_graph.add_edge(sensor_a, sensor_b)

clusters = sorted(nx.connected_components(sensor_graph), key=len, reverse=True)

print("\n--- SENSOR CONNECTIVITY ---")
print(f"Total sensors: {len(sensor_ids)}")
print(f"Connected clusters: {len(clusters)}")

for i, cluster in enumerate(clusters, 1):
    print(f"Cluster {i}: {len(cluster)} sensors")
    print(sorted(cluster))

print("\nSensor to road node mapping:")
for sensor_id, road_node in sensor_to_road_node.items():
    print(f"{sensor_id} -> {road_node}")
print("Unique road nodes:", len(set(sensor_to_road_node.values())))

if PRINT_SENSOR_DISTANCES:
    print("\nDistances between sensor road nodes:")
    for sensor_id, road_node in sensor_to_road_node.items():
        for other_sensor_id, other_road_node in sensor_to_road_node.items():
            if sensor_id < other_sensor_id:
                try:
                    distance = nx.shortest_path_length(
                        road_graph, source=road_node, target=other_road_node, weight="length"
                    )
                    print(f"{sensor_id} -> {other_sensor_id}: {distance:.2f} metres")
                except nx.NetworkXNoPath:
                    print(f"{sensor_id} -> {other_sensor_id}: no road path")

# Example: shortest road path between the first two mapped sensors
if len(sensor_ids) >= 2:
    a, b = sensor_ids[0], sensor_ids[1]
    print(f"\nShortest road path between {a} and {b}:")
    try:
        path = nx.shortest_path(
            road_graph, source=sensor_to_road_node[a], target=sensor_to_road_node[b], weight="length"
        )
        print(f"Road nodes in path: {len(path)}")
        print(f"Path: {path}")
    except nx.NetworkXNoPath:
        print("No road path found.")

print("\nMapped road segment information:")
for sensor_id, (u, v, key) in sensor_to_road_edge.items():
    edge_data = road_graph.edges[u, v, key]
    print(
        f"Sensor {sensor_id} -> edge {(u, v, key)}: "
        f"length={float(edge_data['length']):.2f} metres, "
        f"name={edge_data.get('name', 'Unknown')}, "
        f"highway={edge_data.get('highway', 'Unknown')}"
    )

# 5. Step 3 Representation Model: the OSM road graph, with sensor state on its edges
G = road_graph

for sensor_id, (u, v, key) in sensor_to_road_edge.items():
    G.edges[u, v, key]["sensor_id"] = sensor_id
    G.edges[u, v, key]["current_speed"] = 65.0        # Default baseline
    G.edges[u, v, key]["traffic_state"] = "FREE_FLOW"  # High-level inferred state

print("\nRepresentation Graph Model Initialized.")
print(f"Road intersections: {G.number_of_nodes()}")
print(f"Road segments: {G.number_of_edges()}")
print(f"Road segments with a sensor: {len(set(sensor_to_road_edge.values()))}")

# 6a. Weather station entity (fed by the weather.SJC.* topics, which the producer replays from SJC.csv)
RAIN_TIMEOUT = pd.Timedelta(hours=1)       # p01m is a 1-hour total and is only published when it rains
STALE_AFTER = pd.Timedelta(hours=2)        # weather observations arrive roughly every hour
LOW_VISIBILITY_MILES = 3.0                 # vsby is in miles
PERIODIC_WEATHER = {"tmpc", "dwpc", "relh", "drct", "sped", "vsby"}  # event-based: p01m, gust, peak_wind_gust

weather_station = {
    "station": WEATHER_STATION,
    "values": {},              # variable -> latest value
    "updated": {},             # variable -> timestamp of that value
    "rain": "UNKNOWN",         # DRY / RAIN
    "visibility": "UNKNOWN",   # GOOD / LOW_VISIBILITY
    "last_rain_ts": None,
    "stale": [],               # periodic variables with no recent observation
}

# Relation: every traffic sensor is influenced by the (single) weather station
for s in sensor_graph.nodes:
    sensor_graph.nodes[s]["influenced_by"] = WEATHER_STATION


def update_weather(variable, ts, value):
    """Apply one weather reading to the station. Returns a list of events."""
    weather_station["values"][variable] = value
    weather_station["updated"][variable] = ts
    events = []

    if variable == "p01m":
        # a p01m message only exists when it rains
        weather_station["last_rain_ts"] = ts
        if weather_station["rain"] != "RAIN":
            weather_station["rain"] = "RAIN"
            events.append("RainStarted")

    elif variable == "vsby":
        old = weather_station["visibility"]
        new = "LOW_VISIBILITY" if value < LOW_VISIBILITY_MILES else "GOOD"
        weather_station["visibility"] = new
        if old != new and new == "LOW_VISIBILITY":
            events.append("LowVisibilityStarted")
        elif old == "LOW_VISIBILITY" and new == "GOOD":
            events.append("LowVisibilityEnded")

    return events


def check_weather_timeouts(now, twin_start):
    """Rules that depend on the passing of time, not on a message. Returns a list of events."""
    events = []

    # Rain has no 'stopped' message: it ends when no p01m arrived for RAIN_TIMEOUT
    if weather_station["rain"] == "RAIN" and now - weather_station["last_rain_ts"] > RAIN_TIMEOUT:
        weather_station["rain"] = "DRY"
        events.append("RainEnded")
    # Initial situation: if no rain was seen during the first RAIN_TIMEOUT, it is dry
    elif weather_station["rain"] == "UNKNOWN" and now - twin_start > RAIN_TIMEOUT:
        weather_station["rain"] = "DRY"

    # Periodic variables become stale when not observed for STALE_AFTER
    weather_station["stale"] = [
        v for v in PERIODIC_WEATHER
        if v not in weather_station["updated"] or now - weather_station["updated"][v] > STALE_AFTER
    ]
    return events

# 6. Setup Kafka Consumer
consumer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'traffic-representation-group-v2',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
# Automatic partition assignment wasn't working, so partition 0 of each topic is assigned manually
partitions = [TopicPartition(topic, 0, OFFSET_BEGINNING) for topic in SENSOR_TOPICS + WEATHER_TOPICS]
consumer.assign(partitions)

twin_clock = None   # latest timestamp received (the twin's 'now')
twin_start = None   # first timestamp received

print("\nListening for live streaming traffic updates from Kafka...")

try:
    while True:
        msg = consumer.poll(1.0)  # Wait up to 1 second for a message

        if msg is None:
            print("No message received in the last second...")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Kafka Error: {msg.error()}")
            break

        # Parse the incoming sensor message
        data = json.loads(msg.value().decode('utf-8'))
        #print("GOT", msg.topic(), data.get("ts"))
        ts = pd.Timestamp(data["ts"])

        # Twin clock: always the most recent timestamp seen on any topic
        if twin_start is None:
            twin_start = ts
        if twin_clock is None or ts > twin_clock:
            twin_clock = ts

        # Time-based weather rules run on every message, traffic included
        for event in check_weather_timeouts(twin_clock, twin_start):
            print(f"EVENT [{twin_clock}] {event} at {WEATHER_STATION}")

        if msg.topic().startswith("weather."):
            variable = data["variable"].replace("-", "_")   # back to the SJC.csv column name
            value = float(data["value"])
            for event in update_weather(variable, ts, value):
                print(f"EVENT [{ts}] {event} at {WEATHER_STATION}")
            print(f"[{ts}] Weather {variable} = {value} {data.get('unit') or ''} | "
                f"rain={weather_station['rain']} visibility={weather_station['visibility']}")
            continue   # weather handled; skip the traffic code below
        run_id = data.get("run_id", "unknown")
        # TrafficSensor payload: sensor_id is a string, the reading is in 'value', time in 'ts'
        sensor_id = int(data['sensor_id'])
        timestamp = data['ts']
        speed = data['value']

        if sensor_id not in sensor_to_road_edge:
            continue  # Sensor without a known location

        # 7. Update the Representation Model State
        # Derive higher-level context from raw numbers as required by Step 3
        derived_state = "CONGESTED" if speed < 45.0 else "FREE_FLOW"

        u, v, key = sensor_to_road_edge[sensor_id]
        G.edges[u, v, key]['current_speed'] = speed
        G.edges[u, v, key]['traffic_state'] = derived_state

        # 8. Print Verification Log
        #print(f"[Run: {run_id[:8]}][{timestamp}] Topic: {msg.topic()} -> "
        #   f"Sensor {sensor_id} (edge {u}->{v}) updated: {speed} MPH | State: {derived_state}")

except KeyboardInterrupt:
    print("\nStopping Consumer...")
finally:
    consumer.close()
