import time
import json
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import uuid
from datetime import datetime, timezone

# === CONFIGURATION ===
BOOTSTRAP_SERVERS = 'localhost:9092'
REPLAY_SPEED = 60.0  # 60x speed: 5 minutes of data replayed in 5 seconds
RUN_ID = str(uuid.uuid4())

print(f"Starting replay run: {RUN_ID}")
# Step 1 Compliance: Filter down to a manageable spatial network sub-grid (e.g., 5 sensors)
SELECTED_SENSORS = [400001, 400017, 400030, 400040, 400045]
SENSOR_TOPICS = [f"sensor-{s}" for s in SELECTED_SENSORS]

# Initialize Kafka Client
producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

# Auto-create the unique topics if they don't exist
try:
    admin_client = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in SENSOR_TOPICS]
    admin_client.create_topics(new_topics)
    print("Kafka topics verified/created.")
except Exception as e:
    pass 

# Load and slice data
print("Loading H5 dataset...")
df = pd.read_hdf('pems-bay.h5', key='speed')[SELECTED_SENSORS]

# Ensure timestamps are parsed natively for math calculations
df.index = pd.to_datetime(df.index.astype(str))

print(f"Starting historical replay at {REPLAY_SPEED}x speed...")

# Step 2 Formula Variables
T0 = df.index[0]         # Dataset start time
t0 = time.time()         # Wall-clock start time

for T, row in df.iterrows():
    # Calculate exact wall-clock target time using the required formula:
    # t_target = t0 + (T - T0) / speed
    time_elapsed_dataset = (T - T0).total_seconds()
    t_target = t0 + (time_elapsed_dataset / REPLAY_SPEED)
    
    # Wait until wall-clock time catches up to the target cadence
    while time.time() < t_target:
        time.sleep(0.01)
        
    # Step 2 Compliance: Publish each sensor reading as an individual message to its OWN topic
    for sensor_id, speed_val in row.items():
        topic_name = f"sensor-{sensor_id}"
        
        payload = {
            "run_id": RUN_ID,
            "sensor_id": int(sensor_id),
            "timestamp": str(T),
            "speed": float(speed_val)
        }
        
        producer.produce(
            topic=topic_name,
            key=str(sensor_id).encode('utf-8'),
            value=json.dumps(payload).encode('utf-8')
        )
        
    producer.poll(0)
    print(f"[Replay Time: {T}] Broadcasted individual streams for {len(SELECTED_SENSORS)} sensors.")

producer.flush()
