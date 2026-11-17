import sys
import time
import h5py
import pandas as pd
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import uuid
from datetime import datetime, timezone

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))
from sensor_simulator.sensors import ReplayClock, Sensor, TrafficSensor, WeatherSensor  # noqa: E402

# === CONFIGURATION ===
BOOTSTRAP_SERVERS = 'localhost:9092'
REPLAY_SPEED = 60.0  # 60x speed: 5 minutes of data replayed in 5 seconds
RUN_ID = str(uuid.uuid4())

TRAFFIC_H5 = ROOT_DIR / "data" / "original_datasets" / "pems-bay.h5"
WEATHER_CSV = ROOT_DIR / "data" / "original_datasets" / "SJC.csv"
SENSORS_SUBSET = ROOT_DIR / "data" / "processing" / "pems-bay-sensors-subset.csv"

WEATHER_TIMESTAMP_COL = "valid"
WEATHER_STATION_COL = "station"
# Time zone of the replay; all published timestamps are ISO-8601 with offset
DATA_TZ = "America/Los_Angeles"

# Units of the ASOS/METAR columns (Iowa Environmental Mesonet export)
WEATHER_UNITS = {
    "tmpc": "degC",           # air temperature
    "dwpc": "degC",           # dew point
    "relh": "%",              # relative humidity
    "drct": "deg",            # wind direction
    "sped": "mph",            # wind speed
    "p01m": "mm",             # 1-hour precipitation
    "vsby": "miles",          # visibility
    "gust": "mph",            # wind gust
    "peak_wind_gust": "mph",  # peak wind gust
}
# Variables published only when the event happens:
#   p01m                  -> only when it rains (value != 0)
#   gust, peak_wind_gust  -> reported by the station only when a gust occurs
#                            (~7% / ~3% of the observations; 'M' otherwise)
WEATHER_EVENT_VARIABLES = {"p01m", "gust", "peak_wind_gust"}

print(f"Starting replay run: {RUN_ID}")

# Step 1 Compliance: Filter down to the selected sensors 
SELECTED_SENSORS = pd.read_csv(SENSORS_SUBSET)['sensor_id'].tolist()
# LEGACY (no longer used): old traffic topics, now TrafficSensor publishes on traffic.<id>.speed
# SENSOR_TOPICS = [f"sensor-{s}" for s in SELECTED_SENSORS]

# Initialize Kafka Client
producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

# Load and slice data
print("Loading H5 dataset...")
# pems-bay.h5 was written by pandas 0.15 and pd.read_hdf fails on it with pandas >= 3,
# so the frame is rebuilt directly from the HDF5 arrays
with h5py.File(TRAFFIC_H5, 'r') as f:
    g = f['speed']
    df = pd.DataFrame(
        g['block0_values'][:],
        index=pd.to_datetime(g['axis1'][:]),  # int64 nanoseconds since epoch
        columns=g['block0_items'][:],
    )[SELECTED_SENSORS]

# PEMS-BAY timestamps are Bay Area local time WITH daylight saving time
# (02:00-02:55 of 2017-03-12 does not exist). Making them timezone-aware turns
# T - T0 into real elapsed time, so the DST jump is a normal 5-minute step.
df.index = df.index.tz_localize(DATA_TZ, ambiguous="infer", nonexistent="raise")

# Step 2 Formula Variables
T0 = df.index[0]         # Dataset start time

# === WEATHER SENSORS ===
print("Loading weather dataset...")
# 'M' marks missing values in the ASOS export
weather_df = pd.read_csv(WEATHER_CSV, na_values=['M'])
# The IEM/ASOS export is in UTC: convert it to the same time zone as the traffic
weather_df[WEATHER_TIMESTAMP_COL] = (
    pd.to_datetime(weather_df[WEATHER_TIMESTAMP_COL])
      .dt.tz_localize("UTC")
      .dt.tz_convert(DATA_TZ)
)
weather_df = weather_df.set_index(WEATHER_TIMESTAMP_COL).sort_index()
station = str(weather_df[WEATHER_STATION_COL].iloc[0])
# every column except the timestamp (now the index) and the station id is a measured variable
weather_columns = [c for c in weather_df.columns if c != WEATHER_STATION_COL]
weather_df = weather_df[weather_columns].apply(pd.to_numeric, errors='coerce')
# replay only the weather that falls inside the traffic time window
weather_df = weather_df[(weather_df.index >= T0) & (weather_df.index <= df.index[-1])]

# Same T0 and speed as the traffic replay; t0 is set right before the replay starts
clock = ReplayClock(T0.to_pydatetime(), speed=REPLAY_SPEED)

# One WeatherSensor (and therefore one topic) per column
weather_sensors: list[WeatherSensor] = []
for col in weather_columns:
    weather_sensors.append(WeatherSensor(
        station=station,
        # '_' -> '-' : Kafka warns about topic names mixing '.' and '_'
        variable=col.replace("_", "-"),
        readings=Sensor.readings_from_series(weather_df[col]),
        clock=clock, producer=producer, run_id=RUN_ID,
        unit=WEATHER_UNITS.get(col),
        mode=Sensor.MODE_EVENT if col in WEATHER_EVENT_VARIABLES else Sensor.MODE_PERIODIC,
    ))

for s in weather_sensors:
    print(f"  {s}")

# === TRAFFIC SENSORS ===
# One TrafficSensor (and therefore one topic, traffic.<id>.speed) per PEMS-BAY detector
traffic_sensors: list[TrafficSensor] = [
    TrafficSensor(
        sensor_id=sensor_id,
        readings=Sensor.readings_from_series(df[sensor_id]),
        clock=clock, producer=producer, run_id=RUN_ID,
    )
    for sensor_id in SELECTED_SENSORS
]

for s in traffic_sensors:
    print(f"  {s}")

all_sensors: list[Sensor] = traffic_sensors + weather_sensors

# Auto-create the unique topics if they don't exist
try:
    admin_client = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    all_topics = [s.topic for s in all_sensors]
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in all_topics]
    admin_client.create_topics(new_topics)
    print("Kafka topics verified/created.")
except Exception as e:
    pass

print(f"Starting historical replay at {REPLAY_SPEED}x speed...")

# Single wall-clock origin t0 (shared ReplayClock) for traffic and weather,
# set slightly in the future so that all streams start aligned
clock.start(delay_s=1.0)
for s in all_sensors:
    s.start()

# === LEGACY TRAFFIC REPLAY (no longer used) ===
# Replaced by the TrafficSensor threads above. To re-enable it, also restore
# `import json` and SENSOR_TOPICS (and add SENSOR_TOPICS to all_topics).
#
# try:
#     for T, row in df.iterrows():
#         # Exact wall-clock target time from the shared clock:
#         # t_target = t0 + (T - T0) / speed
#         t_target = clock.wall_time(T.to_pydatetime())
#
#         # Wait until wall-clock time catches up to the target cadence
#         delay = t_target - time.monotonic()
#         if delay > 0:
#             time.sleep(delay)
#
#         # Step 2 Compliance: Publish each sensor reading as an individual message to its OWN topic
#         for sensor_id, speed_val in row.items():
#             topic_name = f"sensor-{sensor_id}"
#
#             payload = {
#                 "run_id": RUN_ID,
#                 "sensor_id": int(sensor_id),
#                 "timestamp": T.isoformat(),
#                 "speed": float(speed_val)
#             }
#
#             producer.produce(
#                 topic=topic_name,
#                 key=str(sensor_id).encode('utf-8'),
#                 value=json.dumps(payload).encode('utf-8')
#             )
#
#         # also serves the delivery callbacks of the weather sensors
#         producer.poll(0)
#         print(f"[Replay Time: {T}] Broadcasted individual streams for {len(SELECTED_SENSORS)} sensors.")
# except KeyboardInterrupt:
#     print("Stopping replay...")
# finally:
#     for s in weather_sensors:
#         s.stop()
#     for s in weather_sensors:
#         s.join()
#     producer.flush()

# === REPLAY ===
# Every sensor (traffic and weather) is its own thread publishing on its own topic.
# The main thread only serves the delivery callbacks and reports progress.
PROGRESS_EVERY_S = 5.0
try:
    last_report = time.monotonic()
    while any(s.is_alive() for s in all_sensors):
        producer.poll(0.5)
        if time.monotonic() - last_report >= PROGRESS_EVERY_S:
            last_report = time.monotonic()
            published = sum(s.published for s in traffic_sensors)
            print(f"[Replay] traffic messages published: {published}")
except KeyboardInterrupt:
    print("Stopping replay...")
finally:
    for s in all_sensors:
        s.stop()
    for s in all_sensors:
        s.join()
    producer.flush()

print("Traffic sensors:")
for s in traffic_sensors:
    print(f"  {s.topic}: published={s.published} failed={s.failed} max_lag={s.max_lag_s:.3f}s")
print("Weather sensors:")
for s in weather_sensors:
    print(f"  {s.topic}: published={s.published} failed={s.failed} max_lag={s.max_lag_s:.3f}s")
