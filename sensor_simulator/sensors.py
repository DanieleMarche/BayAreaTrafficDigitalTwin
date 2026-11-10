"""
sensors.py — Virtual weather sensors for the historical replay (Digital Twin, Step 2),
publishing to Apache Kafka.

NOTE: for now only the weather stream uses these classes. The traffic stream
(PEMS-BAY) is still replayed by the original loop in stream/producer.py, on
topics sensor-<id>.

Requirements covered
--------------------
1. Every sensor produces its OWN stream (no aggregation): each sensor is an
   independent thread that replays only its own readings. Whether a sensor is
   PERIODIC or EVENT-based is declared in its class (see the table below) and
   repeated in every message ("mode" field).
2. Cadence: the reading with timestamp T is published at
       wall_time(T) = t0 + (T - T0) / speed
   t0, T0 and speed are shared by all sensors through a single ReplayClock.
3. Broker: each sensor publishes to ITS OWN Kafka topic, one message per reading.
   All sensors share one Producer (one connection to the broker): this does not
   change the streams, which are defined by topic, not by connection.

Sensors
-------
| Class          | Measures                          | Mode      | Topic                         |
|----------------|-----------------------------------|-----------|-------------------------------|
| WeatherSensor  | ONE variable of ONE station       | periodic  | weather.<station>.<variable>  |
|                | (e.g. temperature at SJC)         | or event  |                               |
|                |                                   | (config.) |                               |

Kafka topic names may contain only [a-zA-Z0-9._-]: '/' (MQTT style) is not allowed.
'_' is avoided on purpose, because Kafka warns about names that mix '.' and '_'.
"""
from __future__ import annotations

import json
import math
import threading
import time
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

# Dataset timestamps are Bay Area local time without a time zone.
DATA_TZ = ZoneInfo("America/Los_Angeles")


# --------------------------------------------------------------------------- #
# Shared clock
# --------------------------------------------------------------------------- #
class ReplayClock:
    """Single time reference for all sensors (same t0, same T0, same speed)."""

    def __init__(self, dataset_start: datetime, speed: float = 1.0):
        if speed <= 0:
            raise ValueError("speed must be > 0")
        self.T0 = dataset_start          # dataset start (simulated time)
        self.speed = speed               # replay multiplier
        self.t0: float | None = None     # replay start (wall-clock)

    def start(self, delay_s: float = 1.0) -> None:
        """Set t0 slightly in the future so that all threads start aligned.

        time.monotonic() is used as the wall clock: it measures real elapsed
        time like time.time(), but never jumps (e.g. NTP corrections).
        """
        self.t0 = time.monotonic() + delay_s

    def wall_time(self, T: datetime) -> float:
        """Instant at which the reading with timestamp T must be published."""
        if self.t0 is None:
            raise RuntimeError("call ReplayClock.start() before starting the sensors")
        return self.t0 + (T - self.T0).total_seconds() / self.speed


# --------------------------------------------------------------------------- #
# Base sensor
# --------------------------------------------------------------------------- #
class Sensor(threading.Thread):
    """Generic sensor: replays a list of (timestamp, value) readings on its own topic."""

    MODE_PERIODIC = "periodic"   # readings at a fixed interval
    MODE_EVENT = "event"         # readings only when something happens

    sensor_type = "generic"

    def __init__(
        self,
        sensor_id: str,
        topic: str,
        readings: Iterable[tuple[datetime, float]],
        clock: ReplayClock,
        producer: Producer,
        run_id: str,
        mode: str = MODE_PERIODIC,
        unit: str | None = None,
    ):
        super().__init__(name=f"sensor-{sensor_id}", daemon=True)
        if mode not in (self.MODE_PERIODIC, self.MODE_EVENT):
            raise ValueError(f"invalid mode: {mode}")
        self.sensor_id = str(sensor_id)
        self.topic = topic
        self.clock = clock
        self.producer = producer
        self.run_id = run_id
        self.mode = mode
        self.unit = unit

        # keep only valid readings, sorted by time
        self.readings = sorted(
            ((T, float(v)) for T, v in readings if self.is_valid(v)),
            key=lambda r: r[0],
        )

        self._stop_event = threading.Event()
        # statistics, useful to verify the cadence and the delivery
        self.published = 0
        self.failed = 0
        self.max_lag_s = 0.0

    # ---- extension points for subclasses ----------------------------------- #
    def is_valid(self, value) -> bool:
        """Discard missing values (None/NaN). Subclasses can add their own rules."""
        if value is None:
            return False
        try:
            return not math.isnan(float(value))
        except (TypeError, ValueError):
            return False

    def build_payload(self, T: datetime, value: float) -> dict:
        """Message content. Subclasses add their own fields."""
        return {
            "run_id": self.run_id,
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "mode": self.mode,
            "ts": T.replace(tzinfo=DATA_TZ).isoformat(),  # ORIGINAL dataset timestamp
            "value": value,
            "unit": self.unit,
            "emitted_at": time.time(),   # real send time: lets consumers measure latency
        }

    # ---- replay loop ------------------------------------------------------- #
    def run(self) -> None:
        for T, value in self.readings:
            target = self.clock.wall_time(T)
            delay = target - time.monotonic()
            # wait() instead of sleep(): returns immediately if stop() is called
            if delay > 0 and self._stop_event.wait(delay):
                break
            if self._stop_event.is_set():
                break

            self.max_lag_s = max(self.max_lag_s, time.monotonic() - target)
            self._publish(T, value)

    def _publish(self, T: datetime, value: float) -> None:
        payload = json.dumps(self.build_payload(T, value)).encode("utf-8")
        event_time_ms = int(T.replace(tzinfo=DATA_TZ).timestamp() * 1000)
        while True:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=self.sensor_id.encode("utf-8"),
                    value=payload,
                    timestamp=event_time_ms,       # Kafka record time = event time
                    on_delivery=self._on_delivery,
                )
                self.published += 1
                return
            except BufferError:
                # local send queue full: wait for some deliveries, then retry
                self.producer.poll(0.1)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            print(f"[{self.sensor_id}] delivery failed on {msg.topic()}: {err}")

    def stop(self) -> None:
        self._stop_event.set()

    # ---- utilities --------------------------------------------------------- #
    @staticmethod
    def readings_from_series(series) -> list[tuple[datetime, float]]:
        """Convert a pandas Series (index = timestamp) into a list of readings."""
        s = series.dropna()
        return list(zip(s.index.to_pydatetime(), s.to_numpy()))

    def __repr__(self) -> str:
        return (f"{type(self).__name__}(id={self.sensor_id}, topic={self.topic}, "
                f"mode={self.mode}, readings={len(self.readings)})")


# --------------------------------------------------------------------------- #
# Subclasses
# --------------------------------------------------------------------------- #
class WeatherSensor(Sensor):
    """
    A single variable of a weather station (e.g. temperature at SJC).
    Each variable is a separate sensor: no per-station aggregation.
    Mode: configurable.
      - periodic: publishes every observation (e.g. temperature, wind)
      - event:    publishes only when the event occurs, i.e. value != 0
                  (e.g. precipitation: a message only when it rains)
    Topic: weather.<station>.<variable>
    """

    sensor_type = "weather"
    TOPIC_PREFIX = "weather"

    def __init__(self, station, variable, readings, clock, producer, run_id,
                 unit: str | None = None, mode: str = Sensor.MODE_PERIODIC):
        self.station = station
        self.variable = variable
        self.mode = mode          # already needed by is_valid() during super().__init__
        super().__init__(
            sensor_id=f"{station}-{variable}",
            topic=f"{self.TOPIC_PREFIX}.{station}.{variable}",
            readings=readings,
            clock=clock,
            producer=producer,
            run_id=run_id,
            mode=mode,
            unit=unit,
        )

    def is_valid(self, value) -> bool:
        if not super().is_valid(value):
            return False
        # event-based sensor: no message when the event is not happening
        if self.mode == Sensor.MODE_EVENT and float(value) == 0.0:
            return False
        return True

    def build_payload(self, T, value):
        payload = super().build_payload(T, value)
        payload["station"] = self.station
        payload["variable"] = self.variable
        return payload
