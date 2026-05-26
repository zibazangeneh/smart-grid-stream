

"""
Smart Grid IoT Simulator — Kafka version
=========================================
Simulates 500 virtual smart electricity meters across German grid zones.
Each meter sends a power reading every 5 seconds to a local Kafka topic.

Usage:
    # Mac/Linux
    export KAFKA_BROKER="localhost:9092"
    python simulator.py

    # Windows PowerShell
    $env:KAFKA_BROKER="localhost:9092"
    python simulator.py
"""

import json
import os
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_READINGS  = "raw-meter-readings"
TOPIC_ALERTS    = "anomaly-alerts"
NUM_METERS      = 500
INTERVAL_SEC    = 5

GRID_ZONES  = ["NRW-North", "NRW-South", "Bavaria", "Saxony", "Hamburg"]
METER_TYPES = ["residential", "commercial", "industrial"]

# ── Initialize Kafka Producer ──────────────────────────────────────────────────
print("🔌 Connecting to Kafka...")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
)

print(f"✅ Connected to Kafka at {KAFKA_BROKER}\n")

# ── Generate meter fleet ───────────────────────────────────────────────────────
meters = [
    {
        "meter_id":     f"METER-{str(i).zfill(4)}",
        "grid_zone":    random.choice(GRID_ZONES),
        "meter_type":   random.choice(METER_TYPES),
        "baseline_kwh": round(random.uniform(0.5, 8.0), 3),
    }
    for i in range(NUM_METERS)
]


def generate_reading(meter: dict) -> dict:
    """Generate one realistic power reading for a single meter."""
    kwh = meter["baseline_kwh"] * random.uniform(0.8, 1.2)
    is_anomaly = random.random() < 0.02
    if is_anomaly:
        kwh *= random.uniform(2.5, 5.0)

    return {
        "meter_id":    meter["meter_id"],
        "timestamp":   datetime.utcnow().isoformat() + "Z",
        "kwh_reading": round(kwh, 4),
        "voltage":     round(random.uniform(218.0, 242.0), 1),
        "frequency":   round(random.uniform(49.8, 50.2), 2),
        "grid_zone":   meter["grid_zone"],
        "meter_type":  meter["meter_type"],
        "is_anomaly":  is_anomaly,
    }


# ── Main loop ──────────────────────────────────────────────────────────────────
print(f"📡 Smart Grid Simulator running")
print(f"   Meters   : {NUM_METERS}")
print(f"   Topic    : {TOPIC_READINGS}")
print(f"   Interval : every {INTERVAL_SEC}s")
print(f"   Kafka UI : http://localhost:8080\n")
print("Press Ctrl+C to stop.\n")

total_sent = 0

try:
    while True:
        batch_start  = time.time()
        anomaly_count = 0

        for meter in meters:
            reading = generate_reading(meter)
            if reading["is_anomaly"]:
                anomaly_count += 1
                producer.send(TOPIC_ALERTS, key=reading["meter_id"], value={
                    "meter_id":   reading["meter_id"],
                    "timestamp":  reading["timestamp"],
                    "kwh":        reading["kwh_reading"],
                    "grid_zone":  reading["grid_zone"],
                    "alert_type": "CONSUMPTION_SPIKE",
                })
            producer.send(TOPIC_READINGS, key=reading["meter_id"], value=reading)

        producer.flush()
        total_sent += NUM_METERS

        elapsed     = time.time() - batch_start
        anomaly_str = f" | ⚠️  {anomaly_count} anomalies" if anomaly_count else ""
        print(f"✅ {NUM_METERS} readings sent | Total: {total_sent:,} | {elapsed:.2f}s{anomaly_str}")

        time.sleep(INTERVAL_SEC)

except KeyboardInterrupt:
    print("\n🛑 Simulator stopped.")
    producer.close()
