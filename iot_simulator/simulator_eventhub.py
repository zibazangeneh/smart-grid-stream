"""
Smart Grid IoT Simulator — Azure Event Hubs version
=====================================================
Simulates 500 virtual smart electricity meters across German grid zones.
Each meter sends a power reading every 5 seconds to Azure Event Hubs.

Usage:
    # Mac/Linux
    export EVENTHUB_CONN_STR="your-connection-string"
    python simulator_eventhub.py

    # Windows PowerShell
    $env:EVENTHUB_CONN_STR="your-connection-string"
    python simulator_eventhub.py
"""

import json
import os
import time
import random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData

# ── Configuration ──────────────────────────────────────────────────────────────
CONNECTION_STR = os.environ.get("EVENTHUB_CONN_STR")   # never hardcode secrets
EVENTHUB_NAME  = "raw-meter-readings"
NUM_METERS     = 500
INTERVAL_SEC   = 5

if not CONNECTION_STR:
    raise ValueError(
        "EVENTHUB_CONN_STR environment variable is not set.\n"
        "Mac/Linux: export EVENTHUB_CONN_STR='your-connection-string'\n"
        "Windows:   $env:EVENTHUB_CONN_STR='your-connection-string'"
    )

GRID_ZONES  = ["NRW-North", "NRW-South", "Bavaria", "Saxony", "Hamburg"]
METER_TYPES = ["residential", "commercial", "industrial"]

# ── Initialize Event Hub Producer ─────────────────────────────────────────────
print("🔌 Connecting to Azure Event Hubs...")

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME,
)

print("✅ Connected to Azure Event Hubs\n")

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
print(f"   Meters    : {NUM_METERS}")
print(f"   Event Hub : {EVENTHUB_NAME}")
print(f"   Interval  : every {INTERVAL_SEC}s\n")
print("Press Ctrl+C to stop.\n")

total_sent = 0

try:
    while True:
        batch_start   = time.time()
        anomaly_count = 0

        event_data_batch = producer.create_batch()

        for meter in meters:
            reading = generate_reading(meter)
            if reading["is_anomaly"]:
                anomaly_count += 1
            event_data_batch.add(EventData(json.dumps(reading)))

        producer.send_batch(event_data_batch)
        total_sent += NUM_METERS

        elapsed     = time.time() - batch_start
        anomaly_str = f" | ⚠️  {anomaly_count} anomalies" if anomaly_count else ""
        print(f"✅ {NUM_METERS} readings sent | Total: {total_sent:,} | {elapsed:.2f}s{anomaly_str}")

        time.sleep(INTERVAL_SEC)

except KeyboardInterrupt:
    print("\n🛑 Simulator stopped.")
    producer.close()
