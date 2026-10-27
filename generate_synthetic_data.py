"""
generate_synthetic_data.py
==========================
Generates ALL missing data files required to run the Real-Time Event
Intelligence Platform without the private operational dataset.

Outputs:
  data/reference/event_catalogue.csv        <- loaded by Java Flink job + Python enrichment
  data/reference/network_hierarchy.csv      <- loaded by Java Flink job + Python enrichment
  data/sample_events.jsonl                  <- consumed by replay_producer.py
  data/anomaly_features_real.csv            <- consumed by ml/train_isolation_forest.py

Run:
  python generate_synthetic_data.py

Requirements:
  pip install pandas numpy
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

# ─────────────────────────────────────────────
# Network topology (synthetic but realistic)
# ─────────────────────────────────────────────

SUBSTATIONS = ["SUB_NORTH", "SUB_SOUTH", "SUB_EAST", "SUB_WEST"]

FEEDERS = {
    "SUB_NORTH": ["FEED_N1", "FEED_N2", "FEED_N3"],
    "SUB_SOUTH": ["FEED_S1", "FEED_S2"],
    "SUB_EAST":  ["FEED_E1", "FEED_E2", "FEED_E3"],
    "SUB_WEST":  ["FEED_W1", "FEED_W2"],
}

DTRS = {
    "FEED_N1": [("DTR_N1A", "NC001"), ("DTR_N1B", "NC002")],
    "FEED_N2": [("DTR_N2A", "NC003"), ("DTR_N2B", "NC004")],
    "FEED_N3": [("DTR_N3A", "NC005")],
    "FEED_S1": [("DTR_S1A", "SC001"), ("DTR_S1B", "SC002")],
    "FEED_S2": [("DTR_S2A", "SC003")],
    "FEED_E1": [("DTR_E1A", "EC001"), ("DTR_E1B", "EC002")],
    "FEED_E2": [("DTR_E2A", "EC003")],
    "FEED_E3": [("DTR_E3A", "EC004"), ("DTR_E3B", "EC005")],
    "FEED_W1": [("DTR_W1A", "WC001"), ("DTR_W1B", "WC002")],
    "FEED_W2": [("DTR_W2A", "WC003")],
}

# Generate 200 synthetic meters
NUM_METERS = 200
meters = []
for i in range(1, NUM_METERS + 1):
    msn = f"MSN{i:05d}"
    substation = random.choice(SUBSTATIONS)
    feeder = random.choice(FEEDERS[substation])
    dtr_name, dtr_code = random.choice(DTRS[feeder])
    meters.append({
        "msn": msn,
        "msn_normalized": msn.upper(),
        "substation_name": substation,
        "feeder_name": feeder,
        "dtr_name": dtr_name,
        "dtr_network_code": dtr_code,
    })

# ─────────────────────────────────────────────
# 1. network_hierarchy.csv
# ─────────────────────────────────────────────

NETWORK_HIERARCHY_PATH = Path("data/reference/network_hierarchy.csv")
NETWORK_HIERARCHY_PATH.parent.mkdir(parents=True, exist_ok=True)

with open(NETWORK_HIERARCHY_PATH, "w", newline="", encoding="utf-8") as f:
    fieldnames = [
        "msn_normalized", "msn", "substation_name",
        "feeder_name", "dtr_name", "dtr_network_code",
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(meters)

print(f"[OK] {NETWORK_HIERARCHY_PATH} — {len(meters)} meters")

# ─────────────────────────────────────────────
# 2. event_catalogue.csv
#    Columns required by EventCatalogueLoader.java:
#      event_id, event_tblrefid, event_name, event_state,
#      isrestoration, priorityname, eventclassification_name
#    Also used by enrichment_service.py:
#      + event_description
# ─────────────────────────────────────────────

EVENT_CATALOGUE = [
    # Power failure / restoration pairs
    {
        "event_id": 1,
        "event_tblrefid": 101,
        "event_name": "Power Failure",
        "event_description": "Meter lost power supply",
        "event_state": "OCCURRENCE",
        "isrestoration": "false",
        "priorityname": "High",
        "eventclassification_name": "Outage",
    },
    {
        "event_id": 2,
        "event_tblrefid": 101,
        "event_name": "Power Restored",
        "event_description": "Power supply restored to meter",
        "event_state": "RESTORATION",
        "isrestoration": "true",
        "priorityname": "High",
        "eventclassification_name": "Outage",
    },
    # Voltage events
    {
        "event_id": 3,
        "event_tblrefid": 102,
        "event_name": "Low Voltage",
        "event_description": "Voltage below threshold",
        "event_state": "OCCURRENCE",
        "isrestoration": "false",
        "priorityname": "Medium",
        "eventclassification_name": "Voltage",
    },
    {
        "event_id": 4,
        "event_tblrefid": 102,
        "event_name": "Voltage Normalised",
        "event_description": "Voltage returned to normal range",
        "event_state": "RESTORATION",
        "isrestoration": "true",
        "priorityname": "Medium",
        "eventclassification_name": "Voltage",
    },
    {
        "event_id": 5,
        "event_tblrefid": 103,
        "event_name": "High Voltage",
        "event_description": "Voltage above threshold",
        "event_state": "OCCURRENCE",
        "isrestoration": "false",
        "priorityname": "Medium",
        "eventclassification_name": "Voltage",
    },
    {
        "event_id": 6,
        "event_tblrefid": 103,
        "event_name": "High Voltage Cleared",
        "event_description": "High voltage condition cleared",
        "event_state": "RESTORATION",
        "isrestoration": "true",
        "priorityname": "Medium",
        "eventclassification_name": "Voltage",
    },
    # Tamper / meter events
    {
        "event_id": 7,
        "event_tblrefid": 104,
        "event_name": "Tamper Detected",
        "event_description": "Potential meter tampering detected",
        "event_state": "OCCURRENCE",
        "isrestoration": "false",
        "priorityname": "Critical",
        "eventclassification_name": "Tamper",
    },
    {
        "event_id": 8,
        "event_tblrefid": 104,
        "event_name": "Tamper Cleared",
        "event_description": "Tamper condition cleared",
        "event_state": "RESTORATION",
        "isrestoration": "true",
        "priorityname": "Critical",
        "eventclassification_name": "Tamper",
    },
    # Heartbeat / comms
    {
        "event_id": 9,
        "event_tblrefid": 105,
        "event_name": "Meter Heartbeat",
        "event_description": "Periodic meter health check-in",
        "event_state": "OCCURRENCE",
        "isrestoration": "false",
        "priorityname": "Low",
        "eventclassification_name": "Communication",
    },
    {
        "event_id": 10,
        "event_tblrefid": 106,
        "event_name": "Communication Loss",
        "event_description": "Meter failed to communicate",
        "event_state": "OCCURRENCE",
        "isrestoration": "false",
        "priorityname": "High",
        "eventclassification_name": "Communication",
    },
    {
        "event_id": 11,
        "event_tblrefid": 106,
        "event_name": "Communication Restored",
        "event_description": "Meter communication re-established",
        "event_state": "RESTORATION",
        "isrestoration": "true",
        "priorityname": "High",
        "eventclassification_name": "Communication",
    },
]

EVENT_CATALOGUE_PATH = Path("data/reference/event_catalogue.csv")

with open(EVENT_CATALOGUE_PATH, "w", newline="", encoding="utf-8") as f:
    fieldnames = [
        "event_id", "event_tblrefid", "event_name", "event_description",
        "event_state", "isrestoration", "priorityname",
        "eventclassification_name",
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(EVENT_CATALOGUE)

print(f"[OK] {EVENT_CATALOGUE_PATH} — {len(EVENT_CATALOGUE)} event types")

# ─────────────────────────────────────────────
# 3. sample_events.jsonl
#    Required fields (from replay_producer.py + Flink pipeline):
#      msn, msn_id, evnt_id, ts (ISO 8601), dt (YYYYMMDDhhmm),
#      log_seq_no, v_r, v_y, v_b
# ─────────────────────────────────────────────

EVENTS_PATH = Path("data/sample_events.jsonl")
EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)

# Build meter_id lookup
msn_to_id = {m["msn"]: idx + 1 for idx, m in enumerate(meters)}

# Event weights — heartbeat most common, failure/restore less
EVENT_WEIGHTS = {
    1: 0.05,   # Power Failure
    2: 0.04,   # Power Restored
    3: 0.06,   # Low Voltage
    4: 0.05,   # Voltage Normalised
    5: 0.04,   # High Voltage
    6: 0.04,   # High Voltage Cleared
    7: 0.01,   # Tamper
    8: 0.01,   # Tamper Cleared
    9: 0.55,   # Heartbeat
    10: 0.08,  # Comm Loss
    11: 0.07,  # Comm Restored
}
event_ids = list(EVENT_WEIGHTS.keys())
event_probs = list(EVENT_WEIGHTS.values())

# Generate 2 weeks of events starting 30 days ago
START_TIME = datetime.now() - timedelta(days=30)
END_TIME = START_TIME + timedelta(days=14)

events = []
log_seq = 1

# Each meter generates events roughly every 15 minutes on average
# We simulate by iterating per-15m bucket
current = START_TIME
while current < END_TIME:
    # Pick a random subset of meters active in this window
    active_count = random.randint(20, 80)
    active_meters = random.sample(meters, active_count)

    for meter in active_meters:
        msn = meter["msn"]
        evnt_id = random.choices(event_ids, weights=event_probs)[0]

        # Random jitter within the 15-minute bucket (in seconds)
        jitter = timedelta(seconds=random.randint(0, 899))
        event_time = current + jitter

        # Generate realistic voltage values (230V nominal ±10%)
        if evnt_id in [3]:  # Low Voltage
            v_base = random.uniform(190, 215)
        elif evnt_id in [5]:  # High Voltage
            v_base = random.uniform(245, 265)
        elif evnt_id in [1, 10]:  # Power failure / comm loss
            v_base = random.uniform(0, 10)
        else:
            v_base = random.uniform(220, 240)

        v_r = round(v_base + random.gauss(0, 1.5), 2)
        v_y = round(v_base + random.gauss(0, 1.5), 2)
        v_b = round(v_base + random.gauss(0, 1.5), 2)

        event = {
            "msn": msn,
            "msn_id": msn_to_id[msn],
            "evnt_id": evnt_id,
            "ts": event_time.isoformat(timespec="milliseconds"),
            "dt": int(current.strftime("%Y%m%d%H%M")),
            "log_seq_no": log_seq,
            "v_r": v_r,
            "v_y": v_y,
            "v_b": v_b,
        }
        events.append(event)
        log_seq += 1

    current += timedelta(minutes=15)

# Sort by timestamp
events.sort(key=lambda e: e["ts"])

with open(EVENTS_PATH, "w", encoding="utf-8") as f:
    for evt in events:
        f.write(json.dumps(evt) + "\n")

print(f"[OK] {EVENTS_PATH} — {len(events):,} events over 14 days")

# ─────────────────────────────────────────────
# 4. anomaly_features_real.csv
#    Required by ml/train_isolation_forest.py:
#      msn, window_start, window_end,
#      event_count_15m, avg_voltage_15m,
#      voltage_range_15m, power_failure_count_15m
# ─────────────────────────────────────────────

FEATURES_PATH = Path("data/anomaly_features_real.csv")
rows = []

# Aggregate real events into 15-minute windows
# Group events by (msn, 15m bucket)
from collections import defaultdict

bucket_events = defaultdict(list)
for evt in events:
    ts = datetime.fromisoformat(evt["ts"])
    # Round down to nearest 15-min boundary
    bucket_start = ts.replace(
        minute=(ts.minute // 15) * 15,
        second=0,
        microsecond=0
    )
    key = (evt["msn"], bucket_start)
    bucket_events[key].append(evt)

for (msn, window_start), evts in bucket_events.items():
    window_end = window_start + timedelta(minutes=15)

    voltages = [
        v for e in evts
        for v in [e.get("v_r"), e.get("v_y"), e.get("v_b")]
        if v is not None and v > 0
    ]

    avg_voltage = round(float(np.mean(voltages)), 4) if voltages else None
    voltage_range = round(float(np.max(voltages) - np.min(voltages)), 4) if len(voltages) > 1 else 0.0

    power_fail_count = sum(
        1 for e in evts if e.get("evnt_id") == 1
    )

    rows.append({
        "msn": msn,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "event_count_15m": len(evts),
        "avg_voltage_15m": avg_voltage,
        "voltage_range_15m": voltage_range,
        "power_failure_count_15m": power_fail_count,
    })

features_df = pd.DataFrame(rows)
features_df.to_csv(FEATURES_PATH, index=False)

print(f"[OK] {FEATURES_PATH} — {len(features_df):,} 15-minute feature windows")
print(f"     Unique meters: {features_df['msn'].nunique()}")
print(f"     Anomalous windows (power_failure_count>0): {(features_df['power_failure_count_15m'] > 0).sum()}")

# ─────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────

print()
print("=" * 55)
print("Synthetic data generation complete!")
print("=" * 55)
print()
print("Files created:")
print(f"  {NETWORK_HIERARCHY_PATH}")
print(f"  {EVENT_CATALOGUE_PATH}")
print(f"  {EVENTS_PATH}")
print(f"  {FEATURES_PATH}")
print()
print("Next steps:")
print("  1. Copy CSVs into the Flink JAR resources:")
print("     flink-java/src/main/resources/reference/event_catalogue.csv")
print("     flink-java/src/main/resources/reference/network_hierarchy.csv")
print()
print("  2. Train the ML model:")
print("     python ml/train_isolation_forest.py")
print()
print("  3. Export the model to JSON (for Java runtime):")
print("     python ml/export_isolation_forest_json.py")
print()
print("  4. Copy the model JSON into Flink resources:")
print("     flink-java/src/main/resources/models/isolation_forest_model.json")
print()
print("  5. Start Docker infrastructure, build Flink JAR, then run:")
print("     python producer/replay_producer.py --file data/sample_events.jsonl --speed 3600")
