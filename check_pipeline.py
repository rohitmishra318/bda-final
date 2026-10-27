import urllib.request, json

# 1. Flink job state
r = urllib.request.urlopen("http://localhost:8081/jobs/overview")
jobs = json.loads(r.read())["jobs"]
running = [j for j in jobs if j["state"] == "RUNNING"]
print("=== FLINK ===")
print("Running jobs:", len(running))
for j in running:
    name = j["name"][:55]
    state = j["state"]
    print(" ", name, "-", state)

# 2. ClickHouse row counts
print()
print("=== CLICKHOUSE ROW COUNTS ===")
tables = [
    "processed_events",
    "meter_features_15m",
    "outage_incident_events",
    "topology_inference_results",
    "anomaly_detection_results",
]
for t in tables:
    q = ("SELECT count() FROM event_intelligence." + t).encode()
    req = urllib.request.Request(
        "http://localhost:8123/?user=event_user&password=change_me",
        data=q, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            count = resp.read().decode().strip()
        print(" ", t, "->", count, "rows")
    except Exception as e:
        print(" ", t, "-> ERROR:", e)
