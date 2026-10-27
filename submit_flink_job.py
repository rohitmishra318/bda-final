"""
submit_flink_job.py
Uploads the built JAR to Flink and submits the EventIntelligenceJob.
"""
import json
import urllib.request

JAR_PATH   = r"flink-java\target\event-intelligence-flink-1.0-SNAPSHOT.jar"
FLINK_URL  = "http://localhost:8081"
ENTRY_CLASS = "com.esyasoft.eventintelligence.EventIntelligenceJob"
BOUNDARY   = "FlinkUploadBoundary7891"

# ── Step 1: Upload JAR ────────────────────────────────────────────────────────
print("Uploading JAR to Flink JobManager...")

with open(JAR_PATH, "rb") as f:
    jar_bytes = f.read()

part_header = (
    "--" + BOUNDARY + "\r\n"
    "Content-Disposition: form-data; name=\"jarfile\";"
    " filename=\"event-intelligence-flink-1.0-SNAPSHOT.jar\"\r\n"
    "Content-Type: application/octet-stream\r\n\r\n"
).encode("utf-8")

part_footer = ("\r\n--" + BOUNDARY + "--\r\n").encode("utf-8")
body = part_header + jar_bytes + part_footer

upload_req = urllib.request.Request(
    FLINK_URL + "/jars/upload",
    data=body,
    headers={"Content-Type": "multipart/form-data; boundary=" + BOUNDARY},
    method="POST",
)

with urllib.request.urlopen(upload_req, timeout=60) as resp:
    upload_resp = json.loads(resp.read())

jar_id = upload_resp["filename"].split("/")[-1]
print("JAR uploaded:", jar_id)

# ── Step 2: Submit job ────────────────────────────────────────────────────────
print("Submitting job...")

submit_body = json.dumps({
    "entryClass": ENTRY_CLASS,
    "parallelism": 1,
}).encode("utf-8")

run_req = urllib.request.Request(
    FLINK_URL + "/jars/" + jar_id + "/run",
    data=submit_body,
    headers={"Content-Type": "application/json"},
    method="POST",
)

with urllib.request.urlopen(run_req, timeout=30) as resp:
    run_resp = json.loads(resp.read())

job_id = run_resp.get("jobid", "unknown")
print("Job submitted successfully!")
print("Job ID :", job_id)
print("View at:", FLINK_URL + "/#/job/running")
