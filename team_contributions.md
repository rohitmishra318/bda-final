# Team Contributions

## Member 1 — Real-Time Streaming and Flink Processing

Responsible for developing the core real-time event-processing pipeline.

### Main contributions

- Kafka-based smart-meter event ingestion
- Apache Flink streaming pipeline
- Event validation
- Event catalogue enrichment
- Network hierarchy enrichment
- Event-time processing
- Stateful outage detection
- Modular plugin architecture
- Building and submitting the Java Flink job

### Technologies

Apache Kafka, Apache Flink, Java

### Professor-facing explanation

> I developed the real-time event-processing backbone using Kafka and Apache Flink, including event validation, event enrichment, event-time handling, and stateful outage processing.

### Relevant project areas

- `flink-java/src/main/java/`
- `flink-java/src/main/resources/usecases.json`
- `producer/replay_producer.py`

## Member 2 — Analytics and Machine Learning

Responsible for implementing the analytical intelligence and machine-learning use cases.

### Main contributions

- Reusable 15-minute streaming feature engineering
- Supply reliability analysis
- Outage frequency and duration analysis
- Topology inference
- Candidate feeder ranking
- Isolation Forest anomaly detection
- Python-to-Java model integration
- Validation of Python and Java prediction parity

### Technologies

Python, scikit-learn, Java, Apache Flink

### Professor-facing explanation

> I implemented the analytical and machine-learning layer, including reusable streaming features, outage reliability analysis, topology inference, and Isolation Forest-based anomaly detection.

### Relevant project areas

- `ml/`
- `flink-java/src/main/java/com/esyasoft/eventintelligence/TopologyInferencePlugin.java`
- `flink-java/src/main/java/com/esyasoft/eventintelligence/TopologyInferenceProcessFunction.java`
- `flink-java/src/main/java/com/esyasoft/eventintelligence/AnomalyDetectionPlugin.java`
- `flink-java/src/main/java/com/esyasoft/eventintelligence/AnomalyScoringFunction.java`
- `flink-java/src/main/java/com/esyasoft/eventintelligence/MeterFeatureWindowFunction.java`

## Member 3 — Data Storage, Visualization, and Deployment

Responsible for integrating the processed results into the analytical storage and visualization layers.

### Main contributions

- Kafka-to-ClickHouse data flow
- ClickHouse Kafka Engine tables
- ClickHouse materialized views
- Analytical storage schemas
- Grafana operational dashboards
- Dashboard panel queries and layout
- Docker Compose infrastructure
- System integration and demonstration

### Technologies

ClickHouse, Grafana, Docker, Docker Compose

### Professor-facing explanation

> I designed the analytical storage and visualization layer using ClickHouse and Grafana, integrated the streaming outputs, created the operational dashboards, and supported deployment through Docker Compose.

### Relevant project areas

- `clickhouse/sql/`
- `dashboard/real_time_event_intelligence_dashboard.json`
- `docker-compose.example.yml`
- `grafana/provisioning/`

## Shared Responsibilities

All team members contributed to:

- System integration
- Testing and debugging
- Documentation
- Performance and functionality verification
- Final presentation and project demonstration

## Overall Project Description

> Our team developed a modular real-time smart-meter event intelligence platform. The system replays historical events through Kafka, processes them using Apache Flink, applies outage, reliability, topology, and machine-learning analytics, stores the results in ClickHouse, and presents operational insights through Grafana dashboards.
