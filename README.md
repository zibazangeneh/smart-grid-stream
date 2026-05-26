# ⚡ Smart Grid Stream

> A real-time IoT data engineering platform I built to simulate, process, and analyze energy consumption data across German power grid zones — using a modern cloud-native stack.

![Status](https://img.shields.io/badge/status-in%20progress-orange)
![Stack](https://img.shields.io/badge/stack-Kafka%20%7C%20PySpark%20%7C%20dbt%20%7C%20Snowflake%20%7C%20Azure-blue)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 💡 Why I Built This

Germany's *Energiewende* — the national energy transition — generates enormous amounts of sensor data across the power grid. As a data engineer based in NRW, I wanted to build something that reflects a real problem in this region: how do you process hundreds of thousands of smart meter readings in real time, detect anomalies, and make the data available for analysis?

This project is my answer to that question. It's not a tutorial follow-along — I designed the architecture, chose the tools, and built it end to end. The goal was to close the gap between the batch ETL work I've done professionally and the streaming, cloud-native stack that modern senior DE roles in Germany require.

---

## 🏗️ Architecture

```
IoT Simulator (Python)
      │
      │  500 virtual smart meters
      │  one reading per meter every 5 seconds
      │  ~6,000 events per minute
      │
      ▼
Apache Kafka (local dev) / Azure Event Hubs (cloud)
  Topics: raw-meter-readings (3 partitions) · anomaly-alerts
      │
      ▼
PySpark Structured Streaming · Azure Databricks
  • 5-minute tumbling windows with 2-minute watermark
  • Rule-based anomaly detection (2.5× rolling baseline)
  • Medallion architecture: Bronze → Silver → Gold
  • Delta Lake on Azure ADLS Gen2
      │
      ▼
Snowflake (auto-ingested via Snowpipe from ADLS Gold layer)
      │
      ▼
dbt Core
  staging → intermediate → marts
  fct_energy_consumption · dim_meters · dim_grid_zones · dim_dates
  Incremental models · source freshness checks · 100% test coverage
      │
      ▼
Apache Airflow · hourly DAG · Kafka lag monitoring · failure alerts
GitHub Actions · CI on every PR: dbt compile + dbt test
```

---

## 🛠️ Tech Stack

| Layer | Technology | Why I chose it |
|---|---|---|
| Event streaming | Apache Kafka + Azure Event Hubs | Industry standard; Kafka-compatible managed service removes ops overhead |
| Stream processing | PySpark Structured Streaming | Native windowing and late-data handling via watermarks |
| Cloud storage | Azure ADLS Gen2 + Delta Lake | ACID transactions on streaming data; Medallion architecture |
| Cloud compute | Azure Databricks | Managed Spark; native ADLS and Snowflake integration |
| Data warehouse | Snowflake + Snowpipe | Auto-ingest from ADLS; separation of storage and compute |
| Transformation | dbt Core | Version-controlled SQL; lineage docs; incremental models for streaming |
| Orchestration | Apache Airflow | DAG-based scheduling; Kafka lag checks before triggering Spark |
| CI/CD | GitHub Actions | dbt tests run on every PR; no broken models reach production |

---

## 📁 Project Structure

```
smart-grid-stream/
├── iot_simulator/
│   ├── simulator.py                  # Kafka version — local development
│   ├── simulator_eventhub.py         # Azure Event Hubs version — cloud
│   └── requirements.txt
├── kafka/
│   ├── docker-compose.yml            # Kafka + Zookeeper + Kafka UI
│   └── setup_topics.sh               # Creates topics with correct partitions
├── spark_streaming/
│   └── stream_processor_eventhub.py  # PySpark Structured Streaming job
├── smart_grid_dbt/
│   ├── models/
│   │   ├── staging/                  # stg_meter_readings, stg_anomaly_alerts
│   │   ├── intermediate/             # int_consumption_windowed
│   │   └── marts/                    # fct_energy_consumption, dim_*
│   ├── tests/
│   └── dbt_project.yml
├── orchestration/
│   └── dags/
│       └── smart_grid_dag.py         # Airflow DAG
└── .github/
    └── workflows/
        └── dbt_ci.yml                # GitHub Actions CI
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Docker Desktop
- VS Code
- Azure account (free tier)

### 1 — Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/smart-grid-stream.git
cd smart-grid-stream
```

### 2 — Set up Python environment

```bash
python -m venv venv
source venv/bin/activate       # Mac/Linux
venv\Scripts\activate          # Windows

pip install -r iot_simulator/requirements.txt
```

### 3 — Start Kafka locally

```bash
cd kafka
docker compose up -d
bash setup_topics.sh
```

Kafka UI: http://localhost:8080

### 4 — Run the IoT simulator

```bash
# Local Kafka
python iot_simulator/simulator.py

# Azure Event Hubs (set env var first)
export EVENTHUB_CONN_STR="your-connection-string"   # Mac/Linux
python iot_simulator/simulator_eventhub.py
```

---

## 📊 Message Schema

Each smart meter sends a JSON reading every 5 seconds:

```json
{
  "meter_id":    "METER-0247",
  "timestamp":   "2026-05-20T09:15:32Z",
  "kwh_reading": 3.4521,
  "voltage":     229.4,
  "frequency":   50.01,
  "grid_zone":   "NRW-North",
  "meter_type":  "residential",
  "is_anomaly":  false
}
```

---

## ❓ Business Questions This Platform Answers

1. Which NRW grid zones consume the most energy per hour?
2. Which meters show anomalous consumption spikes — and when?
3. What is the peak demand window per day across the grid?
4. How does consumption differ between residential, commercial, and industrial meters?
5. Which zones show the highest anomaly rate over time?

---

## 🔐 Environment Variables

No secrets are hardcoded in this project. Set these before running:

| Variable | Used in | Description |
|---|---|---|
| `EVENTHUB_CONN_STR` | simulator_eventhub.py, Databricks | Azure Event Hubs connection string |
| `AZURE_STORAGE_KEY` | Databricks notebook | Azure ADLS Gen2 account key |
| `KAFKA_BROKER` | simulator.py | Kafka broker address (default: localhost:9092) |

---

## 📈 Project Status

- [x] **Week 1** — Kafka setup + IoT simulator (500 smart meters)
- [x] **Week 2** — PySpark Structured Streaming + local Delta Lake Medallion architecture (Bronze → Silver → Gold)
- [ ] **Week 3** — dbt + Snowflake star schema
- [ ] **Week 4** — Airflow orchestration + GitHub Actions CI/CD

---

## 🧠 What I Learned Building This

- Kafka partition strategy matters: keying by `meter_id` ensures readings for the same meter always land in the same partition — critical for stateful aggregations downstream
- PySpark watermarking is not optional for IoT data: out-of-order events will silently break windowed aggregations without it
- Azure Event Hubs as a Kafka-compatible endpoint is a clean enterprise pattern — same producer code, managed infrastructure, no Kafka ops
- Never hardcode secrets: environment variables keep credentials out of version control entirely

---

## 👩‍💻 About Me

I'm a data engineer with a PhD in Physics, based in Aachen, NRW. My background is in pharma/life sciences data pipelines — ETL, data governance, GDPR compliance. I built this project to expand into modern cloud-native streaming, which is where the German data engineering market is heading in 2026.

📍 Aachen, NRW · 🇩🇪 German citizen · Open to Senior Data Engineer roles in NRW

---

*Built by Dr. Ziba Zangenehpourzadeh · 2026*
