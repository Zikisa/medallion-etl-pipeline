# Real-Time Cape Town Weather Analytics Platform (Medallion Architecture)

An automated, end-to-end data platform pipeline designed to ingest real-time geographic climate structures for Cape Town, process them through an enterprise-grade Medallion architecture (Bronze ➔ Silver ➔ Gold), and compile business-ready analytics layers using modern data stack tooling.

## 🏗️ Architectural Topology

[ Public API Source ]
│
▼ (Requests Ingestion)
┌────────────────────────────────────────────────────────┐
│ 📦 1. BRONZE LAYER : Immutable Landing Ledger           │
│      - Format: Time-stamped Raw JSON Payload Logs     │
└────────────────────────────────────────────────────────┘
│
▼ (Flattening / Cleaning / Schema Enforcement)
┌────────────────────────────────────────────────────────┐
│ 🥈 2. SILVER LAYER : Transactional Lakehouse            │
│      - Format: Delta Lake (Parquet binary blocks)     │
│      - Features: ACID guarantees, Deduplicated Keys   │
└────────────────────────────────────────────────────────┘
│
▼ (DuckDB Vectorized SQL Aggregations)
┌────────────────────────────────────────────────────────┐
│ 🥇 3. GOLD LAYER   : Analytical Reporting Layer         │
│      - Format: Compressed Parquet Dimensions/Facts     │
│      - Content: Aggregated summaries & business alerts │
└────────────────────────────────────────────────────────┘

## 🛠️ Technology Stack & Ecosystem
* **Core Language Engine:** Python 3.14
* **Lakehouse Framework:** Delta Lake (`deltalake` native Rust wrapper)
* **Analytical Processing Core:** DuckDB (Vectorized SQL Query Execution)
* **Data Cleansing & Framing:** Pandas 
* **Orchestration & CI/CD Automation:** GitHub Actions (Automated CRON Scheduling)

## 📁 Repository Blueprint
```text
medallion-etl-pipeline/
├── .github/
│   └── workflows/
│       └── etl_cron.yml             # GitHub Actions End-to-End Orchestrator
├── data/                            # Local Data Store (Git-tracked data layers)
│   ├── bronze/weather/              # Raw immutable JSON batches
│   ├── silver/weather_delta/        # Transactional Delta Lake tables with ACID history
│   └── gold/daily_weather_summary/  # Reporting-optimized Parquet aggregates
├── ingest_bronze_weather.py         # Step 1: Resilient API Ingestion Engine
├── transform_silver_weather.py      # Step 2: Schema Enforcement & Delta Writer
├── aggregate_gold_weather.py        # Step 3: DuckDB SQL Transformation Layer
└── requirements.txt                 # Manifest of structural dependencies
```

## ⚙️ Data Engineering Design Matrix

### 1. Robust API Defensive Safeguards (Bronze)
The `ingest_bronze_weather.py` engine interacts with the open-source Open-Meteo API. It implements rigorous logging, execution safety limits (`timeout=15`), and HTTP exception handling (`raise_for_status()`). Incoming rows are injected with precise ingestion footprints (`_ingested_at`, `_source_system`) to guarantee absolute data lineage tracking.

### 2. Transaction Security & Idempotency (Silver)
The transformation layer unpacks nested payloads, enforces uniform data types (`float64`), and appends records directly into the **Delta Lake** storage standard. By defining a strict logical key footprint over timestamps, it systematically prunes duplicated files during data restructures—guaranteeing pipeline idempotency.

### 3. Serverless Analytical Compilations (Gold)
Rather than spinning up heavy, resource-draining cloud warehouses for micro-batch transformations, this pipeline leverages **DuckDB**. It directly inspects the underlying Silver Delta log, evaluates deep analytical window frames, and injects programmatic condition alerts (*"Heavy Rain Warning"*, *"High Heat Risk"*) to power downstream BI platforms (Power BI / Tableau).

## 🚀 Automation and Orchestration
The environment is fully hands-off, driven by **GitHub Actions** (`.github/workflows/etl_cron.yml`). 
* **CRON Sequence:** Automatically schedules and runs every morning at `04:00 UTC` (**06:00 AM Cape Town SAST**).
* **Caching Footprint:** Utilizes pip actions caching matrices to optimize processing speeds.
* **Commit Verification:** Includes native `[skip ci]` integration flags to write back data partitions securely into the workspace tracking logs without generating terminal recursion loops.

## 💻 Local Setup Verification
1. Clone the repository:
   ```bash
   git clone https://github.com
   cd medallion-etl-pipeline
   ```
2. Build and match dependencies inside your Python runtime environment:
   ```bash
   pip install -r requirements.txt
   ```
3. Execute the data steps sequentially to review terminal traces:
   ```bash
   python ingest_bronze_weather.py
   python transform_silver_weather.py
   python aggregate_gold_weather.py
   ```
