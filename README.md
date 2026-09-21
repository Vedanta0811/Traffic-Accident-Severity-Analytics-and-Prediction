# Traffic Accident Severity Analytics and Prediction

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)]([https://traffic-accident-severity-analytics-and-prediction-3yzwgf7tuhc.streamlit.app/)](https://traffic-accident-severity-analytics-and-prediction-vedanta08.streamlit.app/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![MLflow](https://img.shields.io/badge/MLflow-Tracking-0194E2.svg)](https://mlflow.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)](https://www.docker.com/)

> **Live Deployment Link**: [https://traffic-accident-severity-analytics-and-prediction-3yzwgf7tuhc.streamlit.app/](https://traffic-accident-severity-analytics-and-prediction-3yzwgf7tuhc.streamlit.app/)

An end-to-end Data Engineering and MLOps system that combines public traffic collision records with historical atmospheric weather from the **Open-Meteo API** to build an analytical data warehouse, interactive dashboards, and a machine learning service predicting accident severity.

---

## Key Highlights & System Architecture

### Part 1: Data Engineering & Analytics Warehouse
- **Multi-Source Ingestion**: Ingests UK Road Safety Open Data benchmark records and enriches them with historical hourly weather variables from the **Open-Meteo API**.
- **Raw Landing Zone & Audit Trail**: Immutable landing zone in `data/raw/` with full execution accounting logged to `ingestion_audit`.
- **Data Quality & Quarantine Gate**: Automated validation quarantining corrupted coordinates, negative speeds, or invalid dates to `quarantine_rejected_records` without manual tampering.
- **Dimensional Star Schema & Data Marts**: Relational warehouse in **PostgreSQL / PostGIS** with dimensions (`dim_date`, `dim_location`, `dim_road`, `dim_weather`, `dim_severity`), fact table (`fact_accidents`), and analytical aggregate marts.
- **Pipeline Orchestration**: Scheduled **Apache Airflow DAG** (`accident_pipeline_dag.py`) managing staged extraction, validation, joins, and mart refreshes.
- **Interactive Streamlit BI Dashboard**: Multi-page dashboard featuring KPI cards, temporal peak heatmaps, environmental risk matrices, and 3D **PyDeck geospatial density maps**.

### Part 2: MLOps Pipeline Extension
- **Problem Formulation**: Multi-class classification predicting collision severity: **Fatal** (1), **Serious** (2), and **Slight** (3).
- **Chronological Splitting**: Prevents temporal data leakage via 70/15/15 chronological Train/Val/Test partitioning.
- **Severe Class Imbalance Handling**: Tackles extreme imbalance (Fatal <3%) using SMOTE and cost-sensitive class weighting.
- **Multi-Model Benchmarking**: Benchmarks **Random Forest**, **LightGBM**, and **XGBoost** with experiment tracking in **MLflow**.
- **Production Packaging**: Preprocessing pipeline and classifier packaged into a reproducible inference artifact.
- **FastAPI Microservice**: Low-latency REST API (`/health`, `/predict`, `/predict/batch`, `/metrics`).
- **Containerization**: Multi-container orchestration via **Docker Compose** (Postgres, Airflow, MLflow, FastAPI, Streamlit).
- **Continuous Monitoring**: Detects feature drift (PSI / KS-test), location drift, and class shift with automated retraining trigger criteria.

---

## Repository Structure

```text
traffic-accident-mlops/
├── dags/                          # Apache Airflow DAGs
│   └── accident_pipeline_dag.py
├── src/
│   ├── ingestion/                 # Raw ingestion scripts
│   │   ├── accident_ingest.py
│   │   └── weather_ingest.py
│   ├── etl/                       # Validation, transformation & warehouse loader
│   │   ├── db.py
│   │   ├── validation.py
│   │   └── transform.py
│   ├── ml/                        # Feature engineering, training & evaluation
│   │   ├── features.py
│   │   └── train.py
│   ├── api/                       # FastAPI serving
│   │   ├── main.py
│   │   └── schemas.py
│   └── monitoring/                # Drift detection & retraining engine
│       └── drift_detector.py
├── streamlit_app/                 # Streamlit BI & prediction dashboard
│   ├── app.py
│   └── pages/
│       ├── 1_Temporal_Dynamics.py
│       ├── 2_Environmental_Risk.py
│       ├── 3_Geospatial_Hotspots.py
│       ├── 4_Vehicle_Casualty_DeepDive.py
│       ├── 5_MLOps_Live_Prediction.py
│       └── 6_Pipeline_Audit_Drift.py
├── sql/                           # DDL schemas & analytical marts
│   ├── 01_staging_and_quarantine.sql
│   ├── 02_star_schema.sql
│   └── 03_analytical_marts.sql
├── docs/                          # Project documentation
│   ├── dataset_source_guide.md
│   ├── architecture_and_pipeline_flow.md
│   ├── data_dictionary.md
│   └── execution_evidence.md
├── reports/                       # Academic project report & drift outputs
│   └── PROJECT_REPORT_PART1_PART2.md
├── tests/                         # Integration test suite
│   └── test_pipeline.py
├── models/                        # Serialized inference model & artifacts
│   ├── accident_severity_model.joblib
│   └── test_evaluation_report.json
├── data/
│   ├── raw/                       # Immutable raw landing zone
│   ├── rejected/                  # Quarantined record logs
│   └── traffic_accidents.db       # Local SQLite relational database (PostgreSQL fallback)
├── docker-compose.yml             # Multi-service container orchestration
├── Dockerfile.api                 # Container build for FastAPI
├── Dockerfile.streamlit           # Container build for Streamlit
├── requirements.txt               # Python package dependencies
├── .env.example                   # Environment configuration template
└── README.md
```

---

## Quickstart & Execution Guide

### 1. Local Python Environment (Standalone Mode)
Run the pipeline directly in Python:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run Data Ingestion (Acquires accidents & Open-Meteo weather)
python src/ingestion/accident_ingest.py
python src/ingestion/weather_ingest.py

# 3. Run Data Validation & Star Schema ETL Warehouse Load
python src/etl/transform.py

# 4. Train Models with Class Imbalance & MLflow Experiment Tracking
python src/ml/train.py

# 5. Evaluate Data Drift & Retraining Status
python src/monitoring/drift_detector.py

# 6. Run Automated Test Suite
pytest tests/test_pipeline.py -v

# 7. Launch Streamlit Analytics Dashboard
streamlit run streamlit_app/app.py

# 8. (Optional) Launch FastAPI Model Service
uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

### 2. Multi-Container Docker Compose Mode
When Docker Desktop is active:

```bash
# Build and launch all services (PostgreSQL, Airflow, MLflow, FastAPI, Streamlit)
docker compose up --build
```

- **Streamlit Analytics Dashboard**: [http://localhost:8501](http://localhost:8501)
- **FastAPI Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **MLflow Tracking Server**: [http://localhost:5000](http://localhost:5000)
- **Airflow Webserver**: [http://localhost:8080](http://localhost:8080)

---

## Project Deliverables & Artifact Checklist

| Item | Required Artifact | File Location in Repository | Status |
| :---: | :--- | :--- | :---: |
| **1** | Source code and ETL scripts | [`src/ingestion/`](file:///d:/College/mlops/src/ingestion), [`src/etl/`](file:///d:/College/mlops/src/etl) | Completed |
| **2** | Airflow DAG orchestration workflow | [`dags/accident_pipeline_dag.py`](file:///d:/College/mlops/dags/accident_pipeline_dag.py) | Completed |
| **3** | Database schema and populated tables | [`sql/`](file:///d:/College/mlops/sql), [`data/traffic_accidents.db`](file:///d:/College/mlops/data/traffic_accidents.db) | Completed |
| **4** | Dataset source information & access instructions | [`docs/dataset_source_guide.md`](file:///d:/College/mlops/docs/dataset_source_guide.md) | Completed |
| **5** | Architecture diagram and pipeline flow | [`docs/architecture_and_pipeline_flow.md`](file:///d:/College/mlops/docs/architecture_and_pipeline_flow.md) | Completed |
| **6** | Data dictionary and validation rules | [`docs/data_dictionary.md`](file:///d:/College/mlops/docs/data_dictionary.md) | Completed |
| **7** | Streamlit application & cloud deployment | [`streamlit_app/app.py`](file:///d:/College/mlops/streamlit_app/app.py) & [`pages/`](file:///d:/College/mlops/streamlit_app/pages) | Completed |
| **8** | Academic report (Part 1 & Part 2) | [`reports/PROJECT_REPORT_PART1_PART2.md`](file:///d:/College/mlops/reports/PROJECT_REPORT_PART1_PART2.md) | Completed |
| **9** | Execution evidence & logs | [`docs/execution_evidence.md`](file:///d:/College/mlops/docs/execution_evidence.md) | Completed |
| **10**| Setup and run documentation | [`README.md`](file:///d:/College/mlops/README.md) | Completed |
| **Part 2**| Model pipeline, MLflow tracking & registry | [`src/ml/train.py`](file:///d:/College/mlops/src/ml/train.py), [`models/`](file:///d:/College/mlops/models) | Completed |
| **Part 2**| FastAPI microservice & Docker containerization | [`src/api/main.py`](file:///d:/College/mlops/src/api/main.py), [`Dockerfile.api`](file:///d:/College/mlops/Dockerfile.api), [`docker-compose.yml`](file:///d:/College/mlops/docker-compose.yml) | Completed |
| **Part 2**| Data drift monitoring & retraining lifecycle | [`src/monitoring/drift_detector.py`](file:///d:/College/mlops/src/monitoring/drift_detector.py), [`reports/`](file:///d:/College/mlops/reports) | Completed |

---
*Developed for the Data Engineering and MLOps Capstone Project.*
