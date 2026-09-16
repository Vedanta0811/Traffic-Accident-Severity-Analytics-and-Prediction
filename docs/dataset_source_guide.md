# Dataset Source Information & Access Instructions

## 1. Overview of Data Sources
This project integrates two primary public and legally accessible data sources to establish a time- and location-aware traffic accident analytical database and predictive machine learning pipeline.

---

## 2. Primary Dataset: UK Road Safety Open Data
- **Entity**: UK Department for Transport (DfT)
- **Licensing**: Open Government Licence (OGL v3.0) — Free, institutional, and publicly accessible for commercial and non-commercial research.
- **Access URL**: [https://www.data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data](https://www.data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data)
- **Direct Download & API Endpoint**: Available through DfT statistics data downloads and historical archives.
- **Attributes Included**:
  - `accident_index`: Unique accident reference string.
  - `latitude` / `longitude`: Decimal WGS84 coordinates.
  - `accident_severity`: 1 (Fatal), 2 (Serious), 3 (Slight).
  - `number_of_vehicles` / `number_of_casualties`: Integer counts.
  - `speed_limit`: Posted speed limit (MPH).
  - `road_type`: Single carriageway, Dual carriageway, Roundabout, One-way street, Slip road.
  - `light_conditions`: Daylight, Darkness (lit, unlit, no lighting).
  - `road_surface_conditions`: Dry, Wet/Damp, Frost/Ice, Snow, Flood.
  - `urban_or_rural_area`: Geographic classification.
  - `local_authority`: Municipal district.

---

## 3. Atmospheric Enrichment: Open-Meteo Weather API
- **Entity**: Open-Meteo Open-Source Weather API
- **Licensing**: Open Database License (ODbL) / Non-commercial and Open Data Creative Commons Attribution 4.0.
- **Access Protocol**: Public REST API (No API key required).
- **Endpoint Structure**:
  - Historical Archive API: `https://archive-api.open-meteo.com/v1/archive`
  - Current / Forecast API: `https://api.open-meteo.com/v1/forecast`
- **Request Parameters**:
  - `latitude`: Float coordinate
  - `longitude`: Float coordinate
  - `start_date`: YYYY-MM-DD
  - `end_date`: YYYY-MM-DD
  - `hourly`: `temperature_2m`, `precipitation`, `wind_speed_10m`, `visibility`, `weather_code`
- **Response Format**: JSON hourly arrays.

---

## 4. Automated Acquisition & Raw Landing Zone Compliance
- **Raw Landing Zone**:
  - Incoming data is stored directly in `data/raw/` in immutable form (`accidents_raw_{batch_id}.csv` and `weather_raw_{batch_id}.json`).
  - No records in the raw landing zone are modified or overwritten.
- **Ingestion Script**:
  - Script path: `src/ingestion/accident_ingest.py` and `src/ingestion/weather_ingest.py`.
  - Execution command: `python src/ingestion/accident_ingest.py`
  - Audit logging: Execution status, record count, timestamps, and error traces are recorded in the `ingestion_audit` relational database table.
