# Data Dictionary & Data Validation Rules

## 1. Relational & Dimensional Schema Dictionary

### Table: `dim_date`
Stores calendar and time-of-day dimensional attributes for time-aware analytical slices.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `date_key` | INT | PRIMARY KEY | Unique integer key formatted as `YYYYMMDDHH` |
| `full_date` | DATE | NOT NULL | Calendar date (YYYY-MM-DD) |
| `year` | INT | NOT NULL | Year integer (e.g., 2024) |
| `quarter` | INT | NOT NULL | Calendar quarter (1-4) |
| `month` | INT | NOT NULL | Month integer (1-12) |
| `month_name` | VARCHAR(20) | NOT NULL | Name of month (January - December) |
| `day_of_month` | INT | NOT NULL | Day index within month (1-31) |
| `day_of_week` | INT | NOT NULL | ISO weekday index (0=Monday, 6=Sunday) |
| `day_name` | VARCHAR(20) | NOT NULL | Full day name (Monday - Sunday) |
| `is_weekend` | BOOLEAN | NOT NULL | Flag (1 if Saturday or Sunday, else 0) |
| `hour` | INT | NOT NULL | Hour of day (0-23) |
| `time_of_day` | VARCHAR(30) | NOT NULL | Categorical bucket: `Morning Rush`, `Midday`, `Evening Rush`, `Night` |

---

### Table: `dim_location`
Geospatial and jurisdictional attributes for accident occurrence sites.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `location_key` | INT | PRIMARY KEY | Surrogate integer key |
| `latitude` | NUMERIC(10,6)| NOT NULL | Decimal latitude coordinate (WGS84) |
| `longitude` | NUMERIC(10,6)| NOT NULL | Decimal longitude coordinate (WGS84) |
| `urban_or_rural`| VARCHAR(32)| NULL | Urban or Rural area classification |
| `local_authority`| VARCHAR(128)| NULL | Municipal governing district name |
| `region` | VARCHAR(64) | NULL | Regional administrative grouping |

---

### Table: `dim_road`
Road network features, speed regulation, and physical conditions.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `road_key` | INT | PRIMARY KEY | Surrogate integer key |
| `road_type` | VARCHAR(64) | NOT NULL | Single carriageway, Dual carriageway, Roundabout, Slip road |
| `speed_limit` | INT | NOT NULL | Posted legal speed limit in MPH (20, 30, 40, 50, 60, 70) |
| `light_conditions`| VARCHAR(128)| NOT NULL | Illumination state (Daylight, Dark lit, Dark unlit, etc.) |
| `road_surface_conditions`| VARCHAR(128)| NOT NULL | Surface condition (Dry, Wet/Damp, Frost/Ice, Snow, Flood) |

---

### Table: `dim_weather`
Atmospheric variables captured from Open-Meteo API.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `weather_key` | INT | PRIMARY KEY | Surrogate integer key |
| `weather_condition`| VARCHAR(128)| NOT NULL | Descriptive condition (Fine, Raining, Fog, Snowing, etc.) |
| `temperature_c` | NUMERIC(5,2)| NULL | Ambient 2-meter air temperature (°C) |
| `precipitation_mm`| NUMERIC(5,2)| NULL | Hourly liquid precipitation equivalent (mm) |
| `visibility_m` | NUMERIC(8,2)| NULL | Horizontal visibility in meters |
| `wind_speed_kmh` | NUMERIC(5,2)| NULL | 10-meter wind speed in km/h |
| `weather_risk_level`| VARCHAR(32)| NOT NULL | Composite classification: `Low`, `Moderate`, `Severe` |

---

### Table: `dim_severity`
Accident severity categories mapped to official DfT definitions.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `severity_key` | INT | PRIMARY KEY | Severity integer key (1, 2, 3) |
| `severity_code` | INT | NOT NULL | 1: Fatal, 2: Serious, 3: Slight |
| `severity_name` | VARCHAR(32) | NOT NULL | Nominal label: 'Fatal', 'Serious', 'Slight' |
| `severity_description`| TEXT | NULL | Formal definition of injury outcome |

---

### Table: `fact_accidents`
Central transaction fact recording each collision event with dimensional foreign keys.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `accident_key` | INT | PRIMARY KEY | Surrogate fact table primary key |
| `accident_index` | VARCHAR(64) | UNIQUE, NOT NULL | Natural business identifier of accident |
| `date_key` | INT | REFERENCES dim_date | Foreign key to `dim_date` |
| `location_key` | INT | REFERENCES dim_location | Foreign key to `dim_location` |
| `road_key` | INT | REFERENCES dim_road | Foreign key to `dim_road` |
| `weather_key` | INT | REFERENCES dim_weather | Foreign key to `dim_weather` |
| `severity_key` | INT | REFERENCES dim_severity | Foreign key to `dim_severity` |
| `number_of_vehicles`| INT | DEFAULT 1 | Count of motorized vehicles involved |
| `number_of_casualties`| INT | DEFAULT 1 | Count of persons injured or killed |
| `created_at` | TIMESTAMP | DEFAULT NOW() | Record creation timestamp |

---

## 2. Ingestion Audit & Quarantine Error Logging

### Table: `ingestion_audit`
| Column | Type | Description |
| :--- | :--- | :--- |
| `batch_id` | VARCHAR(64) | Unique execution identifier |
| `source_name` | VARCHAR(128) | Source entity (UK Road Safety / Open-Meteo) |
| `file_or_endpoint` | VARCHAR(256) | File location or API URL |
| `status` | VARCHAR(32) | 'SUCCESS', 'FAILED', or 'PARTIAL' |
| `records_ingested` | INT | Valid records loaded to warehouse |
| `records_rejected` | INT | Corrupted records quarantined |
| `started_at` / `completed_at` | TIMESTAMP | Ingestion run timestamps |
| `error_message` | TEXT | Detailed exception traceback if failure occurred |

### Table: `quarantine_rejected_records`
| Column | Type | Description |
| :--- | :--- | :--- |
| `rejection_id` | INT | Auto-incrementing primary key |
| `batch_id` | VARCHAR(64) | Originating batch ID |
| `source_entity` | VARCHAR(64) | Staging table name |
| `record_identifier`| VARCHAR(128) | Accident index or primary key of offending row |
| `rejection_reason` | VARCHAR(256) | Reason code why record failed quality gate |
| `raw_payload` | TEXT | Complete raw JSON snapshot of the rejected record |
| `rejected_at` | TIMESTAMP | Timestamp of quarantine event |

---

## 3. Data Quality Gate & Validation Rules

| Rule ID | Rule Name | Target Column | Validation Condition | Quarantine Action |
| :--- | :--- | :--- | :--- | :--- |
| **VR-01** | Geographic Coordinates | `latitude`, `longitude` | `49.0 <= latitude <= 61.5` AND `-8.5 <= longitude <= 2.5` | Route to quarantine (`INVALID_COORDINATES`) |
| **VR-02** | Valid Speed Limit | `speed_limit` | `10 <= speed_limit <= 100` | Route to quarantine (`INVALID_SPEED_LIMIT`) |
| **VR-03** | Date/Time Integrity | `accident_date`, `accident_time` | Must not be NULL, string length >= 8, valid date parse | Route to quarantine (`MISSING_DATETIME`) |
| **VR-04** | Valid Severity Domain | `accident_severity` | Value must belong to `{1, 2, 3, 'Fatal', 'Serious', 'Slight'}` | Route to quarantine (`INVALID_SEVERITY`) |
| **VR-05** | Primary Key Uniqueness | `accident_index` | Must be unique across all incoming batches | Route duplicate to quarantine (`DUPLICATE_ACCIDENT_INDEX`) |
