# Execution Evidence & Pipeline Demonstration

This document provides verified terminal logs, test logs, and database evidence demonstrating successful end-to-end execution of the Traffic Accident Severity Analytics and MLOps Pipeline.

---

## 1. Automated Test Suite Execution (PyTest)
All 5 unit and integration tests passed with 100% compliance:

```text
============================= test session starts =============================
platform win32 -- Python 3.13.9, pytest-8.4.2, pluggy-1.6.0
rootdir: D:\College\mlops
collected 5 items

tests/test_pipeline.py::test_database_init PASSED                        [ 20%]
tests/test_pipeline.py::test_data_validation_and_quarantine PASSED       [ 40%]
tests/test_pipeline.py::test_model_pipeline_predict PASSED               [ 60%]
tests/test_pipeline.py::test_api_health_endpoint PASSED                  [ 80%]
tests/test_pipeline.py::test_api_prediction_endpoint PASSED              [100%]

======================= 5 passed in 13.06s =======================
```

---

## 2. Ingestion & Raw Landing Zone Evidence
```text
2026-09-11 16:55:23,229 - accident_ingest - INFO - Generating benchmark accident dataset (batch: batch_acc_35cc938b)...
2026-09-11 16:55:23,277 - accident_ingest - INFO - Saved raw landing file to: data\raw\accidents_raw_batch_acc_35cc938b.csv (2500 records)
2026-09-11 16:55:27,858 - db_manager - INFO - Applying schema: 01_staging_and_quarantine.sql
2026-09-11 16:55:27,892 - db_manager - INFO - Applying schema: 02_star_schema.sql
2026-09-11 16:55:27,937 - db_manager - INFO - Database schemas successfully initialized.
2026-09-11 16:55:27,993 - accident_ingest - INFO - Ingestion batch batch_acc_35cc938b logged to audit successfully.
```

---

## 3. Atmospheric Weather Ingestion (Open-Meteo API)
```text
2026-09-11 16:56:11,447 - weather_ingest - INFO - Saved raw weather data to: data\raw\weather_raw_batch_wea_3569976b.json (720 hourly readings)
2026-09-11 16:56:11,474 - weather_ingest - INFO - Weather batch batch_wea_3569976b successfully loaded and audited.
```

---

## 4. Data Quality Validation & Quarantine Execution
```text
2026-09-11 16:56:32,908 - data_validation - INFO - Starting quality checks on 2500 records...
2026-09-11 16:56:32,939 - data_validation - WARNING - Quarantined 37 invalid records out of 2500. Logged to quarantine_rejected_records.
2026-09-11 16:56:32,940 - etl_transform - INFO - Transforming 2463 validated accident records...
2026-09-11 16:56:34,127 - etl_transform - INFO - ETL transformation, star schema load, and analytical marts refresh complete!
```

---

## 5. MLOps Multi-Model Training & MLflow Tracking
```text
2026-09-11 16:57:51,960 - feature_engineering - INFO - Loaded 2463 records from warehouse for feature engineering.
2026-09-11 16:57:51,990 - feature_engineering - INFO - Chronological split - Train: 1724, Val: 369, Test: 370
2026-09-11 16:57:51,990 - model_training - INFO - Fitting preprocessor on chronological training split...
2026-09-11 16:57:52,020 - model_training - INFO - Calculated class weights: {0: 0.418, 1: 1.836, 2: 15.123}
2026-09-11 16:57:52,020 - model_training - INFO - --- Training RandomForest ---
2026-09-11 16:57:53,312 - model_training - INFO - RandomForest Results - Macro F1: 0.2973, Fatal Recall: 0.0000, ROC-AUC: 0.5232
2026-09-11 16:57:54,112 - model_training - INFO - --- Training LightGBM ---
2026-09-11 16:58:00,274 - model_training - INFO - LightGBM Results - Macro F1: 0.3138, Fatal Recall: 0.0000, ROC-AUC: 0.4725
2026-09-11 16:58:00,483 - model_training - INFO - --- Training XGBoost ---
2026-09-11 16:58:00,931 - model_training - INFO - XGBoost Results - Macro F1: 0.3175, Fatal Recall: 0.0000, ROC-AUC: 0.5019
2026-09-11 16:58:01,111 - model_training - INFO - === Best Model Selected: XGBoost with Val Macro-F1: 0.3175 ===
2026-09-11 16:58:26,858 - model_training - INFO - Saved complete inference pipeline to: models\accident_severity_model.joblib
2026-09-11 16:58:26,859 - model_training - INFO - Saved test evaluation report to: models\test_evaluation_report.json
```

---

## 6. Drift Monitoring & Retraining Execution
```text
2026-09-11 16:58:53,682 - drift_monitoring - INFO - Analyzing drift - Reference: 1724 records, Current: 739 records
2026-09-11 16:58:53,701 - drift_monitoring - INFO - Drift evaluation report written to: reports\drift_report_2026-09-11.json
2026-09-11 16:58:53,701 - drift_monitoring - INFO - Retraining Trigger Status: OK (STABLE)
```
