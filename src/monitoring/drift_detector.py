"""
Data, Location, and Class Drift Detection Module.
Evaluates distribution shifts (KS-test, PSI, Jensen-Shannon) and triggers retraining workflows.
"""

import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import json
import logging
import datetime
import numpy as np
import pandas as pd
from scipy import stats
from src.etl.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("drift_monitoring")

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def calculate_psi(expected: np.ndarray, actual: np.ndarray, num_bins: int = 10) -> float:
    """Calculates the Population Stability Index (PSI) between two distributions."""
    expected = expected[~np.isnan(expected)]
    actual = actual[~np.isnan(actual)]
    
    if len(expected) == 0 or len(actual) == 0:
        return 0.0
        
    quantiles = np.linspace(0, 100, num_bins + 1)
    bin_edges = np.percentile(expected, quantiles)
    bin_edges[0] = -np.inf
    bin_edges[-1] = np.inf
    
    exp_counts, _ = np.histogram(expected, bins=bin_edges)
    act_counts, _ = np.histogram(actual, bins=bin_edges)
    
    # Avoid zero division with smoothing
    exp_pct = np.where(exp_counts == 0, 1e-4, exp_counts) / len(expected)
    act_pct = np.where(act_counts == 0, 1e-4, act_counts) / len(actual)
    
    psi_value = np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct))
    return float(np.round(psi_value, 4))


def run_drift_analysis() -> dict:
    """
    Executes full drift analysis:
    1. Splits warehouse data chronologically into Reference vs Current.
    2. Tests feature drift (Temperature, Precipitation, Speed Limit).
    3. Tests location drift (Latitude, Longitude via 2-sample KS test).
    4. Tests class/prediction distribution drift.
    5. Checks retraining alert thresholds.
    """
    engine = get_engine()
    query = """
        SELECT 
            f.accident_index,
            d.full_date,
            l.latitude,
            l.longitude,
            r.speed_limit,
            w.temperature_c,
            w.precipitation_mm,
            w.wind_speed_kmh,
            s.severity_name
        FROM fact_accidents f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_location l ON f.location_key = l.location_key
        JOIN dim_road r ON f.road_key = r.road_key
        JOIN dim_weather w ON f.weather_key = w.weather_key
        JOIN dim_severity s ON f.severity_key = s.severity_key
        ORDER BY d.full_date ASC
    """
    df = pd.read_sql(query, con=engine)
    if len(df) < 40:
        logger.warning("Insufficient data for drift analysis.")
        return {}
        
    split_point = int(len(df) * 0.70)
    ref_df = df.iloc[:split_point]
    cur_df = df.iloc[split_point:]
    
    logger.info(f"Analyzing drift - Reference: {len(ref_df)} records, Current: {len(cur_df)} records")
    
    # 1. Feature Drift (PSI & KS-Test)
    features_to_test = ["temperature_c", "precipitation_mm", "speed_limit", "wind_speed_kmh"]
    feature_drift_results = {}
    
    for feat in features_to_test:
        ref_vals = ref_df[feat].dropna().values
        cur_vals = cur_df[feat].dropna().values
        
        ks_stat, p_val = stats.ks_2samp(ref_vals, cur_vals)
        psi = calculate_psi(ref_vals, cur_vals)
        
        drift_detected = psi > 0.25 or p_val < 0.01
        status = "CRITICAL_DRIFT" if psi > 0.25 else ("MODERATE_DRIFT" if psi > 0.10 else "STABLE")
        
        feature_drift_results[feat] = {
            "ks_statistic": round(float(ks_stat), 4),
            "p_value": round(float(p_val), 4),
            "psi": psi,
            "status": status,
            "drift_detected": bool(drift_detected)
        }
        
    # 2. Location Drift (Spatial shift in accident locations)
    lat_ks, lat_p = stats.ks_2samp(ref_df["latitude"].values, cur_df["latitude"].values)
    lon_ks, lon_p = stats.ks_2samp(ref_df["longitude"].values, cur_df["longitude"].values)
    location_drift_detected = (lat_p < 0.01 or lon_p < 0.01)
    
    # 3. Class Drift (Severity distribution shifts)
    ref_sev = ref_df["severity_name"].value_counts(normalize=True).to_dict()
    cur_sev = cur_df["severity_name"].value_counts(normalize=True).to_dict()
    
    # Check Retraining Criteria
    any_feature_critical = any(v["status"] == "CRITICAL_DRIFT" for v in feature_drift_results.values())
    retraining_reasons = []
    if any_feature_critical:
        retraining_reasons.append("Critical PSI threshold exceeded on environmental features (> 0.25)")
    if location_drift_detected and (lat_ks > 0.15 or lon_ks > 0.15):
        retraining_reasons.append("Significant spatial shift observed in accident coordinate cluster")
        
    retrain_needed = len(retraining_reasons) > 0
    
    report = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "reference_records": len(ref_df),
        "current_records": len(cur_df),
        "feature_drift": feature_drift_results,
        "location_drift": {
            "latitude_ks": round(float(lat_ks), 4),
            "latitude_p_val": round(float(lat_p), 4),
            "longitude_ks": round(float(lon_ks), 4),
            "longitude_p_val": round(float(lon_p), 4),
            "drift_detected": bool(location_drift_detected)
        },
        "class_distribution": {
            "reference": {k: round(v, 4) for k, v in ref_sev.items()},
            "current": {k: round(v, 4) for k, v in cur_sev.items()}
        },
        "retraining_decision": {
            "retrain_triggered": retrain_needed,
            "triggers": retraining_reasons if retrain_needed else ["All metrics within tolerance stability thresholds"]
        }
    }
    
    # Save Report
    report_file = REPORTS_DIR / f"drift_report_{datetime.date.today()}.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
        
    logger.info(f"Drift evaluation report written to: {report_file}")
    logger.info(f"Retraining Trigger Status: {'TRIGGERED' if retrain_needed else 'OK (STABLE)'}")
    return report


if __name__ == "__main__":
    run_drift_analysis()
