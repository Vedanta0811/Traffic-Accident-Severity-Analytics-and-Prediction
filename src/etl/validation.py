"""
Data Quality Validation and Quarantine Pipeline.
Enforces business rules, filters invalid/duplicate records, and logs rejected rows to quarantine.
"""

import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import json
import logging
import datetime
import pandas as pd
from sqlalchemy import text
from src.etl.db import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("data_validation")


def validate_and_quarantine(batch_id: str = None) -> dict:
    """
    Validates records in stg_accidents_raw.
    Routes non-compliant records to quarantine_rejected_records table and returns valid DataFrame.
    """
    engine = get_engine()
    
    # Query records from staging
    if batch_id:
        query = f"SELECT * FROM stg_accidents_raw WHERE batch_id = '{batch_id}'"
    else:
        query = "SELECT * FROM stg_accidents_raw"
        
    df = pd.read_sql(query, con=engine)
    if df.empty:
        logger.warning("No records found in staging to validate.")
        return {"total": 0, "valid": 0, "rejected": 0}
        
    total_records = len(df)
    logger.info(f"Starting quality checks on {total_records} records...")
    
    valid_mask = pd.Series(True, index=df.index)
    rejection_reasons = {}
    
    # Check 1: Valid Coordinates
    coord_invalid = (
        df["latitude"].isna() | 
        df["longitude"].isna() | 
        (df["latitude"] < 49.0) | (df["latitude"] > 61.5) | 
        (df["longitude"] < -8.5) | (df["longitude"] > 2.5)
    )
    for idx in df[coord_invalid].index:
        valid_mask[idx] = False
        rejection_reasons[idx] = "INVALID_COORDINATES: Lat/Lon out of plausible geographic bounds"
        
    # Check 2: Missing or malformed Date/Time
    date_invalid = df["accident_date"].isna() | (df["accident_date"].str.len() < 8)
    for idx in df[date_invalid].index:
        if idx not in rejection_reasons:
            valid_mask[idx] = False
            rejection_reasons[idx] = "MISSING_DATETIME: Accident date is null or invalid format"
            
    # Check 3: Valid Speed Limit
    speed_invalid = df["speed_limit"].isna() | (df["speed_limit"] <= 0) | (df["speed_limit"] > 100)
    for idx in df[speed_invalid].index:
        if idx not in rejection_reasons:
            valid_mask[idx] = False
            rejection_reasons[idx] = "INVALID_SPEED_LIMIT: Speed limit not in range [10, 100]"
            
    # Check 4: Valid Severity
    sev_invalid = df["accident_severity"].isna() | (~df["accident_severity"].astype(str).isin(["1", "2", "3", "Fatal", "Serious", "Slight"]))
    for idx in df[sev_invalid].index:
        if idx not in rejection_reasons:
            valid_mask[idx] = False
            rejection_reasons[idx] = "INVALID_SEVERITY: Severity not mapped to 1, 2, or 3"
            
    # Check 5: Duplicate Accident Index
    dup_mask = df.duplicated(subset=["accident_index"], keep="first")
    for idx in df[dup_mask].index:
        if idx not in rejection_reasons:
            valid_mask[idx] = False
            rejection_reasons[idx] = "DUPLICATE_ACCIDENT_INDEX: Duplicate key collision"
            
    valid_df = df[valid_mask].copy()
    rejected_df = df[~valid_mask].copy()
    
    # Store rejected records into quarantine table
    if not rejected_df.empty:
        rejected_records_to_insert = []
        for idx, row in rejected_df.iterrows():
            reason = rejection_reasons.get(idx, "FAILED_GENERAL_VALIDATION")
            raw_payload = row.to_json()
            rejected_records_to_insert.append({
                "batch_id": row.get("batch_id", "UNKNOWN"),
                "source_entity": "stg_accidents_raw",
                "record_identifier": str(row.get("accident_index", f"ROW_{idx}")),
                "rejection_reason": reason,
                "raw_payload": raw_payload,
                "rejected_at": datetime.datetime.now(datetime.timezone.utc)
            })
            
        with engine.begin() as conn:
            for rec in rejected_records_to_insert:
                conn.execute(
                    text("""
                        INSERT INTO quarantine_rejected_records 
                        (batch_id, source_entity, record_identifier, rejection_reason, raw_payload, rejected_at)
                        VALUES (:batch_id, :source_entity, :record_identifier, :rejection_reason, :raw_payload, :rejected_at)
                    """),
                    rec
                )
        
        # Save rejection summary artifact in data/rejected/
        rejected_dir = Path("data/rejected")
        rejected_dir.mkdir(parents=True, exist_ok=True)
        summary_path = rejected_dir / f"quarantine_summary_{datetime.date.today()}.json"
        
        summary_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "total_evaluated": total_records,
            "passed_validation": len(valid_df),
            "quarantined_count": len(rejected_df),
            "reasons_breakdown": {
                reason: sum(1 for r in rejection_reasons.values() if r == reason)
                for reason in set(rejection_reasons.values())
            }
        }
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary_data, f, indent=2)
            
        logger.warning(f"Quarantined {len(rejected_df)} invalid records out of {total_records}. Logged to quarantine_rejected_records.")
    else:
        logger.info("All records passed data quality checks!")
        
    return {
        "total": total_records,
        "valid": len(valid_df),
        "rejected": len(rejected_df),
        "valid_df": valid_df
    }


if __name__ == "__main__":
    validate_and_quarantine()
