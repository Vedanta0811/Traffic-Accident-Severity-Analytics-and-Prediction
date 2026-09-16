import sys
import uuid
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.etl.db import get_engine, init_db


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("accident_ingest")


def ingest_accidents():
    batch_id = f"batch_acc_{uuid.uuid4().hex[:8]}"

    source_file = (
        Path(__file__).resolve().parents[2]
        / "datasets"
        / "dft-road-casualty-statistics-collision-last-5-years.csv"
    )

    raw_folder = Path("data/raw")
    raw_folder.mkdir(parents=True, exist_ok=True)

    start_time = datetime.now(timezone.utc)

    # Read actual collision dataset
    df = pd.read_csv(
    source_file,
    skiprows=2,
    names=[
        "collision_index",
        "collision_year",
        "collision_ref_no",
        "location_easting_osgr",
        "location_northing_osgr",
        "longitude",
        "latitude",
        "police_force",
        "collision_severity",
        "number_of_vehicles",
        "number_of_casualties",
        "date",
        "day_of_week",
        "time",
        "local_authority_district",
        "local_authority_ons_district",
        "local_authority_highway",
        "local_authority_highway_current",
        "first_road_class",
        "first_road_number",
        "road_type",
        "speed_limit",
        "junction_detail_historic",
        "junction_detail",
        "junction_control",
        "second_road_class",
        "second_road_number",
        "pedestrian_crossing_human_control_historic",
        "pedestrian_crossing_physical_facilities_historic",
        "pedestrian_crossing",
        "light_conditions",
        "weather_conditions",
        "road_surface_conditions",
        "special_conditions_at_site",
        "carriageway_hazards_historic",
        "carriageway_hazards",
        "urban_or_rural_area",
        "did_police_officer_attend_scene_of_accident",
        "trunk_road_flag",
        "lsoa_of_accident_location",
        "enhanced_severity_collision",
        "collision_injury_based",
        "collision_adjusted_severity_serious",
        "collision_adjusted_severity_slight",
    ],
    low_memory=False,
)

    # Select and rename required columns
    df = df[
        [
            "collision_index",
            "date",
            "time",
            "latitude",
            "longitude",
            "collision_severity",
            "number_of_vehicles",
            "number_of_casualties",
            "speed_limit",
            "road_type",
            "light_conditions",
            "weather_conditions",
            "road_surface_conditions",
            "urban_or_rural_area",
            "local_authority_district",
        ]
    ]

    df.columns = [
        "accident_index",
        "accident_date",
        "accident_time",
        "latitude",
        "longitude",
        "accident_severity",
        "number_of_vehicles",
        "number_of_casualties",
        "speed_limit",
        "road_type",
        "light_conditions",
        "weather_conditions",
        "road_surface_conditions",
        "urban_or_rural_area",
        "local_authority",
    ]

    df["batch_id"] = batch_id

    # Save raw copy
    raw_file = raw_folder / f"accidents_raw_{batch_id}.csv"
    df.to_csv(raw_file, index=False)

    # Initialize database
    engine = get_engine()
    init_db(engine)

    try:
        # Insert records into staging table
        df.to_sql(
            "stg_accidents_raw",
            con=engine,
            if_exists="append",
            index=False,
        )

        # Insert audit record
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO ingestion_audit
                    (
                        batch_id,
                        source_name,
                        file_or_endpoint,
                        status,
                        records_ingested,
                        records_rejected,
                        started_at,
                        completed_at
                    )
                    VALUES
                    (
                        :batch_id,
                        :source_name,
                        :file_path,
                        'SUCCESS',
                        :records,
                        0,
                        :started,
                        :completed
                    )
                    """
                ),
                {
                    "batch_id": batch_id,
                    "source_name": "UK DfT Collision Dataset",
                    "file_path": str(source_file),
                    "records": len(df),
                    "started": start_time,
                    "completed": datetime.now(timezone.utc),
                },
            )

        logger.info(f"Ingestion successful: {len(df)} records")
        logger.info(f"Batch ID: {batch_id}")

    except Exception:
        logger.exception("Accident ingestion failed")
        raise


if __name__ == "__main__":
    ingest_accidents()