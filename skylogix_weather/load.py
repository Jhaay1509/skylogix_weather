import os
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load(data_path: str) -> Optional[int]:
    """
    Idempotent load of flattened weather data into PostgreSQL.
    Deduplicates by (city, observed_at, provider).
    Returns inserted row count, 0 for empty input, or None on failure.
    """
    # 1) Validate input + read pickle
    if not isinstance(data_path, str) or not data_path.strip():
        logger.error("data_path must be a non-empty string")
        return None

    try:
        df = pd.read_pickle(data_path)
    except Exception as exc:
        logger.error("Could not read pickle at %s: %s", data_path, exc)
        return None

    if df.empty:
        logger.warning("No data to load: DataFrame is empty.")
        return 0

    logger.info("Loaded %d records from %s", len(df), data_path)

    # 2) Normalize types
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df = df.dropna(subset=["observed_at"])

    numeric_cols = [
        "lat", "lon", "temp_c", "humidity_pct",
        "wind_speed_ms", "rain_1h_mm", "snow_1h_mm",
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # 3) Deduplicate
    original = len(df)
    df = df.drop_duplicates(subset=["city", "observed_at", "provider"])
    logger.info("Deduplicated: %d → %d", original, len(df))

    # 4) Connect to Postgres
    conn_string: Optional[str] = os.getenv("POSTGRES_STRING")
    if not conn_string:
        logger.error("POSTGRES_STRING environment variable not set.")
        return None

    engine: Engine = create_engine(conn_string)

    # 5) Bulk insert (ignore duplicates)
    stmt = text(
        """
        INSERT INTO weather_readings (
            city, country, lat, lon, observed_at,
            temp_c, humidity_pct, wind_speed_ms, rain_1h_mm, snow_1h_mm,
            condition, description, provider
        )
        VALUES (
            :city, :country, :lat, :lon, :observed_at,
            :temp_c, :humidity_pct, :wind_speed_ms, :rain_1h_mm, :snow_1h_mm,
            :condition, :description, :provider
        )
        ON CONFLICT (city, observed_at, provider) DO NOTHING;
        """
    )

    rows = df.to_dict(orient="records")

    try:
        with engine.begin() as conn:
            result = conn.execute(stmt, rows)
            inserted = int(result.rowcount or 0)

        logger.info(
            "Load complete: inserted=%d, duplicates_ignored=%d",
            inserted,
            len(rows) - inserted,
        )
        return inserted

    except Exception as exc:
        logger.error("Failed to load into PostgreSQL: %s", exc)
        return None
