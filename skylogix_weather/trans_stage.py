import logging
import os
from pathlib import Path

import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def transform() -> str:
    """
    Read raw OpenWeather docs from MongoDB, flatten into a tabular shape,
    write a pickle file, and return the output path.
    """
    # 1) Config (same values, but env-driven to avoid hardcoding)
    uri = os.getenv(
        "MONGO_URI",
        "mongodb://manny-hub:$hello_Yello@127.0.0.1:27017/weather_raw?authSource=admin",
    )
    db_name = os.getenv("MONGO_DB", "weather_raw")
    coll_name = os.getenv("MONGO_COLLECTION", "weather_raw")
    output_path = os.getenv("TRANSFORM_OUTPUT", "/tmp/weather_pipeline/weather_flattened.pkl")

    if not uri or not db_name or not coll_name:
        raise ValueError("Missing MongoDB config (MONGO_URI/MONGO_DB/MONGO_COLLECTION)")

    # 2) Extract from MongoDB
    with MongoClient(uri) as client:
        collection = client[db_name][coll_name]
        raw_docs = list(collection.find({}))

    if not raw_docs:
        logger.warning("MongoDB collection is empty. No data to transform.")
        return ""

    logger.info("Fetched %d raw documents", len(raw_docs))

    # 3) Transform (flatten)
    rows = []
    for doc in raw_docs:
        main = doc.get("main") or {}
        wind = doc.get("wind") or {}
        weather_list = doc.get("weather") or []
        weather = weather_list[0] if weather_list else {}

        observed_at = (
            pd.to_datetime(doc.get("dt"), unit="s", utc=True)
            if doc.get("dt")
            else None
        )

        rows.append(
            {
                "city": doc.get("name"),
                "country": (doc.get("sys") or {}).get("country"),
                "lat": (doc.get("coord") or {}).get("lat"),
                "lon": (doc.get("coord") or {}).get("lon"),
                "observed_at": observed_at,
                "temp_c": main.get("temp"),
                "humidity_pct": main.get("humidity"),
                "wind_speed_ms": wind.get("speed"),
                "rain_1h_mm": (doc.get("rain") or {}).get("1h", 0.0),
                "snow_1h_mm": (doc.get("snow") or {}).get("1h", 0.0),
                "condition": weather.get("main"),
                "description": weather.get("description"),
                "provider": "openweather",
            }
        )

    # 4) Save to pickle
    df = pd.DataFrame(rows)
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(out_path)

    logger.info("Saved %d rows to %s", len(df), out_path)
    return str(out_path)


if __name__ == "__main__":
    transform()
