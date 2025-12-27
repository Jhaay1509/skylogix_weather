import requests
import os 
import logging
from typing import Dict, Any, Union
from datetime import datetime, timezone

from pymongo import MongoClient
from dotenv import load_dotenv

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables early
load_dotenv()

# --- 1. CONFIGURATION FUNCTION ---

def _get_config() -> Dict[str, Any]:
    """Retrieves and consolidates all configuration settings from environment variables."""
    
    # .env format: Nairobi,KE,Lagos,NG,...
    cities_raw = os.getenv(
        "CITIES",
        "Nairobi,KE,Lagos,NG,Accra,GH,Johannesburg,ZA"
    ).split(",")

    # ✅ FIX: keep only city names (even indexes)
    cities = [cities_raw[i] for i in range(0, len(cities_raw), 2)]

    return {
        # Database Settings
        "MONGO_URL": os.getenv("MONGO_URL"),
        "DB_NAME": "weather_raw",
        "COLLECTION_NAME": "weather_raw",

        # API Settings
        "BASE_URL": "https://api.openweathermap.org/data/2.5/weather",
        "API_KEY": os.getenv("API_KEY"),
        "API_UNITS": "metric",
        
        # Target Locations List
        "CITIES": cities
    }

# --- 2. CORE UTILITY FUNCTIONS ---

def get_mongodb_collection(config: Dict[str, Any]) -> Any | None:
    mongo_url = config.get("MONGO_URL")
    
    if not mongo_url:
        logging.error("MONGO_URL environment variable is not set.")
        return None
    
    try:
        client = MongoClient(mongo_url)
        db = client[config["DB_NAME"]]
        collection = db[config["COLLECTION_NAME"]]
        logging.info("Successfully connected to MongoDB.")
        return collection
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return None

def build_document_id(record: Dict[str, Any]) -> str:
    """Creates a stable, idempotent document ID."""
    return f"{record['name']}_{record['dt']}"

def upsert_weather(collection: Any, record: Dict[str, Any]):
    doc_id = build_document_id(record)

    collection.update_one(
        {"_id": doc_id},
        {
            # Insert raw payload ONLY if new
            "$setOnInsert": record,

            # Always update metadata
            "$set": {
                "updatedAt": datetime.now(timezone.utc)
            }
        },
        upsert=True
    )

    logging.info(f"Upserted weather data for {record['name']}")


# --- 3. MAIN INGESTION FUNCTION ---

def ingest_weather_data() -> None:
    config = _get_config()
    cities = config["CITIES"]

    logging.info(f"Starting ingestion for cities: {cities}")

    collection = get_mongodb_collection(config)
    api_key = config.get("API_KEY")

    if collection is None or not api_key:
        logging.error("Aborting ingestion due to missing MongoDB URL or API Key.")
        return
        
    with requests.Session() as session:
        for city in cities:
            logging.info(f"Fetching data for {city}")

            params = {
                "q": city,
                "appid": api_key,
                "units": config["API_UNITS"]
            }

            try:
                response = session.get(config["BASE_URL"], params=params)
                response.raise_for_status()

                record = response.json()
                upsert_weather(collection, record)

            except Exception as e:
                logging.error(f"Failed for {city}: {e}")

    logging.info("Weather data ingestion finished.")

# --- 4. SCRIPT EXECUTION ---
if __name__ == "__main__":
    ingest_weather_data()
