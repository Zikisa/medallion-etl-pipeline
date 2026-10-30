import os
import json
import logging
from datetime import datetime
import requests
import pandas as pd

# 1. Setup Structured Logging (Crucial for production tracking)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 2. Define Configuration Constants
CAPE_TOWN_LAT = -33.9249
CAPE_TOWN_LON = 18.4241
BRONZE_DIR = os.path.join("data", "bronze", "weather")

def ensure_directory_exists(directory_path: str):
    """Creates the target directory if it does not exist."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info(f"Created landing directory: {directory_path}")


def fetch_cape_town_weather() -> dict:
    """Fetches real-time weather metrics for Cape Town via Open-Meteo API."""

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": CAPE_TOWN_LAT,
        "longitude": CAPE_TOWN_LON,
        "current": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "apparent_temperature,"
            "rain,"
            "wind_speed_10m"
        ),
        "timezone": "Africa/Johannesburg"
    }

    logger.info("Initiating API request to Open-Meteo...")

    try:
        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        # Check whether the API returned an HTTP error
        response.raise_for_status()

        # Convert the API response into JSON
        try:
            weather_data = response.json()
        except ValueError:
            logger.error(
                f"Invalid JSON response: {response.text[:500]}"
            )
            raise

        logger.info("Successfully retrieved weather data.")

        return weather_data

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise

    
    logger.info("Initiating API request to Open-Meteo...")
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise

def save_to_bronze(raw_data: dict, target_dir: str):
    """Saves the raw JSON data along with an ingestion timestamp footprint."""
    ensure_directory_exists(target_dir)
    
    # Generate unique filenames using precise timestamps to prevent overwrites
    execution_time = datetime.now()
    timestamp_str = execution_time.strftime("%Y%m%d_%H%M%S")
    
    # Add metadata footprint to the raw data (Standard Data Platform practice)
    raw_data["_ingested_at"] = execution_time.isoformat()
    raw_data["_source_system"] = "open_meteo_api"
    
    filename = f"raw_weather_{timestamp_str}.json"
    file_path = os.path.join(target_dir, filename)
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=4)
        
    logger.info(f"Successfully landed raw batch data to Bronze: {file_path}")

def main():
    logger.info("Starting Bronze Ingestion Job...")
    try:
        raw_weather_payload = fetch_cape_town_weather()
        save_to_bronze(raw_weather_payload, BRONZE_DIR)
        logger.info("Bronze Ingestion Job completed successfully.")
    except Exception as e:
        logger.critical(f"Ingestion pipeline failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
