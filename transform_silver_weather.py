import os
import glob
import json
import logging
import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

# 1. Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 2. Paths Configuration
BRONZE_DIR = os.path.join("data", "bronze", "weather")
SILVER_DIR = os.path.join("data", "silver", "weather_delta")

def read_all_bronze_json() -> list:
    """Finds and extracts metrics from all raw JSON batch files in Bronze."""
    json_files = glob.glob(os.path.join(BRONZE_DIR, "raw_weather_*.json"))
    
    if not json_files:
        logger.warning("No raw files found in the Bronze layer.")
        return []
    
    parsed_records = []
    
    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
                
                # Extract nested data from Open-Meteo payload structure
                current_data = payload.get("current", {})
                
                # Flatten the data structure neatly into a single record dictionary
                record = {
                    "latitude": payload.get("latitude"),
                    "longitude": payload.get("longitude"),
                    "timestamp_sast": current_data.get("time"), # Raw ISO string format
                    "temperature_celsius": current_data.get("temperature_2m"),
                    "apparent_temperature_celsius": current_data.get("apparent_temperature"),
                    "relative_humidity_percentage": current_data.get("relative_humidity_2m"),
                    "rainfall_mm": current_data.get("rain"),
                    "wind_speed_kmh": current_data.get("wind_speed_10m"),
                    "ingested_at": payload.get("_ingested_at")
                }
                parsed_records.append(record)
        except Exception as e:
            logger.error(f"Failed parsing file {file_path}: {e}")
            continue
            
    return parsed_records

def clean_and_transform(raw_records: list) -> pd.DataFrame:
    """Applies structured data transformations and schema type-casting."""
    df = pd.DataFrame(raw_records)
    
    # Drop rows missing crucial timestamp markers or data keys
    df = df.dropna(subset=["timestamp_sast"])
    
    # Cast raw ISO weather strings into standardized Pandas datetime formats
    df["timestamp_sast"] = pd.to_datetime(df["timestamp_sast"])
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    
    # Explicitly enforce clean numeric casting to match our pipeline contracts
    float_cols = ["temperature_celsius", "apparent_temperature_celsius", 
                  "relative_humidity_percentage", "rainfall_mm", "wind_speed_kmh"]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        
    # Deduplicate rows cleanly based on weather timestamp log entries
    df = df.drop_duplicates(subset=["timestamp_sast"], keep="last")
    
    return df

def write_to_silver_delta(df: pd.DataFrame, target_path: str):
    """Writes the cleaned DataFrame to a performant Delta Lake table with Append mode."""
    if df.empty:
        logger.info("DataFrame is empty. Skipping Silver write.")
        return
        
    logger.info(f"Writing {len(df)} records into Silver Delta Lake format at: {target_path}")
    
    # Removed the deprecated engine="pyarrow" argument
    write_deltalake(
        target_path, 
        df, 
        mode="append"
    )
    logger.info("Successfully updated Silver Delta table schema transaction log.")

def main():
    logger.info("Commencing Silver Transformation Job...")
    
    raw_data = read_all_bronze_json()
    if not raw_data:
        return
        
    cleaned_df = clean_and_transform(raw_data)
    write_to_silver_delta(cleaned_df, SILVER_DIR)
    
    logger.info("Silver Layer Transformation completed successfully.")

if __name__ == "__main__":
    main()
