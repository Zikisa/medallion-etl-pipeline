import os
import logging
import duckdb

# 1. Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 2. Paths Configuration
SILVER_DELTA_DIR = os.path.join("data", "silver", "weather_delta")
GOLD_DIR = os.path.join("data", "gold", "daily_weather_summary")

def ensure_directory_exists(directory_path: str):
    """Creates the target directory if it does not exist."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info(f"Created Gold analytical layer directory: {directory_path}")

def run_gold_aggregations():
    """Uses DuckDB to run analytics over Silver Delta Lake data and output Gold aggregates."""
    ensure_directory_exists(GOLD_DIR)
    
    logger.info("Initializing DuckDB connection and loading Delta extensions...")
    # Open an in-memory DuckDB session
    conn = duckdb.connect(database=':memory:')
    
    # Install and load the spatial/parquet extensions required to parse delta tables natively
    conn.execute("INSTALL delta; LOAD delta;")
    
    logger.info(f"Querying Silver Delta files from: {SILVER_DELTA_DIR}")
    
    # This SQL query reads directly from the Delta Lake transaction log/parquet files,
    # aggregates the hourly/point-in-time entries into daily business metrics,
    # and generates key indicators like a high-heat flag or rainfall warnings.
    gold_query = f"""
        SELECT 
            CAST(timestamp_sast AS DATE) AS reporting_date,
            ROUND(AVG(temperature_celsius), 2) AS avg_temp_c,
            MAX(temperature_celsius) AS max_temp_c,
            MIN(temperature_celsius) AS min_temp_c,
            ROUND(SUM(rainfall_mm), 2) AS total_rainfall_mm,
            ROUND(AVG(relative_humidity_percentage), 2) AS avg_humidity_pct,
            ROUND(MAX(wind_speed_kmh), 2) AS max_wind_speed_kmh,
            -- Business logic flag: Highlighting Cape Town's intense summer/winter shifts
            CASE 
                WHEN MAX(temperature_celsius) >= 30.0 THEN 'High Heat Risk'
                WHEN SUM(rainfall_mm) >= 15.0 THEN 'Heavy Rain Warning'
                ELSE 'Normal Conditions'
            END AS weather_condition_alert,
            COUNT(*) AS total_hourly_records_processed,
            NOW() AS gold_processed_at
        FROM delta_scan('{SILVER_DELTA_DIR}')
        GROUP BY CAST(timestamp_sast AS DATE)
        ORDER BY reporting_date DESC
    """
    
    try:
        # Define output destination file path
        output_file = os.path.join(GOLD_DIR, "daily_weather_summary.parquet")
        
        logger.info("Executing analytical SQL transformations...")
        # DuckDB lets you write the output of a query directly into a compressed parquet file
        conn.execute(f"COPY ({gold_query}) TO '{output_file}' (FORMAT PARQUET);")
        
        logger.info(f"Successfully compiled and landed Gold analytics table at: {output_file}")
        
        # Quick validation check: Print the generated Gold data directly to the terminal
        logger.info("--- PREVIEWING GENERATED GOLD DATA ---")
        preview = conn.execute(f"SELECT * FROM parquet_scan('{output_file}') LIMIT 5;").fetchdf()
        print(preview.to_string(index=False))
        
    except Exception as e:
        logger.error(f"Gold aggregation layer failed: {e}")
        raise
    finally:
        conn.close()

def main():
    logger.info("Commencing Gold Aggregation Layer Job...")
    run_gold_aggregations()
    logger.info("Gold Layer Aggregation completed successfully.")

if __name__ == "__main__":
    main()
