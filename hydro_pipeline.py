import requests
import sqlite3
import pandas as pd
import logging
import os

# Logging Configuration 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# Constants
BASE_URL = "https://environment.data.gov.uk/hydrology"
STATION_ID = "E64999A"
STATION_NAME = "Hipper Park Road Bridge"
TARGET_MEASURES = {
    "DISSOLVED OXYGEN": "mg/l",
    "CONDUCTIVITY": "uS/cm"
}
DB_FILE = "hydrological.db"

def station_measures(station_id):
    # Retrieve all measurement types for a station.
    url = f"{BASE_URL}/id/measures"
    params = {"station": station_id}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        logging.info("Successfully fetched station measures.")
        return response.json().get("items", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching measures: {e}")
        return []


def recent_readings(measure_url, limit=10):
    # Fetch latest readings for a measure.
    url = f"{measure_url}/readings.json"
    params = {"_limit": limit, "_sort": "-dateTime"}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        logging.info(f"Fetched recent readings from {measure_url}.")
        return response.json().get("items", [])
    except requests.RequestException as e:
        logging.error(f"Error fetching readings: {e}")
        return []

def run_pipeline():
    # Main ETL pipeline.
    
    logging.info("Pipeline started.")

    # Extraction
    all_measures = station_measures(STATION_ID)

    selected_measures = [
        m for m in all_measures
        if (m.get("parameter") == "DISSOLVED OXYGEN" and (m.get("unitName") or "").lower() == "mg/l")
        or m.get("parameter") == "CONDUCTIVITY"
    ]

    if not selected_measures:
        logging.warning("No matching measures found.")
        return

    rows = []
    
    for measure in selected_measures:
        parameter_name = measure.get("parameter")
        readings = recent_readings(measure.get("@id"), limit=10)

        if readings:
            logging.info(f"{len(readings)} readings extracted for {parameter_name}.")
        else:
            logging.warning(f"No readings returned for {parameter_name}.")
        
        for r in readings:
            rows.append({
                "station_id": STATION_ID,
                "parameter": parameter_name,
                "date_time": r.get("dateTime"),
                "value": r.get("value")
            })

    if not rows:
        logging.warning("No data returned from API.")
        return
    
    # Convert to DataFrame
    
    df = pd.DataFrame(rows)
    
    # Save raw extracted data
    os.makedirs("../data/raw", exist_ok=True)
    raw_df = pd.DataFrame(rows)
    raw_df.to_csv("../data/raw/raw_data.csv", index=False)
    logging.info("Raw data saved to ../data/raw/raw_data.csv")
    
    # Transformation
    # Remove duplicates based on station_id, parameter, and date_time, keeping the last occurrence
    df = df.drop_duplicates(subset=["station_id", "parameter", "date_time"], keep="last")
    
    # Convert all column names to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_', regex=True)
    
    # Convert all string columns to lowercase and replace spaces with underscores
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.replace(' ', '_', regex=True)
        df[col] = df[col].str.lower()

    
    # Convert dateTime to proper datetime type
    df["date_time"] = pd.to_datetime(df["date_time"], errors='coerce')
    
    # Remove rows with missing values
    df = df.dropna(subset=["value", "date_time"])
    
    # To ensure value is float
    df["value"] = df["value"].astype(float)

    # Keep only the latest 10 rows per parameter
    df = (
        df.sort_values(["parameter", "date_time"], ascending=[True, False])
          .groupby("parameter")
          .head(10)
          .reset_index(drop=True)
    )

    logging.info("Data transformation completed successfully.")
    
    # Save transformed data
    os.makedirs("../data/cleaned_data", exist_ok=True)
    df.to_csv("../data/cleaned_data/processed_data.csv", index=False)
    logging.info("Processed data saved to ../data/cleaned_data/processed_data.csv")
    
    # Loading
    # Format date_time to string for database insertion
    df["date_time"] = df["date_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Load into SQLite database using star schema
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()

        # Create dimension table for stations
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_id TEXT PRIMARY KEY,
            station_name TEXT
        )
        """)

        # Insert station info
        cursor.execute("""
        INSERT OR REPLACE INTO stations (station_id, station_name)
        VALUES (?, ?)
        """, (STATION_ID, STATION_NAME))

        # Create fact table for measurements
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY,
            station_id TEXT,
            parameter TEXT,
            date_time TEXT,
            value REAL,
            UNIQUE(station_id, parameter, date_time),
            FOREIGN KEY(station_id) REFERENCES stations(station_id)
        )
        """)
    
        # Convert DataFrame to list of dicts for insertion
        records = df.to_dict(orient="records")
        
        # Using INSERT OR IGNORE to prevent duplicates based on the UNIQUE constraint
        cursor.executemany("""
        INSERT OR IGNORE INTO measurements
        (station_id, parameter, date_time, value)
        VALUES (:station_id, :parameter, :date_time, :value)
        """, records)

        conn.commit()

    logging.info(f"Pipeline completed successfully. {len(df)} rows processed.")


if __name__ == "__main__":
    run_pipeline()