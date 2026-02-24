import logging
import os
import pandas as pd

from etl.extract import station_measures, recent_readings
from etl.transform import transform_data
from etl.load import load

STATION_ID = "E64999A"
STATION_NAME = "Hipper Park Road Bridge"
DB_FILE = "hydrological.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def run_pipeline():
    logging.info("Pipeline started.")

    measures = station_measures(STATION_ID)

    selected = [
        m for m in measures
        if m.get("parameter") in ["DISSOLVED OXYGEN", "CONDUCTIVITY"]
    ]

    rows = []

    for measure in selected:
        readings = recent_readings(measure.get("@id"), limit=10)

        for r in readings:
            rows.append({
                "station_id": STATION_ID,
                "parameter": measure.get("parameter"),
                "date_time": r.get("dateTime"),
                "value": r.get("value")
            })

    if not rows:
        logging.warning("No data returned.")
        return

    df = pd.DataFrame(rows)

    os.makedirs("data/raw", exist_ok=True)
    df.to_csv("data/raw/raw_data.csv", index=False)

    df = transform_data(df)

    os.makedirs("data/cleaned_data", exist_ok=True)
    df.to_csv("data/cleaned_data/processed_data.csv", index=False)

    load(df, DB_FILE, STATION_ID, STATION_NAME)

    logging.info("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()