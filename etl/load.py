import sqlite3
import logging


def load(df, db_file, station_id, station_name):
    # Convert datetime to string for SQLite TEXT storage
    df["date_time"] = df["date_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_id TEXT PRIMARY KEY,
            station_name TEXT
        )
        """)

        cursor.execute("""
        INSERT OR REPLACE INTO stations (station_id, station_name)
        VALUES (?, ?)
        """, (station_id, station_name))

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

        records = df.to_dict(orient="records")

        cursor.executemany("""
        INSERT OR IGNORE INTO measurements
        (station_id, parameter, date_time, value)
        VALUES (:station_id, :parameter, :date_time, :value)
        """, records)

        conn.commit()

    logging.info(f"{len(df)} rows loaded into database.")