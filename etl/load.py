import sqlite3


DB_FILE = "hydrological.db"
STATION_ID = "E64999A"
STATION_NAME = "Hipper Park Road Bridge"


def load_to_database(df):
  
    # Load transformed dataframe into SQLite using star schema.
    if df.empty:
        return
    
    #  Format date_time to string for database insertion
    df["date_time"] = df["date_time"].dt.strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()

        # Create dimension table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_id TEXT PRIMARY KEY,
            station_name TEXT
        )
        """)

        cursor.execute("""
        INSERT OR REPLACE INTO stations (station_id, station_name)
        VALUES (?, ?)
        """, (STATION_ID, STATION_NAME))

        # Create fact table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            parameter TEXT,
            date_time TEXT,
            value REAL,
            UNIQUE(station_id, parameter, date_time),
            FOREIGN KEY(station_id) REFERENCES stations(station_id)
        )
        """)
        conn.commit()
        
        # Convert DataFrame to list of dicts for insertion
        records = df.to_dict(orient="records")
        
        # Using INSERT OR IGNORE to prevent duplicates based on the UNIQUE constraint
        cursor.executemany("""
        INSERT OR IGNORE INTO measurements
        (station_id, parameter, date_time, value)
        VALUES (:station_id, :parameter, :date_time, :value)
        """, records)
        
        conn.commit()