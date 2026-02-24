# Hydrological Data ETL Pipeline

## Overview
This project implements a **modular ETL pipeline** that extracts hydrological measurement data from the UK Environment Agency Hydrology Data Explorer API (public dataset), transforms it into a clean analytical format, and loads it into a **SQLite database** structured using a simple **star schema**.

The pipeline retrieves the 10 most recent readings for:

- **Dissolved Oxygen (mg/l)**
- **Conductivity (µS/cm)**

**Station:** Hipper Park Road Bridge (`E64999A`)

---

## Architecture

### ETL Approach
- **Extract:** Retrieve measurement metadata and recent readings via REST API  
- **Transform:** Clean, deduplicate, and standardise data using `pandas`  
- **Load:** Store structured data in SQLite following a star schema design  

**Reasoning:**  
An ETL approach ensures data quality and validation occur before persistence, reducing the risk of storing inconsistent or malformed data.

### Database Design (Star Schema)

**Dimension Table:** `stations`

| Column       | Type | Description         |
|--------------|------|-------------------|
| station_id   | PK   | Unique station ID |
| station_name | Text | Station name      |

**Fact Table:** `measurements`

| Column      | Type | Description |
|-------------|------|------------|
| id          | PK   | Unique measurement ID |
| station_id  | FK   | Links to `stations` |
| parameter   | Text | Measurement type |
| date_time   | Text | Timestamp of measurement |
| value       | Real | Measurement value |

**Note on `date_time` column:**  
In the `measurements` fact table, the `date_time` column is stored as `TEXT` to preserve the exact timestamp format returned by the Hydrological Data Explorer API.  
This approach avoids potential issues with automatic type conversions in SQLite, ensures compatibility across platforms, and simplifies enforcing the idempotent uniqueness constraint `(station_id, parameter, date_time)`.

---

## Project Structure

hydrological-etl-pipeline/
├─ etl/
│ ├─ extract.py
│ ├─ transform.py
│ └─ load.py
├─ data/
│ ├─ raw/
│ └─ cleaned_data/
├─ tests/
├─ hydro_pipeline.py # Orchestrates the ETL process
├─ Hydrological Architecture.png
├─ hydrological.db
├─ README.md
├─ requirements.txt
└─ .gitignore


---

## Requirements

- **Python 3.10+**
- `pandas`
- `requests`
- `pytest` (for running tests)

**Tested on:** Windows 11, Python 3.10

All dependencies are listed in `requirements.txt`.

---

## Setup Instructions (Windows)

1. **Clone the repository**

        git clone <repo_url>
        cd hydrological-etl-pipeline

2. **Create a virtual environment (optional but recommended)**

        python -m venv venv
        venv\Scripts\activate

3. **Install dependencies**

        pip install -r requirements.txt

4. **Run the pipeline**

        python hydro_pipeline.py

5. **Run tests**

        python -m pytest -v

---

## Key Design Decisions

- Functional, modular structure for maintainability
- Idempotent load using database uniqueness constraint
- Logging for observability
- Separation of raw and cleaned data
- Windows-compatible paths

## Future Improvements

- **Incremental Loads:** Use timestamps to ingest only new data
- **Parameter Configuration:** Move tracked parameters into config files or environment variables
- **Error Handling & Retries:** Add structured exception handling and retry logic for API failures
- **Structured Logging:** Configurable log levels and optional file-based logging
- **Containerisation:** Dockerise the pipeline for portability
- **Scheduling:** Integrate with cron or Airflow for automated runs
- **Cloud Deployment:** Use managed storage in Azure/AWS instead of local SQLite
- **Data Validation:** Validate missing values, units, and anomalies
- **CI/CD Integration:** GitHub Actions to automatically run tests on pull requests
