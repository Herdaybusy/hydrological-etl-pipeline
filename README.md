Hydrological Data ETL Pipeline
Overview

This project implements a modular ETL pipeline that extracts hydrological measurement data from the UK Environment Agency Hydrology Data Explorer API (public dataset), transforms it into a clean analytical format, and loads it into a SQLite database structured using a simple star schema.

The pipeline retrieves the 10 most recent readings for:

Dissolved Oxygen (mg/l)

Conductivity (µS/cm)

station: Hipper Park Road Bridge (E64999A)


Architecture

ETL Approach

    Extract – Retrieve measurement metadata and recent readings via REST API

    Transform – Clean, deduplicate and standardise data using pandas

    Load – Store structured data in SQLite following a star schema design

An ETL approach was chosen to ensure data quality and validation occur before persistence in the database, reducing the risk of storing inconsistent or malformed data.


Database Design (Star Schema)

Dimension Table:
stations

    station_id (PK)

    station_name

Fact Table:
measurements

    id (PK)

    station_id (FK)

    parameter

    date_time

    value

A composite UNIQUE constraint on (station_id, parameter, date_time) ensures idempotent behaviour and prevents duplicate records on re-execution.

Project Structure
etl/
    extract.py
    transform.py
    load.py
data/
    raw/
    cleaned_data/
tests/
    test_hydro_pipeline.py
hydro_pipeline.py
hydrological_architecture
hydrological.db
README.md
requirements.txt

hydro_pipeline.py – Orchestrates the end-to-end ETL process

Requirements

    Python 3.10+

    pandas

    requests

    pytest (for running tests)

All dependencies are listed in requirements.txt

Tested on:
    Windows 11
    Python 3.13


Setup Instructions (Windows)

Clone the repository:

    git clone <repo_url>

Create a virtual environment (optional but recommended):

    python -m venv venv
    venv\Scripts\activate

Install dependencies:

    pip install -r requirements.txt

Run the pipeline:

    python hydro_pipeline.py

Run tests:

    python -m pytest -v


Key Design Decisions

    Functional modular structure for maintainability

    Idempotent load using database uniqueness constraint

    Logging for observability

    Separation of raw and cleaned data

    Windows-compatible paths


Future Improvements

Incremental Loads
Instead of dropping the table on every run, implement incremental ingestion using  timestamps to avoid duplicate records and improve efficiency.

Parameter Configuration
Move tracked parameters (e.g., dissolved oxygen, conductivity) into a configuration file or environment variable to make the pipeline more flexible.

Error Handling & Retries
Add retry logic and structured exception handling for API failures or network interruptions.

Structured Logging
Enhance logging with configurable log levels and optional file-based logging

Containerisation
Dockerise the pipeline for portability and easier deployment.

Scheduling
Integrate with a scheduler (e.g., cron or Airflow) for automated periodic runs.

Cloud Deployment
Deploy to a cloud environment (e.g., Azure or AWS) with managed storage instead of local SQLite.

Data Validation
Add validation checks for missing values, unexpected units, or anomalous readings.

CI/CD Integration
Configure GitHub Actions to automatically run tests on pull requests.