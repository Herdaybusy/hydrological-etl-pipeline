import pytest
import pandas as pd
import sqlite3
from unittest.mock import patch

from hydro_pipeline import station_measures, recent_readings, run_pipeline
# Mock data for testing
mock_measures = [
    {"parameter": "DISSOLVED OXYGEN", "unitName": "mg/l", "@id": "measure/do"},
    {"parameter": "CONDUCTIVITY", "unitName": "uS/cm", "@id": "measure/cond"},
]

mock_readings = [
    {"dateTime": "2026-02-11T20:01:12", "value": 11.4},
    {"dateTime": "2026-02-11T19:01:12", "value": 11.38},
]

# Unit tests
@patch("hydro_pipeline.requests.get")
def test_station_measures(mock_get):
    """Station measures should return a list of measure items."""
    mock_get.return_value.json.return_value = {"items": mock_measures}
    mock_get.return_value.raise_for_status = lambda: None

    result = station_measures("dummy_station")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["parameter"] == "DISSOLVED OXYGEN"


@patch("hydro_pipeline.requests.get")
def test_recent_readings(mock_get):
    """Recent readings should return reading data."""
    mock_get.return_value.json.return_value = {"items": mock_readings}
    mock_get.return_value.raise_for_status = lambda: None

    result = recent_readings("measure/do")

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["value"] == 11.4 

@patch("hydro_pipeline.station_measures")
@patch("hydro_pipeline.recent_readings")
def test_run_pipeline(mock_recent, mock_measures_func, tmp_path):
    """
    Basic end-to-end check:
    - pipeline runs
    - data is written to DB
    - expected parameters exist
    """
    mock_measures_func.return_value = mock_measures
    mock_recent.return_value = mock_readings

    temp_db = tmp_path / "test_hydro.db"

    with patch("hydro_pipeline.DB_FILE", temp_db):
        run_pipeline()

    conn = sqlite3.connect(temp_db)
    df = pd.read_sql("SELECT * FROM measurements", conn)
    conn.close()

    assert not df.empty
    assert "dissolved_oxygen" in df["parameter"].values
    assert len(df) == 4