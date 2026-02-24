import requests
import logging

BASE_URL = "https://environment.data.gov.uk/hydrology"
STATION_ID = "E64999A"

TARGET_PARAMETERS = {
    "DISSOLVED OXYGEN": "mg/l",
    "CONDUCTIVITY": "uS/cm"
}


def fetch_station_measures():
   
    # Retrieve all available measures for the station.
    url = f"{BASE_URL}/id/measures"
    params = {"station": STATION_ID}

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    return response.json().get("items", [])


def filter_target_measures(measures):
    
    # Filter for dissolved oxygen and conductivity.
    selected = []

    for measure in measures:
        parameter = measure.get("parameter")
        unit = (measure.get("unitName") or "").lower()

        if parameter in TARGET_PARAMETERS:
            if TARGET_PARAMETERS[parameter].lower() == unit or parameter == "CONDUCTIVITY":
                selected.append(measure)

    return selected


def fetch_recent_readings(measure_url, limit=10):
   
    # Fetch latest readings for a measure.
    
    url = f"{measure_url}/readings.json"
    params = {"_limit": limit, "_sort": "-dateTime"}

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    return response.json().get("items", [])