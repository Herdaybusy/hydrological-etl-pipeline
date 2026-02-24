import requests
import logging

BASE_URL = "https://environment.data.gov.uk/hydrology"
STATION_ID = "E64999A"

TARGET_PARAMETERS = {
    "DISSOLVED OXYGEN": "mg/l",
    "CONDUCTIVITY": "uS/cm"
}


def station_measures(station_id):
   
    # Retrieve all available measures for the station.
    url = f"{BASE_URL}/id/measures"
    params = {"station": STATION_ID}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        logging.info("Successfully fetched station measures.")
    except requests.RequestException as e:
        logging.error(f"Error fetching station measures: {e}")
        raise

    data = response.json()
    return data["items"]


def filter_target_measures(measures):
    
    # Filter for dissolved oxygen and conductivity.
    selected = []

    for measure in measures:
        parameter = measure.get("parameter")
        unit = (measure.get("unitName") or "").lower()

        if parameter in TARGET_PARAMETERS:
            logging.info(f"Evaluating measure: {parameter} with unit {unit}")
            if TARGET_PARAMETERS[parameter].lower() == unit or parameter == "CONDUCTIVITY":
                selected.append(measure)
    
    logging.info(f"Selected measures: {[m['parameter'] for m in selected]}")

    return selected


def recent_readings(measure_url, limit=10):
   
    # Fetch latest readings for a measure.
    
    url = f"{measure_url}/readings.json"
    params = {"_limit": limit, "_sort": "-dateTime"}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully fetched recent readings for {measure_url}.")    
    except requests.RequestException as e:
        logging.error(f"Error fetching recent readings for {measure_url}: {e}")
        raise

    data = response.json()
    return data["items"]