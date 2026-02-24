import pandas as pd
import os
import logging

def validate_schema(df):
    required_columns = ["station_id", "parameter", "date_time", "value"]

    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if not pd.api.types.is_numeric_dtype(df["value"]):
        raise TypeError("Column 'value' must be numeric.")

    if not pd.api.types.is_datetime64_any_dtype(df["date_time"]):
        raise TypeError("Column 'date_time' must be datetime.")

    logging.info("Schema validation passed.")


def transform_data(rows):
    
    # Clean and prepare extracted rows.
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Save raw copy
    os.makedirs("data/raw", exist_ok=True)
    df.to_csv("data/raw/raw_data.csv", index=False)
    logging.info("Raw data saved to data/raw/raw_data.csv")

    # Remove duplicates
    df = df.drop_duplicates(
        subset=["station_id", "parameter", "date_time"],
        keep="last"
    )
    
    # Convert all column names to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_', regex=True)

    # Convert all string columns to lowercase and replace spaces with underscores
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.replace(' ', '_', regex=True)
        df[col] = df[col].str.lower()
    
    # Convert datetime
    df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")

    # Drop nulls
    df = df.dropna(subset=["value", "date_time"])

    # Ensure numeric
    df["value"] = df["value"].astype(float)

    # Keep latest 10 per parameter
    df = (
        df.sort_values(["parameter", "date_time"], ascending=[True, False])
          .groupby("parameter")
          .head(10)
          .reset_index(drop=True)
    )
    
    logging.info("Data transformed and cleaned.")
    
    # Validate before returning
    validate_schema(df)
    
    # Save cleaned data
    os.makedirs("data/cleaned_data", exist_ok=True)
    df.to_csv("data/cleaned_data/processed_data.csv", index=False)
    logging.info("Cleaned data saved to data/cleaned_data/processed_data.csv")

    return df