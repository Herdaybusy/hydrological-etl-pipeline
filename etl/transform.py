import pandas as pd
import os


def transform_data(rows):
    
    # Clean and prepare extracted rows.
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Save raw copy
    os.makedirs("data/raw", exist_ok=True)
    df.to_csv("data/raw/raw_data.csv", index=False)

    # Remove duplicates
    df = df.drop_duplicates(
        subset=["station_id", "parameter", "date_time"],
        keep="last"
    )

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

    # Save cleaned data
    os.makedirs("data/cleaned_data", exist_ok=True)
    df.to_csv("data/cleaned_data/processed_data.csv", index=False)

    return df