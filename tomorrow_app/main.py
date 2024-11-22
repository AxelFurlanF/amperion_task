import json
import logging
import os

import pandas as pd
import requests
from sqlalchemy import create_engine

COLUMNS = ["snapshot_time", "latitude",
           "longitude", "temperature", "wind_speed"]
QUERY_FIELDS = ["temperature", "windSpeed"]

TABLE = "weather_history_forecast"
SCHEMA = "bronze_data"


def get_locations():
    """
    Read the locations from the locations.json file

    Returns:
    list: A list of dictionaries containing the location data
    """
    with open("locations.json", "r") as f:
        locations = json.load(f)
    return locations["locations"]


def transform_row(row, location):
    """
    Transform the row data

    Args:
    row (dict): A dictionary containing the row data

    Returns:
    dict: A dictionary containing the transformed row data
    """
    values = row["values"]
    return {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "snapshot_time": row["startTime"],
        "temperature": values["temperature"],
        "wind_speed": values["windSpeed"],
    }


def fetch_weather_data(api_key, locations, start_time="nowMinus1h", end_time="nowPlus6h", params={}):
    """
    Fetch the weather data from the Tomorrow.io API

    Args:
    api_key (str): The API key
    locations (list): A list of dictionaries containing the location data
    start_time (str): The start time for the weather data
    end_time (str): The end time for the weather data
    params (dict): Additional query parameters

    Returns:
    list: A list of dictionaries containing the weather data
    """
    rows = []
    base_url = f"https://api.tomorrow.io/v4/timelines"

    for location in locations:
        s_location = f"{location['lat']}, {location['lon']}"
        headers = {"accept": "application/json"}
        query_params = {
            "apikey": api_key,
            "fields": ','.join(QUERY_FIELDS),
            "units": "metric",
            "timesteps": ["1h"],
            "location": s_location,
            "startTime": start_time,
            "endTime": end_time,
            **params
        }

        response = requests.get(base_url, headers=headers, params=query_params)
        response.raise_for_status()

        data = response.json()
        timelines = data["data"]["timelines"][0]["intervals"]

        for timeline in timelines:
            row = transform_row(timeline, location)
            rows.append(row)

    return rows


def get_history_and_forecast(api_key, locations, snapshot_time=None):
    """
    Get the forecast and history data from the Tomorrow.io API

    Args:
    api_key (str): The API key
    locations (list): A list of dictionaries containing the location data
    snapshot_time (str): The snapshot time for the forecast data

    Returns:
    pd.DataFrame: A DataFrame containing the forecast data
    """
    start_time = None
    end_time = None
    if snapshot_time:
        # extract datetime from ISO 8601 or similar format
        snapshot_time = pd.to_datetime(snapshot_time)

        start_time = snapshot_time - pd.Timedelta(hours=1)  # 1 hour before
        end_time = snapshot_time + pd.Timedelta(days=5)  # 5 days later

        # to ISO 8601 format
        start_time = start_time.isoformat()
        end_time = end_time.isoformat()

    # if snapshot_time is not specified, use the default values
    rows = fetch_weather_data(
        api_key, locations,
        start_time=start_time or "nowMinus1h",
        end_time=end_time or "nowPlus5d")
    final_df = pd.DataFrame(rows, columns=COLUMNS)
    return final_df


def upsert_to_postgres(df, table_name, schema, db_url, pk_cols):
    """
    Perform an UPSERT (insert or update) of a Pandas DataFrame into a PostgreSQL table.

    :param df: Pandas DataFrame to be upserted.
    :param table_name: Name of the table in the database.
    :param schema: Schema name in the database.
    :param db_url: Database URL (e.g., 'postgresql://user:password@host:port/database').
    :param pk_cols: Columns to check for conflict (list of strings).
    """
    engine = create_engine(db_url)
    temp_table = f"{schema}.temp_{table_name}"

    with engine.connect() as conn:
        # Save df to temp table
        df.to_sql(temp_table.split(
            '.')[-1], engine, schema=schema, if_exists='replace', index=False)

        # Use SQL for UPSERT
        conflict_cols = ", ".join(pk_cols)
        update_set = ", ".join(
            [f"{col}=EXCLUDED.{col}" for col in df.columns if col not in pk_cols])

        upsert_query = f"""
        INSERT INTO {schema}.{table_name} ({', '.join(df.columns)})
        SELECT * FROM {temp_table}
        ON CONFLICT ({conflict_cols}) DO UPDATE
        SET {update_set};
        """
        conn.execute(upsert_query)

        conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

    logging.info(f"Data upserted into {schema}.{table_name} successfully.")


if __name__ == '__main__':
    api_key = os.environ.get("TOMORROW_API_KEY")
    snapshot_time = os.environ.get("SNAPSHOT_TIME")

    locations = get_locations()
    weather_df = get_history_and_forecast(api_key, locations, snapshot_time)

    # Save the weather data to a parquet file
    weather_df.to_parquet("data/weather_data.parquet")

    # Load the data to the PostgreSQL database
    postgres_uri = os.environ.get("POSTGRES_URI")
    table = os.environ.get("TABLE", TABLE)
    schema = os.environ.get("SCHEMA", SCHEMA)
    upsert_to_postgres(weather_df, table, schema, postgres_uri,
                       ["latitude", "longitude", "snapshot_time"])
