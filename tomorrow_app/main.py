import json
import logging
import os
from typing import Dict, List, Optional

import pandas as pd
import requests
from sqlalchemy import MetaData, Table, create_engine, text

COLUMNS = ["snapshot_time", "latitude",
           "longitude", "temperature", "wind_speed"]
QUERY_FIELDS = ["temperature", "windSpeed"]

TABLE = "weather_history_forecast"
SCHEMA = "bronze_data"

DUMP_DIR = "/tmp"


def get_locations() -> List[Dict[str, float]]:
    """
    Read the locations from the locations.json file

    Returns:
    list: A list of dictionaries containing the location data
    """
    with open("locations.json", "r") as f:
        locations = json.load(f)
    return locations["locations"]


def transform_row(row: Dict, location: Dict[str, float]) -> Dict[str, float]:
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


def fetch_weather_data(
    api_key: str,
    locations: List[Dict[str, float]],
    start_time: str = "nowMinus1h",
    end_time: str = "nowPlus6h",
    params: Optional[Dict] = {},
) -> List[Dict]:
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
    base_url = "https://api.tomorrow.io/v4/timelines"

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

        # TODO: remove this break
        # break

    return rows


def get_history_and_forecast(
    api_key: str,
    locations: List[Dict[str, float]],
    snapshot_time: Optional[str] = None,
) -> pd.DataFrame:
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

    final_df["snapshot_time"] = pd.to_datetime(final_df["snapshot_time"])
    final_df["latitude"] = final_df["latitude"].astype(float)
    final_df["longitude"] = final_df["longitude"].astype(float)

    return final_df


def upsert_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    schema: str,
    db_url: str,
    pk_cols: List[str],
) -> None:
    """
    Perform an UPSERT (insert or update) of a Pandas DataFrame into a PostgreSQL table using MERGE.

    :param df: Pandas DataFrame to be upserted.
    :param table_name: Name of the table in the database.
    :param schema: Schema name in the database.
    :param db_url: Database URL (e.g., 'postgresql://user:password@host:port/database').
    :param pk_cols: Columns to check for conflict (list of strings).
    """
    engine = create_engine(db_url)
    temp_table = f"{schema}.temp_{table_name}"

    with engine.connect() as conn:

        metadata = MetaData()
        target_table = Table(
            f"{table_name}",
            metadata,
            schema=schema,
            autoload_with=engine
        )

        # Map the target table's column types dynamically
        dtype_mapping = {col.name: col.type for col in target_table.columns}

        # Save df to temp table
        df.to_sql(temp_table.split(
            '.')[-1], engine, schema=schema, if_exists='replace', index=False, dtype=dtype_mapping)

        # Prepare the columns and values for the MERGE query
        merge_conditions = " AND ".join(
            [f"target.{col} = source.{col}" for col in pk_cols]
        )
        update_set = ", ".join(
            [f"{col} = source.{col}" for col in df.columns if col not in pk_cols]
        )
        insert_values = ", ".join(
            [f"source.{col}" for col in df.columns]
        )

        # Construct the MERGE query
        merge_query = text(f"""
        MERGE INTO {schema}.{table_name} AS target
        USING {temp_table} AS source
        ON {merge_conditions}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(df.columns)})
            VALUES ({insert_values});
        """)

        # Execute the MERGE query
        conn.execute(merge_query)
        conn.commit()

    logging.info(f"Data upserted into {schema}.{table_name} successfully.")


if __name__ == '__main__':
    api_key = os.environ.get("TOMORROW_API_KEY")
    snapshot_time = os.environ.get("SNAPSHOT_TIME")

    locations = get_locations()
    weather_df = get_history_and_forecast(api_key, locations, snapshot_time)

    # Save the weather data to a parquet file
    weather_df.to_parquet(f"{DUMP_DIR}/weather_data.parquet", index=False)

    # Load the data to the PostgreSQL database
    postgres_uri = os.environ.get("POSTGRES_URI")
    table = os.environ.get("TABLE", TABLE)
    schema = os.environ.get("SCHEMA", SCHEMA)
    upsert_to_postgres(weather_df, table, schema, postgres_uri,
                       ["latitude", "longitude", "snapshot_time"])
