import os
import requests
import json
import pandas as pd

COLUMNS = ["time", "latitude", "longitude", "temperature", "wind_speed"]
QUERY_FIELDS = ["temperature", "windSpeed"]


def get_locations():
    """
    Read the locations from the locations.json file

    Returns:
    list: A list of dictionaries containing the location data
    """
    with open("locations.json", "r") as f:
        locations = json.load(f)
    return locations["locations"]


def fetch_weather_data(api_key, locations, start_time="now", end_time="nowPlus6h", params={}):
    """
    Fetch the weather data from the Tomorrow.io API

    Args:
    api_key (str): The API key
    locations (list): A list of dictionaries containing the location data
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
        timelines = data["timelines"]["hourly"]

        for timeline in timelines:
            print(timeline)
            values = timeline["values"]
            row = {
                "latitude": location["lat"],
                "longitude": location["lon"],
                "time": timeline["time"],
                "temperature": values["temperature"],
                "wind_speed": values["windSpeed"],
            }
            rows.append(row)
            break  # Remove this `break` if you want all timelines
        break  # Remove this `break` if you want all locations

    return rows


def get_history_and_forecast(api_key, locations, snapshot_time=None):
    """
    Get the forecast and history data from the Tomorrow.io API

    Args:
    api_key (str): The API key
    locations (list): A list of dictionaries containing the location data

    Returns:
    pd.DataFrame: A DataFrame containing the forecast data
    """
    start_time = None
    end_time = None
    if snapshot_time:
        # extract datetime from ISO 8601 or similar format
        snapshot_time = pd.to_datetime(snapshot_time)

        start_time = snapshot_time - pd.Timedelta(hours=1)  # 1 hour before
        # TODO: change this to +5 days
        end_time = snapshot_time + pd.Timedelta(hours=1)  # 5 days after

        # to ISO 8601 format
        start_time = start_time.isoformat()
        end_time = end_time.isoformat()

    # if snapshot_time is not specified, use the default values
    rows = fetch_weather_data(
        api_key, locations,
        start_time=start_time or "nowMinus1h",
        end_time=end_time or "nowPlus5d")
    final_df = pd.DataFrame(rows, columns=COLUMNS)
    print(final_df)
    return final_df


if __name__ == '__main__':
    api_key = os.environ.get("TOMORROW_API_KEY")
    snapshot_time = os.environ.get("SNAPSHOT_TIME")

    locations = get_locations()
    weather_df = get_history_and_forecast(api_key, locations, snapshot_time)
    print(weather_df)
