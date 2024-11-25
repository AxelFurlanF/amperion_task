## Description

This project scrapes the Tomorrow IO API for weather forecasts and recent weather history for a set of geographic locations. The data is then stored in a PostgreSQL database and can be queried to answer specific questions about the weather data.

## Features

- Scrapes weather data from the Tomorrow IO API
- Stores weather data in a PostgreSQL database
- Provides a Jupyter notebook for data visualization

## Extra Features
- CI/CD using Github Actions
- Unit testing on `tomorrow_app/tests`
- Dependency installation using `uv`. A new lightweight and fast package manager.

## Assumptions
I assume that the following phrase:
> The forecasts should be scraped hourly for each location on the list,

means that the timesteps parameter of the tomorrow API has to be set to `1h`. The other interpretation that this could have is that I also need to setup a system to run this process in an hourly fashion. For this, I explain in the section below that an Airflow setup (or similar) should be included in the solution. But given that having to wait for an airflow setup to build and deploy locally is probably a pain for the person testing this solution, I decided to leave it out.

## Considerations to move to a production environment
This project is not intended 100% to be production ready. There's a couple considerations to be taken into account if moving into production:
1. Consider opting out of `pandas` if loading larger amounts of data (more locations or more precise timesteps). Consider [polars](https://pola.rs) or [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) using a Spark cluster.
2. It's a better idea to get the raw data and directly dump into into a bucket or cloud storage. Also using partitions for the parquet files, probably based on the timesteps. Something like `year=2024/month=01/day=01/hour=00` or based on the location like `location=newyork` if given locations with names.
3. Move the "load" part, specifically the `upsert_to_postgres` function to another isolated process. Probably a generic process that you can pass the parquet files, the target table for it to MERGE the source files with the target. This way, we can have the raw data sitting in a Data Lake and can move it to a specific table in a Data Warehouse using this process.
4. Consider moving away from Postgres, if dealing with larger amounts of data. Options like Snowflake, BigQuery, Redshift, Clickhouse or even engines like Athena are a good idea.

And also, one of the biggest ones:

1. A process like this needs a system to schedule it. It's a good idea to deploy it in a service that let's you handle failures gracefully, provide support for secrets saving and also provides monitoring tools. Services like Apache Airflow, Dagster and Prefect are good for this.

## Requirements

- Docker
- Docker Compose

## Setup and usage steps

1. Create a `.env` file with the following environment variables:

    ```env
    TOMORROW_API_KEY=<your_tomorrow_api_key>
    ```

2. Build and start the Docker containers:
    ```sh
    docker compose up --build
    ```

3. Access the Jupyter notebook server at [localhost:8888](http://localhost:8888). Then access the `work` folder, inside you should see the notebook.

4. Optionally, access the postgres client to run queries by doing:
    ```sh
    docker exec -it amperon_task-postgres-1 bash
    ```
    and inside the container:
    ```sh
    psql postgresql://postgres:postgres@postgres:5432/tomorrow
    ```

5. If you want to inspect the generated parquet file, you can do so by checking the `tomorrow_app/data` directory.

If you want to change locations to be analyzed, please refer to the `locations.json` file inside of the `tomorrow_app` folder. Make the changes necessary and re-deploy by running the container again like so:
 ```sh
 docker compose up tomorrow
 ```

Also, you can pass an environment variable called `SNAPSHOT_TIME` to be an ISO date. The container will grab that date and fill the data using that starting point. **This is particularly useful for backfills.**
 ```sh
 SNAPSHOT_TIME=2024-01-01 docker compose up tomorrow
 ```

**IMPORTANT: free tier API keys don't allow you to query data no more than 24h ago and no more than 5 days ahead.**

## SQL Queries
The following queries provide the answers for the two questions outlined in the assignment text.

- Latest temperature and wind speed for each geolocation:
    ```sql
    SELECT
    latitude,
    longitude,
    temperature AS latest_temperature,
    wind_speed AS latest_wind_speed,
    snapshot_time AS latest_snapshot_time
    FROM (
        SELECT
            latitude,
            longitude,
            temperature,
            wind_speed,
            snapshot_time,
            ROW_NUMBER() OVER (PARTITION BY latitude, longitude ORDER BY snapshot_time DESC) AS row_num
        FROM bronze_data.weather_history_forecast
    ) latest_data
    WHERE row_num = 1;
    ```

- Hourly time series of temperature for a selected location:
    ```sql
    SELECT snapshot_time, temperature, wind_speed
    FROM bronze_data.weather_history_forecast
    WHERE latitude = {location_lat} AND longitude = {location_lon}
    AND snapshot_time BETWEEN NOW() - INTERVAL '1 day' AND NOW() + INTERVAL '5 days'
    ORDER BY snapshot_time;
    ```

## Table structure
The table definition looks like this:
```SQL
CREATE TABLE IF NOT EXISTS bronze_data.weather_history_forecast (
    snapshot_time TIMESTAMP NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL
);
```
The latitude and longitude could also be condensed into a `GEOGRAPHY` data type, using `PostGIS`. This extension needs to be installed, and since there was no big advantage on using this on this project, I decided to opt out of it. More information about the `GEOGRAPHY` time [here](https://postgis.net/docs/using_postgis_dbmanagement.html#PostGIS_Geography).


## File Structure

- main.py: Main script to fetch and process weather data.
- test_transform_row.py: Unit tests for the transform_row function.
- locations.json: List of geographic locations.
- init-db.sql: SQL script to initialize the PostgreSQL database schema.
- docker-compose.yaml: Docker Compose configuration.
