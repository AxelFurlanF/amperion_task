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

means that the timesteps parameter of the tomorrow API has to be set to `1h`. The other interpretation that this could have is that I also need to setup a system to run this process in an hourly fashion. For this, I write below that an Airflow setup (or similar) should be included in the solution. But given that having to wait for an airflow setup to build and deploy locally is probably a pain for the person testing this solution, I left it out.

In exchange, I just leave you a simple `crontab` command to achieve a similar behaviour (if you want to).

Do:
```sh
crontab -e
```
and inside the vim editor, write:
```
docker compose up tomorrow -e SNAPSHOT_TIME=<ISO timestamp>
```

## Considerations to move to a production environment
This project is not intended 100% to be production ready. There's a couple considerations to be taken into account if moving into production:
1. Consider opting out of `pandas` if loading larger amounts of data (more locations or more precise timesteps). Consider [polars](https://pola.rs) or [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) using a Spark cluster.
2. It's a better idea to get the raw data and directly dump into into a bucket or cloud storage. Also using partitions for the parquet files, probably based on the timesteps. Something like `year=2024/month=01/day=01/hour=00` or based on the location like `location=newyork` if given locations with names.
3. Move the "load" part, specifically the `upsert_to_postgres` function to another process. Probably a generic process that you can pass the parquet files, the target table and it MERGEs the source with the target. This way, we can have the raw data sitting in a Data Lake and can move it to a specific table in a Data Warehouse using this process.
4. Consider moving away from Postgres, if dealing with larger amounts of data. Options like Snowflake, BigQuery, Redshift, Clickhouse or even engines like Athena are a good idea.

And also, one of the biggest ones:

5. A process like this that needs to be scheduled, and probably provide a sort of fault tolerancy behaviour, needs a better way to be scheduled. Tools like Apache Airflow, Dagster and Prefect are good for this.

## Requirements

- Docker
- Docker Compose

## Setup

1. Create a `.env` file with the following environment variables:

    ```env
    TOMORROW_API_KEY=<your_tomorrow_api_key>
    ```

2. Build and start the Docker containers:
    ```sh
    docker compose up --build
    ```

3. Access the Jupyter notebook server at [localhost:8888](http://localhost:8888).

4. Optionally, access the postgres client to run queries by doing:
    ```sh
    docker exec -it <postgres-container-name> bash
    ```
    and inside the container:
    ```sh
    psql postgresql://postgres:postgres@postgres:5432/tomorrow
    ```

5. If you want to inspect the generated parquet file, you can do so inside of `tomorrow_app/data`

## Usage

1. Fetch weather data and store it in a parquet file:
    ```sh
    python tomorrow_app/main.py
    ```

2. Load the weather data into the PostgreSQL database:
    ```sh
    python tomorrow_app/main.py
    ```

3. Visualize the data using the provided Jupyter notebook.

## SQL Queries

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

## File Structure

- main.py: Main script to fetch and process weather data.
- test_transform_row.py: Unit tests for the transform_row function.
- locations.json: List of geographic locations.
- init-db.sql: SQL script to initialize the PostgreSQL database schema.
- docker-compose.yaml: Docker Compose configuration.
