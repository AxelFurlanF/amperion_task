CREATE SCHEMA IF NOT EXISTS bronze_data;

CREATE TABLE IF NOT EXISTS bronze_data.weather_history_forecast (
    snapshot_time TIMESTAMP NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL
);
