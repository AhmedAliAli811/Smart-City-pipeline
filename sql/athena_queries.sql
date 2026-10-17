-- Smart City Data Engineering Project
-- Amazon Athena CTAS queries for curated analytical tables.
--
-- Database: smartcity
-- Output format: Parquet on Amazon S3
--
-- Run each query separately in Athena.
-- CTAS creates a new table, so if a table already exists, drop it first
-- and make sure its external_location is empty before rerunning.

-- 1. Vehicle summary
CREATE TABLE smartcity.curated_vehicle_summary
WITH (
    format = 'PARQUET',
    external_location = 's3://smart-city-streaming-data-458195084286/curated/vehicle_summary/'
) AS
SELECT
    make,
    model,
    fueltype,
    COUNT(*) AS total_records,
    COUNT(DISTINCT device_id) AS unique_vehicles,
    ROUND(AVG(speed), 2) AS avg_speed,
    ROUND(MAX(speed), 2) AS max_speed,
    ROUND(AVG(CAST(year AS DOUBLE)), 1) AS avg_vehicle_year
FROM smartcity.vehicle_data
GROUP BY
    make,
    model,
    fueltype;


-- 2. Traffic summary by hour, location, and camera
CREATE TABLE smartcity.curated_traffic_summary
WITH (
    format = 'PARQUET',
    external_location = 's3://smart-city-streaming-data-458195084286/curated/traffic_summary/'
) AS
SELECT
    date_trunc('hour', timestamp) AS hour_timestamp,
    location,
    camera_id,
    COUNT(*) AS traffic_observations,
    COUNT(DISTINCT device_id) AS unique_devices
FROM smartcity.traffic_data
GROUP BY
    date_trunc('hour', timestamp),
    location,
    camera_id;


-- 3. Weather summary by hour and location
CREATE TABLE smartcity.curated_weather_summary
WITH (
    format = 'PARQUET',
    external_location = 's3://smart-city-streaming-data-458195084286/curated/weather_summary/'
) AS
SELECT
    date_trunc('hour', timestamp) AS hour_timestamp,
    location,
    COUNT(*) AS weather_observations,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(AVG(feels_like), 2) AS avg_feels_like,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    ROUND(AVG(pressure), 2) AS avg_pressure,
    ROUND(AVG(windspeed), 2) AS avg_windspeed,
    ROUND(AVG(visibility), 2) AS avg_visibility,
    ROUND(AVG(uv_index), 2) AS avg_uv_index
FROM smartcity.weather_data
GROUP BY
    date_trunc('hour', timestamp),
    location;


-- 4. Emergency summary by hour, location, incident type, and status
CREATE TABLE smartcity.curated_emergency_summary
WITH (
    format = 'PARQUET',
    external_location = 's3://smart-city-streaming-data-458195084286/curated/emergency_summary/'
) AS
SELECT
    date_trunc('hour', timestamp) AS hour_timestamp,
    location,
    type AS incident_type,
    status,
    COUNT(*) AS incident_count,
    COUNT(DISTINCT incidentID) AS unique_incidents
FROM smartcity.emergency_data
GROUP BY
    date_trunc('hour', timestamp),
    location,
    type,
    status;


-- 5. GPS summary by hour, vehicle type, and direction
CREATE TABLE smartcity.curated_gps_summary
WITH (
    format = 'PARQUET',
    external_location = 's3://smart-city-streaming-data-458195084286/curated/gps_summary/'
) AS
SELECT
    date_trunc('hour', timestamp) AS hour_timestamp,
    vehicle_type,
    direction,
    COUNT(*) AS gps_observations,
    COUNT(DISTINCT device_id) AS unique_devices,
    ROUND(AVG(speed), 2) AS avg_speed,
    ROUND(MAX(speed), 2) AS max_speed
FROM smartcity.gps_data
GROUP BY
    date_trunc('hour', timestamp),
    vehicle_type,
    direction;
