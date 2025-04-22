-- models/staging/stg_taxi.sql

WITH raw_taxi_data AS (
    SELECT
        trip_id,
        trip_duration,
        distance_category,
        payment_type,
        tip_percentage,
        pickup_hour,
        pickup_dayofweek,
        pickup_location_id,
        zone_name
    FROM {{ source('taxi_trips', 'fact_taxi_trips') }}
)

SELECT
    trip_id,
    trip_duration,
    distance_category,
    payment_type,
    tip_percentage,
    pickup_hour,
    pickup_dayofweek,
    pickup_location_id,
    zone_name
FROM raw_taxi_data
