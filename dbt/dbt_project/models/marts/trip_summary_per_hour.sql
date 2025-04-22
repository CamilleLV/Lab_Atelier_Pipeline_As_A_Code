-- models/marts/trip_summary_per_hour.sql

WITH joined_data AS (
    SELECT
        t.trip_id,
        t.trip_duration,
        t.tip_percentage,
        t.pickup_hour,
        w.description_temps
    FROM {{ ref('trip_weather_join') }} t
    JOIN {{ ref('stg_weather') }} w ON t.pickup_hour = w.heure_lever_soleil
)

SELECT
    pickup_hour,
    description_temps,
    COUNT(trip_id) AS number_of_trips,
    AVG(trip_duration) AS avg_trip_duration,
    AVG(tip_percentage) AS avg_tip_percentage
FROM joined_data
GROUP BY pickup_hour, description_temps
