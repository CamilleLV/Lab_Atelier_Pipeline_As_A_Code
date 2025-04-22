-- models/marts/trip_metrics.sql

WITH trip_data AS (
    SELECT
        t.trip_id,
        t.trip_duration,
        t.tip_percentage,
        t.payment_type,
        t.pickup_location_id,
        t.zone_name
    FROM {{ ref('stg_taxi') }} t
)

SELECT
    COUNT(trip_id) AS total_trips,  -- Nombre total de trajets
    AVG(trip_duration) AS avg_trip_duration,  -- Durée moyenne des trajets
    SUM(tip_percentage) AS total_tip_percentage,  -- Montant total des pourboires
    AVG(tip_percentage) AS avg_tip_percentage,  -- Pourcentage moyen de pourboire
    COUNT(DISTINCT zone_name) AS distinct_zones,  -- Nombre de zones uniques
    COUNT(DISTINCT pickup_location_id) AS distinct_pickup_locations  -- Nombre de lieux de prise en charge uniques
FROM trip_data
