-- models/marts/trip_weather_join.sql

WITH taxi_data AS (
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
    FROM {{ ref('stg_taxi') }}
),
weather_data AS (
    SELECT
        id,
        description_temps,
        temperature,
        temperature_ressentie_c,
        humidite_wm,
        pression_atmospherique_hpa,
        niveau_mer_hpa,
        pression_sol_hpa,
        vitesse_vent_m_per_sec,
        direction_vent_deg,
        rafales_vent_m_per_sec,
        precipitations_1h_mm_per_h,
        couverture_nuageuse_perc,
        heure_lever_soleil,
        heure_coucher_soleil
    FROM {{ ref('stg_weather') }}
)

SELECT
    t.trip_id,
    t.trip_duration,
    t.distance_category,
    t.payment_type,
    t.tip_percentage,
    t.pickup_hour,
    t.pickup_dayofweek,
    t.pickup_location_id,
    t.zone_name,
    w.description_temps,
    w.temperature,
    w.temperature_ressentie_c,
    w.humidite_wm,
    w.pression_atmospherique_hpa,
    w.vitesse_vent_m_per_sec,
    w.direction_vent_deg,
    w.rafales_vent_m_per_sec,
    w.precipitations_1h_mm_per_h,
    w.couverture_nuageuse_perc
FROM taxi_data t
JOIN weather_data w
    ON t.pickup_hour = w.heure_lever_soleil
