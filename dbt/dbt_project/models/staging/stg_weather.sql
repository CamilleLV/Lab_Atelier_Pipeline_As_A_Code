-- Ce modèle extrait les données de la table 'weather_data'
with weather_data as (
    select
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
        heure_coucher_soleil,
        date_insertion
    from {{ source('weather', 'dim_weather') }}  -- Ici tu utilises la source définie dans sources.yml
)

select * from weather_data
