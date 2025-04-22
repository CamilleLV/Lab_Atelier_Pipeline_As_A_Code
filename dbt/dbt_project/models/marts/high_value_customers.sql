-- models/marts/high_value_customers.sql

WITH customer_trips AS (
    SELECT
        t.trip_id,
        t.tip_percentage,
        t.payment_type,
        t.pickup_location_id,
        t.zone_name,
        c.passenger_count,
        c.total_spent
    FROM {{ ref('stg_taxi') }} t
    LEFT JOIN {{ ref('customer_data') }} c ON t.trip_id = c.trip_id
)

SELECT
    customer_id,
    COUNT(trip_id) AS number_of_trips,
    SUM(total_spent) AS total_spent,
    AVG(tip_percentage) AS avg_tip_percentage
FROM customer_trips
GROUP BY customer_id
HAVING COUNT(trip_id) > 10
   AND SUM(total_spent) > 300
   AND AVG(tip_percentage) > 15
