{{ config(
    materialized = 'table',
    alias = 'FACT_TRIPS',
    cluster_by = ['pickup_year', 'pickup_month', 'pickup_borough']
) }}


{% call statement('create_gold_schema', fetch_result=False) %}
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.GOLD;
{% endcall %}

WITH base AS (
    SELECT
        pickup_ts,
        dropoff_ts,
        DATEDIFF('minute', pickup_ts, dropoff_ts) AS trip_minutes,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        service_type,
        pickup_zone,
        pickup_borough,
        dropoff_zone,
        dropoff_borough,
        pickup_year,
        pickup_month,
        pickup_day
    FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
),

aggregated AS (
    SELECT
        pickup_year,
        pickup_month,
        pickup_borough,
        pickup_zone,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_distance), 2) AS avg_distance_km,
        ROUND(AVG(trip_minutes), 2) AS avg_trip_minutes,
        ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
        ROUND(AVG(tip_amount), 2) AS avg_tip_amount,
        ROUND(AVG((tip_amount / NULLIF(fare_amount, 0)) * 100), 2) AS avg_tip_percent,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM base
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM aggregated

