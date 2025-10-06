{{ config(
    materialized = 'table',
    alias = 'FACT_TRIPS'
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
        passenger_count,
        service_type,
        pickup_zone,
        pickup_borough,
        dropoff_zone,
        dropoff_borough,
        payment_type,
        VendorID,
        pickup_year,
        pickup_month,
        pickup_day
    FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
),


joined AS (
    SELECT
        -- 🕒 Fecha
        d.date_actual AS trip_date,
        d.year AS trip_year,
        d.month AS trip_month,
        d.day AS trip_day,

        -- 🌍 Zonas
        z.zone_name AS pickup_zone,
        z.borough_name AS pickup_borough,

        -- 🚖 Dimensiones clave
        v.vendor_id,
        p.payment_type_id,
        s.service_type_id,
        r.rate_code_id,
        t.trip_type,

        -- 🧮 Métricas
        b.trip_distance,
        b.trip_minutes,
        b.fare_amount,
        b.tip_amount,
        b.total_amount,
        ROUND((b.tip_amount / NULLIF(b.fare_amount, 0)) * 100, 2) AS tip_percent,
        b.passenger_count

    FROM base b

    LEFT JOIN {{ ref('dim_date') }} d
        ON b.pickup_year = d.year
       AND b.pickup_month = d.month
       AND b.pickup_day = d.day

    LEFT JOIN {{ ref('dim_zone') }} z
        ON b.pickup_zone = z.zone_name

    LEFT JOIN {{ ref('dim_vendor') }} v
        ON b.VendorID = v.vendor_id

    LEFT JOIN {{ ref('dim_payment_type') }} p
        ON b.payment_type = p.payment_type_desc

    LEFT JOIN {{ ref('dim_service_type') }} s
        ON b.service_type = s.service_desc

    LEFT JOIN {{ ref('dim_rate_code') }} r
        ON 1 = 1  -- Sin campo directo, todos los viajes usan la referencia base

    LEFT JOIN {{ ref('dim_trip_type') }} t
        ON 1 = 1  -- Sin campo directo, referencia general
),


aggregated AS (
    SELECT
        trip_year,
        trip_month,
        pickup_borough,
        pickup_zone,
        COUNT(*) AS total_trips,
        ROUND(AVG(trip_distance), 2) AS avg_distance_km,
        ROUND(AVG(trip_minutes), 2) AS avg_trip_minutes,
        ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
        ROUND(AVG(tip_amount), 2) AS avg_tip_amount,
        ROUND(AVG(tip_percent), 2) AS avg_tip_percent,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM joined
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM aggregated
