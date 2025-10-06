{{ config(
    materialized = 'table',
    alias = 'DIM_DATE'
) }}

{% call statement('create_gold_schema', fetch_result=False) %}
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.GOLD;
{% endcall %}

WITH dates AS (
    SELECT DISTINCT DATE(PICKUP_TS) AS date_actual
    FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
    UNION
    SELECT DISTINCT DATE(DROPOFF_TS) AS date_actual
    FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
)
SELECT
    date_actual,
    YEAR(date_actual) AS year,
    MONTH(date_actual) AS month,
    DAY(date_actual) AS day,
    DAYOFWEEK(date_actual) AS day_of_week,
    CASE WHEN DAYOFWEEK(date_actual) IN (6,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type
FROM dates
