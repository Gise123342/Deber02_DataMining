{{ config(
    materialized = 'table',
    alias = 'DIM_ZONE'
) }}

{% call statement('create_gold_schema', fetch_result=False) %}
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.GOLD;
{% endcall %}

SELECT DISTINCT
    pickup_zone AS zone_name,
    pickup_borough AS borough_name
FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
WHERE pickup_zone IS NOT NULL
UNION
SELECT DISTINCT
    dropoff_zone AS zone_name,
    dropoff_borough AS borough_name
FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
WHERE dropoff_zone IS NOT NULL
