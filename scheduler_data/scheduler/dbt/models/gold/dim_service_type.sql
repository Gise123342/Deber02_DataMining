{{ config(
    materialized = 'table',
    alias = 'DIM_SERVICE_TYPE'
) }}

{% call statement('create_gold_schema', fetch_result=False) %}
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.GOLD;
{% endcall %}

SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY SERVICE_TYPE) AS service_type_id,
    SERVICE_TYPE AS service_desc
FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
WHERE SERVICE_TYPE IS NOT NULL
