{{ config(
    materialized = 'table',
    alias = 'DIM_TRIP_TYPE'
) }}

{% call statement('create_gold_schema', fetch_result=False) %}
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.GOLD;
{% endcall %}


SELECT * FROM (
    SELECT 1 AS trip_type, 'Street-hail' AS trip_type_desc UNION ALL
    SELECT 2 AS trip_type, 'Dispatch' AS trip_type_desc UNION ALL
    SELECT 99 AS trip_type, 'Unknown' AS trip_type_desc
)
