{{ config(
    materialized = 'table',
    alias = 'DIM_VENDOR'
) }}

{% call statement('create_gold_schema', fetch_result=False) %}
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.GOLD;
{% endcall %}

SELECT DISTINCT
    VENDORID AS vendor_id,
    CASE
        WHEN VENDORID = 1 THEN 'Creative Mobile Technologies (CMT)'
        WHEN VENDORID = 2 THEN 'VeriFone Inc. (VTS)'
        ELSE 'Unknown'
    END AS vendor_name
FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
WHERE VENDORID IS NOT NULL
