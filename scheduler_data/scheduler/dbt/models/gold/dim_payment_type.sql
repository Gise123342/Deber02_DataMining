{{ config(
    materialized = 'table',
    alias = 'DIM_PAYMENT_TYPE'
) }}

{% call statement('create_gold_schema', fetch_result=False) %}
    CREATE SCHEMA IF NOT EXISTS {{ target.database }}.GOLD;
{% endcall %}

SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY PAYMENT_TYPE) AS payment_type_id,
    PAYMENT_TYPE AS payment_type_desc
FROM {{ source('SILVER', 'TRIPS_CLEAN_ALL') }}
WHERE PAYMENT_TYPE IS NOT NULL
