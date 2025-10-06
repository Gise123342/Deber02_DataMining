{{ config(
    materialized = 'table',
    alias = 'TRIPS_AUDIT'
) }}



WITH raw_counts AS (
    SELECT
        'yellow' AS service_type,
        COUNT(*) AS total_raw
    FROM {{ source('RAW', 'YELLOW_TRIPS') }}
    UNION ALL
    SELECT
        'green' AS service_type,
        COUNT(*) AS total_raw
    FROM {{ source('RAW', 'GREEN_TRIPS') }}
),

clean_counts AS (
    SELECT
        service_type,
        COUNT(*) AS total_clean
    FROM {{ ref('trips_clean_all') }}
    GROUP BY service_type
)

SELECT
    CURRENT_TIMESTAMP() AS audit_ts,
    r.service_type,
    r.total_raw,
    c.total_clean,
    r.total_raw - c.total_clean AS records_removed,
    ROUND((c.total_clean / NULLIF(r.total_raw, 0)) * 100, 2) AS pct_retained


FROM raw_counts r
LEFT JOIN clean_counts c
    ON r.service_type = c.service_type
