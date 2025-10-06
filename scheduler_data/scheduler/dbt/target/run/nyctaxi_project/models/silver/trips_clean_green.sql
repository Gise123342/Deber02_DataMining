
  
    

        create or replace transient table NYCTAXI.SILVER.TRIPS_CLEAN_GREEN
         as
        (


WITH base AS (
    SELECT
        VendorID,
        CAST(lpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup_ts,
        CAST(lpep_dropoff_datetime AS TIMESTAMP_NTZ) AS dropoff_ts,
        passenger_count::INT AS passenger_count,
        trip_distance::FLOAT AS trip_distance,
        fare_amount::FLOAT AS fare_amount,
        tip_amount::FLOAT AS tip_amount,
        total_amount::FLOAT AS total_amount,
        payment_type::INT AS payment_type_raw,
        PULocationID,
        DOLocationID,
        'green' AS service_type
    FROM NYCTAXI.RAW.GREEN_TRIPS
),

filtered AS (
    SELECT *
    FROM base
    WHERE 
        fare_amount > 0
        AND trip_distance > 0
        AND total_amount > 0
        AND passenger_count BETWEEN 1 AND 8
        AND pickup_ts IS NOT NULL
        AND dropoff_ts IS NOT NULL
        AND dropoff_ts >= pickup_ts
        AND DATEDIFF('hour', pickup_ts, dropoff_ts) <= 12
),

enriched AS (
    SELECT
        f.*,
        pu.ZONE AS pickup_zone,
        pu.BOROUGH AS pickup_borough,
        do.ZONE AS dropoff_zone,
        do.BOROUGH AS dropoff_borough
    FROM filtered f
    LEFT JOIN NYCTAXI.RAW.TAXI_ZONE_LOOKUP pu
        ON f.PULocationID = pu.LOCATIONID
    LEFT JOIN NYCTAXI.RAW.TAXI_ZONE_LOOKUP do
        ON f.DOLocationID = do.LOCATIONID
),

final AS (
    SELECT
        VendorID,
        pickup_ts,
        dropoff_ts,
        DATEDIFF('minute', pickup_ts, dropoff_ts) AS trip_minutes,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        ROUND((tip_amount / NULLIF(fare_amount, 0)) * 100, 2) AS tip_percent,
        CASE payment_type_raw
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided trip'
            ELSE 'Other'
        END AS payment_type,
        service_type,
        pickup_zone,
        pickup_borough,
        dropoff_zone,
        dropoff_borough,
        YEAR(pickup_ts) AS pickup_year,
        MONTH(pickup_ts) AS pickup_month,
        DAY(pickup_ts) AS pickup_day,
        DATE_TRUNC('month', pickup_ts) AS pickup_month_start,
        CURRENT_TIMESTAMP() AS load_ts
    FROM enriched
)

SELECT * FROM final
        );
      
  