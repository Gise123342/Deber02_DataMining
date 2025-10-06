
  
    

        create or replace transient table NYCTAXI.SILVER.TRIPS_CLEAN_ALL
         as
        (

SELECT
    VendorID,
    pickup_ts,
    dropoff_ts,
    trip_minutes,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    tip_percent,
    payment_type,
    service_type,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_month_start
FROM NYCTAXI.SILVER.TRIPS_CLEAN

UNION ALL

SELECT
    VendorID,
    pickup_ts,
    dropoff_ts,
    trip_minutes,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    tip_percent,
    payment_type,
    service_type,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_month_start
FROM NYCTAXI.SILVER.TRIPS_CLEAN_GREEN
        );
      
  