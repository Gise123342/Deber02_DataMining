



SELECT DISTINCT
    pickup_zone AS zone_name,
    pickup_borough AS borough_name
FROM NYCTAXI.SILVER.TRIPS_CLEAN_ALL
WHERE pickup_zone IS NOT NULL
UNION
SELECT DISTINCT
    dropoff_zone AS zone_name,
    dropoff_borough AS borough_name
FROM NYCTAXI.SILVER.TRIPS_CLEAN_ALL
WHERE dropoff_zone IS NOT NULL