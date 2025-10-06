
  
    

        create or replace transient table NYCTAXI.GOLD.DIM_SERVICE_TYPE
         as
        (



SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY SERVICE_TYPE) AS service_type_id,
    SERVICE_TYPE AS service_desc
FROM NYCTAXI.SILVER.TRIPS_CLEAN_ALL
WHERE SERVICE_TYPE IS NOT NULL
        );
      
  