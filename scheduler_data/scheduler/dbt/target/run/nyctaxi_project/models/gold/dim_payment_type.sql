
  
    

        create or replace transient table NYCTAXI.GOLD.DIM_PAYMENT_TYPE
         as
        (



SELECT DISTINCT
    DENSE_RANK() OVER (ORDER BY PAYMENT_TYPE) AS payment_type_id,
    PAYMENT_TYPE AS payment_type_desc
FROM NYCTAXI.SILVER.TRIPS_CLEAN_ALL
WHERE PAYMENT_TYPE IS NOT NULL
        );
      
  