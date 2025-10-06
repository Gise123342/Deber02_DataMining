
  
    

        create or replace transient table NYCTAXI.GOLD.DIM_VENDOR
         as
        (



SELECT DISTINCT
    VENDORID AS vendor_id,
    CASE
        WHEN VENDORID = 1 THEN 'Creative Mobile Technologies (CMT)'
        WHEN VENDORID = 2 THEN 'VeriFone Inc. (VTS)'
        ELSE 'Unknown'
    END AS vendor_name
FROM NYCTAXI.SILVER.TRIPS_CLEAN_ALL
WHERE VENDORID IS NOT NULL
        );
      
  