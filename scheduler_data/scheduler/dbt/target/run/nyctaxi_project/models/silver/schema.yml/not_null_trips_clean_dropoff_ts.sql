select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dropoff_ts
from NYCTAXI.SILVER.TRIPS_CLEAN
where dropoff_ts is null



      
    ) dbt_internal_test