select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pickup_ts
from NYCTAXI.SILVER.TRIPS_CLEAN_ALL
where pickup_ts is null



      
    ) dbt_internal_test