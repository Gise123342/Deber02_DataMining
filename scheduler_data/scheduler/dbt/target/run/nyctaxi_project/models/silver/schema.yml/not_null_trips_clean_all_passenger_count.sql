select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select passenger_count
from NYCTAXI.SILVER.TRIPS_CLEAN_ALL
where passenger_count is null



      
    ) dbt_internal_test