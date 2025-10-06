select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select fare_amount
from NYCTAXI.SILVER.TRIPS_CLEAN
where fare_amount is null



      
    ) dbt_internal_test