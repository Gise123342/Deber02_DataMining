select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trip_type
from NYCTAXI.GOLD.DIM_TRIP_TYPE
where trip_type is null



      
    ) dbt_internal_test