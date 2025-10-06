select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trip_type_desc
from NYCTAXI.GOLD.DIM_TRIP_TYPE
where trip_type_desc is null



      
    ) dbt_internal_test