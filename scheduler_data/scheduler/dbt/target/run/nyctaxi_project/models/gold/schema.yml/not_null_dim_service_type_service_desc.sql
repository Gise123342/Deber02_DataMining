select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select service_desc
from NYCTAXI.GOLD.DIM_SERVICE_TYPE
where service_desc is null



      
    ) dbt_internal_test