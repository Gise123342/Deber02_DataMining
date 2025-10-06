select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select service_type_id
from NYCTAXI.GOLD.DIM_SERVICE_TYPE
where service_type_id is null



      
    ) dbt_internal_test