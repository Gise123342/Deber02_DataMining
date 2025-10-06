select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select rate_code_id
from NYCTAXI.GOLD.DIM_RATE_CODE
where rate_code_id is null



      
    ) dbt_internal_test