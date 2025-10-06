select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select payment_type_id
from NYCTAXI.GOLD.DIM_PAYMENT_TYPE
where payment_type_id is null



      
    ) dbt_internal_test