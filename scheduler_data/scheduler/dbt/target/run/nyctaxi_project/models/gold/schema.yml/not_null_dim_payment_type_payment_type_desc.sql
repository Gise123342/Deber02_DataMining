select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select payment_type_desc
from NYCTAXI.GOLD.DIM_PAYMENT_TYPE
where payment_type_desc is null



      
    ) dbt_internal_test