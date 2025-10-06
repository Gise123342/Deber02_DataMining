select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date_actual
from NYCTAXI.GOLD.DIM_DATE
where date_actual is null



      
    ) dbt_internal_test