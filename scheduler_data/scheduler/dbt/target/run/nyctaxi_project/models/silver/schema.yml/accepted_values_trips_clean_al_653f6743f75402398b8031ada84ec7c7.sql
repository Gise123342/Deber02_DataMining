select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        passenger_count as value_field,
        count(*) as n_records

    from NYCTAXI.SILVER.TRIPS_CLEAN_ALL
    group by passenger_count

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8'
)



      
    ) dbt_internal_test