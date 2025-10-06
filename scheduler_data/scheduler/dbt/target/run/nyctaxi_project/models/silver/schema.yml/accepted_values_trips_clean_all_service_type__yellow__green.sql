select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        service_type as value_field,
        count(*) as n_records

    from NYCTAXI.SILVER.TRIPS_CLEAN_ALL
    group by service_type

)

select *
from all_values
where value_field not in (
    'yellow','green'
)



      
    ) dbt_internal_test