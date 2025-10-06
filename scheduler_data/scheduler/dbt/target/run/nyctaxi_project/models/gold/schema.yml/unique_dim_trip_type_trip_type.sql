select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    trip_type as unique_field,
    count(*) as n_records

from NYCTAXI.GOLD.DIM_TRIP_TYPE
where trip_type is not null
group by trip_type
having count(*) > 1



      
    ) dbt_internal_test