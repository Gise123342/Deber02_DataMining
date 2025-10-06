select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select dropoff_zone as from_field
    from NYCTAXI.SILVER.TRIPS_CLEAN_GREEN
    where dropoff_zone is not null
),

parent as (
    select ZONE as to_field
    from NYCTAXI.RAW.TAXI_ZONE_LOOKUP
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test