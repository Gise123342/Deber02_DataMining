
    
    

select
    trip_type as unique_field,
    count(*) as n_records

from NYCTAXI.GOLD.DIM_TRIP_TYPE
where trip_type is not null
group by trip_type
having count(*) > 1


