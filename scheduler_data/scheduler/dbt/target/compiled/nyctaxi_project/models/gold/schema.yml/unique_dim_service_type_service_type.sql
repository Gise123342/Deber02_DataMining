
    
    

select
    service_type as unique_field,
    count(*) as n_records

from NYCTAXI.GOLD.DIM_SERVICE_TYPE
where service_type is not null
group by service_type
having count(*) > 1


