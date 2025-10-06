
    
    

select
    service_type_id as unique_field,
    count(*) as n_records

from NYCTAXI.GOLD.DIM_SERVICE_TYPE
where service_type_id is not null
group by service_type_id
having count(*) > 1


