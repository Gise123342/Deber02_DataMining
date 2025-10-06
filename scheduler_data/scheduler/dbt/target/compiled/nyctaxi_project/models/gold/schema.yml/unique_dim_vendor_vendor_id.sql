
    
    

select
    vendor_id as unique_field,
    count(*) as n_records

from NYCTAXI.GOLD.DIM_VENDOR
where vendor_id is not null
group by vendor_id
having count(*) > 1


