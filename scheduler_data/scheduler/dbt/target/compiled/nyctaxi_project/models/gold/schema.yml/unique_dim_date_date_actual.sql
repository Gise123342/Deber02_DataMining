
    
    

select
    date_actual as unique_field,
    count(*) as n_records

from NYCTAXI.GOLD.DIM_DATE
where date_actual is not null
group by date_actual
having count(*) > 1


