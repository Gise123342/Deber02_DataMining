select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    service_type_id as unique_field,
    count(*) as n_records

from NYCTAXI.GOLD.DIM_SERVICE_TYPE
where service_type_id is not null
group by service_type_id
having count(*) > 1



      
    ) dbt_internal_test