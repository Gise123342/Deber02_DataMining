
    
    

with all_values as (

    select
        payment_type as value_field,
        count(*) as n_records

    from NYCTAXI.SILVER.TRIPS_CLEAN_ALL
    group by payment_type

)

select *
from all_values
where value_field not in (
    'Credit card','Cash','No charge','Dispute','Unknown','Voided trip','Other'
)


