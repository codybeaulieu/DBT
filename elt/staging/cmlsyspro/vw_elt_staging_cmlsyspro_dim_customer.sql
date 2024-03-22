with customer as (
    select 
			concat(customer,':',source_system) as customer_id_bk
		,	source_system 
        ,   customer
        ,   name
		,	_rivery_run_id 
		,	_rivery_last_update 
    from {{ref('vw_stg_cmlsyspro_arcustomer')}}
)
select 
         {{ dbt_utils.surrogate_key(['customer_id_bk'])}}         as customer_sk
    ,    source_system                        as source_system
    ,    cast(customer as string)             as customer_id
    ,    name                                 as customer_name
    ,    _rivery_run_id                       as batch_id
    ,    _rivery_last_update                  as elt_load_datetime,
        sysdate()                             as elt_process_datetime
from customer