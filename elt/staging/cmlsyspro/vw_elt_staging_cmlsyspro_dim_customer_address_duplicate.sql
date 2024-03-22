with customer as (
    select 
			concat(name,':',soldtoaddr5,':',soldtoaddr4,':',soldtoaddr3loc,':',
                            soldtoaddr1,':',soldtoaddr2,':',soldtoaddr3,':',                            
                            soldpostalcode,':',source_system) as address_id_bk
        ,   concat(customer,':',source_system) as customer_id_bk
		,	source_system 
        ,   name
        ,   soldtoaddr5
        ,   soldtoaddr4
        ,   soldtoaddr3loc
        ,   soldtoaddr1
        ,   soldtoaddr2
        ,   soldtoaddr3
        ,   soldpostalcode
		,	_rivery_run_id 
		,	_rivery_last_update 
    from {{ref('vw_stg_cmlsyspro_arcustomer')}}
),
mdnmaster as (
    select concat(dispatchcustname,':',dispatchaddress5,':', dispatchaddress4,':',dispatchaddrloc,':',
                                      dispatchaddress1,':',dispatchaddress2,':',dispatchaddress3,':',
                                      dispatchpostalcode,':',source_system) as address_id_bk
        ,    concat(customer,':',source_system) as customer_id_bk
        ,    source_system
        ,    dispatchcustname
        ,    dispatchaddress5
        ,    dispatchaddress4
        ,    dispatchaddrloc
        ,    dispatchaddress1
        ,    dispatchaddress2
        ,    dispatchaddress3
        ,    dispatchpostalcode
        ,    _rivery_run_id
        ,    _rivery_last_update
    from {{ref('vw_stg_cmlsyspro_mdnmaster')}}
) 
select 
         {{ dbt_utils.surrogate_key(['address_id_bk','customer_id_bk'])}}          as address_sk
    ,    {{ dbt_utils.surrogate_key(['customer_id_bk'])}}         as customer_sk
    ,    'BILLING'              as address_type
    ,    source_system          as source_system
    ,    name                   as address_name
    ,    soldtoaddr5            as address_country
    ,    soldtoaddr4            as address_state
    ,    soldtoaddr3loc         as address_city
    ,    soldtoaddr1            as address_1
    ,    soldtoaddr2            as address_2
    ,    soldtoaddr3            as address_3
    ,    soldpostalcode         as address_zip
    ,    _rivery_run_id         as batch_id
    ,    _rivery_last_update    as elt_load_datetime,
        sysdate()               as elt_process_datetime
from customer
union all 
select 
         {{ dbt_utils.surrogate_key(['address_id_bk','customer_id_bk'])}}          as address_sk
    ,    {{ dbt_utils.surrogate_key(['customer_id_bk'])}}         as customer_sk
    ,    'SHIPPING'             as address_type
    ,    source_system          as source_system
    ,    dispatchcustname       as address_name
    ,    dispatchaddress5       as address_country
    ,    dispatchaddress4       as address_state
    ,    dispatchaddrloc        as address_city
    ,    dispatchaddress1       as address_1
    ,    dispatchaddress2       as address_2
    ,    dispatchaddress3       as address_3
    ,    dispatchpostalcode     as address_zip
    ,    _rivery_run_id         as batch_id
    ,    _rivery_last_update    as elt_load_datetime,
        sysdate()               as elt_process_datetime
from mdnmaster
