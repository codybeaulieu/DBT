with sormaster as (
    select 
			concat(salesorder,':',source_system) as order_id_bk
		,	source_system 
        ,   salesorder
        ,    case 
                when orderstatus = '1' then   'Open order'
                when orderstatus = '2' then   'Open back order'
                when orderstatus = '3' then   'Released back order' 
                when orderstatus = '4' then   'In warehouse'
                when orderstatus = '5' then   'To invoice'
                when orderstatus = '6' then   'To transfer'
                when orderstatus = 'f' then   'Forward order'
                when orderstatus = 's' then   'In suspense'
                when orderstatus = '9' then   'Complete'
                else                          'Unknown'
            end as order_status
		,	_rivery_run_id 
		,	_rivery_last_update 
    from {{ref('vw_stg_cmlsyspro_sormaster')}}
)
select 
         {{ dbt_utils.surrogate_key(['order_id_bk'])}}         as order_sk
    ,    source_system       as source_system
    ,    salesorder          as order_id
    ,    order_status        as order_status
    ,    _rivery_run_id      as batch_id
    ,    _rivery_last_update as elt_load_datetime,
        sysdate()            as elt_process_datetime
from sormaster
