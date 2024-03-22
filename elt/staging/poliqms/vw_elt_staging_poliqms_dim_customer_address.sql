with src as (
     select  
        concat(bill_to.id,':','BILLING',':',bill_to.source_system)  as address_id_bk
    ,   concat(arcusto.id,':',arcusto.source_system) 				as customer_id_bk
    ,   'BILLING' 													as address_type
    ,   bill_to.source_system 										as source_system
    ,   arcusto.company 											as address_name
    ,   bill_to.country 											as address_country
    ,   bill_to.state 												as address_state
    ,   bill_to.city 												as address_city
    ,   bill_to.addr1 												as address_1
    ,   bill_to.addr2 												as address_2
    ,   bill_to.addr3 												as address_3
    ,   bill_to.zip 												as address_zip
    ,   bill_to._rivery_run_id 										as batch_id
    ,   bill_to._rivery_last_update 								as elt_load_datetime
    ,   sysdate() as elt_process_datetime
from {{ref('vw_stg_poliqms_bill_to')}}  bill_to left join 
     {{ref('vw_stg_poliqms_arcusto')}} arcusto
on   bill_to.arcusto_id = arcusto.id and bill_to.source_system = arcusto.source_system
union
select  
        concat(ship_to.id,':','SHIPPING',':',ship_to.source_system) as address_id_bk
    ,   concat(arcusto.id,':',arcusto.source_system)                as customer_id_bk
    ,   'SHIPPING'                                                  as address_type
    ,   ship_to.source_system                                       as source_system
    ,   arcusto.company                                             as address_name
    ,   ship_to.country                                             as address_country
    ,   ship_to.state                                               as address_state
    ,   ship_to.city                                                as address_city
    ,   ship_to.addr1 												as address_1
    ,   ship_to.addr2 												as address_2
    ,   ship_to.addr3 												as address_3
    ,   ship_to.zip 												as address_zip
    ,   ship_to._rivery_run_id 										as batch_id
    ,   ship_to._rivery_last_update 								as elt_load_datetime
    ,   sysdate() 													as elt_process_datetime
from {{ref('vw_stg_poliqms_ship_to')}}  ship_to left join 
     {{ref('vw_stg_poliqms_arcusto')}} arcusto
on   ship_to.arcusto_id = arcusto.id and ship_to.source_system = arcusto.source_system
)
select 
        {{ dbt_utils.surrogate_key(['address_id_bk','customer_id_bk'])}} as address_sk
    ,   {{ dbt_utils.surrogate_key(['customer_id_bk'])}}  as customer_sk
    ,   address_type
    ,   source_system
    ,   address_name
    ,   address_country
    ,   address_state
    ,   address_city
    ,   address_1
    ,   address_2
    ,   address_3
    ,   address_zip
    ,   batch_id
    ,   elt_load_datetime
    ,   elt_process_datetime
from src