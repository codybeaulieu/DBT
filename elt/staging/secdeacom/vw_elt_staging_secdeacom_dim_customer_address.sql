with src as (
    select  
        concat(dmbill.bi_id,':','BILLING',':',dmbill.source_system) as address_id_bk
    ,   concat(xref.gr_id,':',xref.source_system) as customer_id_bk
    ,   'BILLING' as address_type
    ,   dmbill.source_system as source_system
    ,   dmbill.bi_name as address_name
    ,   dmbill.bi_country as address_country
    ,   dmbill.bi_state as address_state
    ,   dmbill.bi_city as address_city
    ,   dmbill.bi_street as address_1
    ,   dmbill.bi_street2 as address_2
    ,   dmbill.bi_street3 as address_3
    ,   dmbill.bi_zip as address_zip
    ,   dmbill._rivery_run_id as batch_id
    ,   dmbill._rivery_last_update as elt_load_datetime
    ,   sysdate() as elt_process_datetime
from {{ref('vw_stg_secdeacom_dmbill')}}  dmbill left join 
     {{ref('vw_elt_staging_secdeacom_customer_xref')}} xref
on   dmbill.bi_id = xref.bi_id and dmbill.bi_name = xref.bi_name
union
select   concat(dmship.sh_id,':','SHIPPING',':',dmship.source_system) as address_id_bk
    ,    concat(xref.gr_id,':',xref.source_system) as customer_id_bk
    ,    'SHIPPING' as address_type
    ,    dmship.source_system as source_system
    ,    dmship.sh_name  as address_name
    ,    dmship.sh_country as address_country
    ,    dmship.sh_state as address_state
    ,    dmship.sh_city as address_city
    ,    dmship.sh_street as address_1
    ,    dmship.sh_street2 as address_2
    ,    dmship.sh_street3 as address_3
    ,    dmship.sh_zip as address_zip
    ,    dmship._rivery_run_id as batch_id
    ,    dmship._rivery_last_update as elt_load_datetime
    ,    sysdate() as elt_process_datetime
from {{ref('vw_stg_secdeacom_dmship')}} dmship left join 
     {{ref('vw_elt_staging_secdeacom_customer_xref')}} xref
on   dmship.sh_id = xref.sh_id and dmship.sh_name = xref.sh_name
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