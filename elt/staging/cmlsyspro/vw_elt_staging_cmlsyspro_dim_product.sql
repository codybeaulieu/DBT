with invmaster as (
    select 
			concat(stockcode,':',source_system) as product_id_bk
		,	source_system 
        ,   stockcode 
        ,   description 
        ,   stockuom 
		,	_rivery_run_id 
		,	_rivery_last_update 
    from {{ref('vw_stg_cmlsyspro_invmaster')}}
),
invmasterplus as (
  select 
			 source_system 
        ,    stockcode 
        ,    salescategory 
        ,    subcat 
		,    coretechnology  
    from {{ref('vw_stg_cmlsyspro_invmaster_plus')}}
)
select
    {{ dbt_utils.surrogate_key(['inm.product_id_bk'])}}   as product_sk,
    inm.source_system                                     as source_system,
    inm.stockcode                                         as product_id,
    inm.description                                       as product_desc,
    inmp.salescategory                                    as product_cat,
    inmp.subcat                                           as product_sub_cat,
    inmp.coretechnology                                   as product_core_tech,
    inm.stockuom                                          as  product_uom,
    inm._rivery_run_id                                    as batch_id,
    inm._rivery_last_update                               as elt_load_datetime,
    sysdate()                                             as elt_process_datetime
from invmaster inm left join invmasterplus inmp
on inm.stockcode = inmp.stockcode and
    inm.source_system = inmp.source_system
