with arinvt as (
    select 
            concat(id,':',source_system)  as product_id_bk
        ,   source_system
        ,   itemno
        ,   unit
        ,   descrip
        ,   _rivery_run_id
        ,   _rivery_last_update
    from {{ref('vw_stg_poliqms_arinvt')}} 
)
select      
            {{ dbt_utils.surrogate_key(['product_id_bk'])}} as product_sk
        ,    arinvt.itemno                  as product_id
        ,    arinvt.descrip                 as product_desc
        ,    null                           as product_cat
        ,    null                           as product_sub_cat
        ,    null                           as product_core_tech
        ,    arinvt.unit                    as product_uom
        ,    arinvt.source_system           as source_system
        ,    arinvt._rivery_run_id          as batch_id
        ,    arinvt._rivery_last_update     as elt_load_datetime
        ,    sysdate()                      as elt_process_datetime
from arinvt
