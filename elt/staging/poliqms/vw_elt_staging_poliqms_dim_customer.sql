with arcusto as (
    select distinct
			concat(id , ':' , source_system) as customer_id_bk
		,	source_system
		,	custno
		,	company
		,	_rivery_run_id                   as batch_id
		,	_rivery_last_update              as elt_load_datetime
    from SOLESIS_DWH_DEV_KMURTHY.stg.vw_stg_poliqms_arcusto
)
select  distinct      
            {{ dbt_utils.surrogate_key(['customer_id_bk'])}}   as customer_sk
        ,    arcusto.source_system                             as source_system
        ,    trim(cast(arcusto.custno as string))              as customer_id
        ,    trim(arcusto.company)                             as customer_name
        ,    arcusto.batch_id                                  as batch_id
        ,    arcusto.elt_load_datetime                         as elt_load_datetime
        ,    sysdate()                                         as elt_process_datetime
from arcusto arcusto