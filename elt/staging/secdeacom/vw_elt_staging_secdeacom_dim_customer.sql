with xref as (
    select 
			concat(gr_id , ':' , source_system) as customer_id_bk
		,	source_system
		,	gr_id
		,	gr_name
        ,   bi_name
		,	batch_id
		,	elt_load_datetime
    from {{ref('vw_elt_staging_secdeacom_customer_xref')}}
)
select      distinct  
            {{ dbt_utils.surrogate_key(['customer_id_bk'])}}   as customer_sk
        ,    xref.source_system                                 as source_system
        ,    cast(xref.gr_id as string)                         as customer_id
        ,    gr_name                                            as customer_name
        ,    xref.batch_id                                      as batch_id
        ,    xref.elt_load_datetime                             as elt_load_datetime
        ,    sysdate()                                          as elt_process_datetime
from xref xref