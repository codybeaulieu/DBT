with dtord as (
  select 	 
		     concat(or_prid,':',source_system) as product_id_bk
		,    concat(or_id,':',source_system)  as order_line_id_bk
        ,    or_id
        ,    or_prid
		,	 or_ordnum
		,    or_linenum 
		,    or_wanted 
		,    or_quant 
		,    or_price
        ,    or_exten  
        ,    or_toid  
		,	 source_system
		,	_rivery_run_id
        ,   _rivery_river_id
		,	_rivery_last_update
    from {{ref('vw_stg_secdeacom_dtord')}}
),
dttord as (
	select 
            concat(left(to_ordnum,9),':',source_system) as order_id_bk
		,   concat(to_biid,':','BILLING',':',source_system) as billing_address_id_bk
        ,	to_orddate as order_date
		,	to_ordnum
        ,   to_orddate
        ,   to_ordered::timestamp_ntz as to_ordered
		,	to_biid
        ,   to_shid
		,	to_ordtype
        ,   max(to_id) as to_id
		,	source_system
	from {{ref('vw_stg_secdeacom_dttord')}}
    where   to_ordtype = 's'  
            and right(to_ordnum,2) = '00'
    group by to_ordnum
        ,   to_orddate
        ,   to_ordered
		,	to_biid
        ,   to_shid
		,	to_ordtype
		,	source_system
),
xref as (
	select 
			concat(gr_id,':',source_system) as customer_id_bk
        ,   gr_id
		,	bi_id
        ,   sh_id
        ,   source_system
	from {{ref('vw_elt_staging_secdeacom_customer_xref')}}
),
comp as (
    select 
        company_sk
    from {{ref('vw_elt_staging_ref_companies')}} 
    where company_abbr='SEC'
)
select 
         {{ dbt_utils.surrogate_key(['order_id_bk'])}}                              as order_sk
    ,    comp.company_sk                                                            as company_sk
    ,    {{ dbt_utils.surrogate_key(['customer_id_bk'])}}                           as customer_sk
    ,    {{ dbt_utils.surrogate_key(['billing_address_id_bk','customer_id_bk'])}}   as billing_address_sk
    ,    {{ dbt_utils.surrogate_key(['product_id_bk'])}}                            as product_sk
    ,    {{ dbt_utils.surrogate_key(['order_line_id_bk'])}}                         as order_line_sk
    ,    dtord.source_system                                                        as source_system
    ,    dtord.or_linenum                                                           as order_line
    ,    dttord.to_ordered                                                          as order_date
    ,    dtord.or_wanted::timestamp_ntz                                             as order_wanted_date
    ,    dtord.or_quant                                                             as order_qty
    ,    dtord.or_price                                                             as order_unit_price
    ,    dtord.or_exten                                                             as order_extend_price
    ,    dtord.or_quant * dtord.or_price                                            as order_line_amt
    ,    dtord._rivery_run_id                                                       as batch_id
    ,    dtord._rivery_last_update                                                  as elt_load_datetime
    ,    sysdate()                                                                  as elt_process_datetime
from dttord dttord  left join dtord dtord
     on dttord.to_id = dtord.or_toid  left join xref xref 
     on dttord.to_biid = xref.bi_id and dttord.to_shid = xref.sh_id 
     left join  comp
