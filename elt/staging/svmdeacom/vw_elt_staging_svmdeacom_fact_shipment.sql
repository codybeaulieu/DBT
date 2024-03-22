with dtord as (
  select  
		    concat(or_prid,':',source_system) as product_id_bk
		,    concat(or_id,':',source_system)  as shipment_line_id_bk
        ,    or_id
        ,    or_prid
		,	 or_ordnum
		,    or_linenum 
		,    or_wanted 
		,    or_quant
        ,    or_qship 
		,    or_price
        ,    or_exten  
        ,    or_toid  
		,	 source_system
		,	_rivery_run_id
        ,   _rivery_river_id
		,	_rivery_last_update
    from {{ref('vw_stg_svmdeacom_dtord')}}
),
dttord as (
	select 
		    concat(to_shid,':','SHIPPING',':',source_system) as shipped_address_id_bk
		,	concat(left(to_ordnum,9),':',source_system) as order_id_bk
		,	concat(to_ordnum,':',source_system) as shipment_id_bk
		,	to_orddate as order_date
		,	to_ordnum
        ,   to_orddate
		,	to_biid
        ,   to_shid
		,	to_shipped::timestamp_ntz as to_shipped
		,	to_ordtype
        ,   to_id
		,	source_system
        ,  row_number() over (partition by to_ordnum order by to_id desc) row_nbr
	from {{ref('vw_stg_svmdeacom_dttord')}}
    WHERE to_ordtype = 's'
),
xref as (
	select 
			concat(gr_id,':',source_system) as customer_id_bk
        ,   gr_id
		,	bi_id
        ,   sh_id
        ,   source_system
	from {{ref('vw_elt_staging_svmdeacom_customer_xref')}}
),
comp as (
    select 
        company_sk
    from {{ref('vw_elt_staging_ref_companies')}} 
    where company_abbr='SVM'
)
select     
         {{ dbt_utils.surrogate_key(['shipment_id_bk'])}}               as shipment_sk
    ,    {{ dbt_utils.surrogate_key(['order_id_bk'])}}                  as order_sk
    ,     comp.company_sk                                               as company_sk
    ,    {{ dbt_utils.surrogate_key(['customer_id_bk'])}}               as customer_sk
    ,    {{ dbt_utils.surrogate_key(['shipped_address_id_bk','customer_id_bk'])}}        as shipped_address_sk
    ,    {{ dbt_utils.surrogate_key(['product_id_bk'])}}                as product_sk
    ,    {{ dbt_utils.surrogate_key(['shipment_line_id_bk'])}}          as shipment_line_sk
    ,    dtord.source_system                                            as source_system
	,	 cast(dttord.to_ordnum as string)                               as shipped_id
    ,    dtord.or_linenum                                               as shipped_line
    ,    dttord.to_shipped                                              as shipped_date
    ,    dtord.or_qship                                                 as shipped_qty
    ,    dtord.or_price                                                 as shipped_unit_price
    ,    dtord.or_exten                                                 as shipped_extend_price
    ,    (dtord.or_qship * dtord.or_price)                              as shipped_line_amt
    ,    dtord._rivery_run_id                                           as batch_id
    ,    dtord._rivery_last_update                                      as elt_load_datetime
    ,    sysdate()                                                      as elt_process_datetime
from dttord dttord  left join dtord dtord
     on dttord.to_id = dtord.or_toid  left join xref xref 
    on dttord.to_biid = xref.bi_id and dttord.to_shid = xref.sh_id  
    left join  comp
where dttord.row_nbr = 1
and dttord.to_shipped is not null 