with orders as (
    select  id
        ,   arcusto_id
        ,   bill_to_id
        ,   concat(arcusto_id,':',source_system) 				as customer_id_bk
        ,   concat(bill_to_id,':','BILLING',':',source_system)  as billing_address_id_bk
        ,   ord_date
        ,   source_system
    from {{ref('vw_stg_poliqms_orders')}}
),
detail as
(
    select id
        ,   orders_id
        ,   arinvt_id
        ,   concat(orders_id,':',source_system) as order_id_bk
        ,   concat(arinvt_id,':',source_system) as product_id_bk
        ,   concat(id,':',source_system) 		as order_line_id_bk
        ,   ord_det_seqno
        ,   total_qty_ord
        ,   unit_price
        ,   source_system
		,	_rivery_run_id						as batch_id
		,	_rivery_last_update					as elt_load_datetime
    from {{ref('vw_stg_poliqms_ord_detail')}}
),
releases as (
    select ord_detail_id
        ,   min(request_date) as request_date
        ,   source_system
    from {{ref('vw_stg_poliqms_releases')}}
    group by ord_detail_id, source_system
),
comp as (
    select 
        company_sk
    from {{ref('vw_elt_staging_ref_companies')}} 
    where company_abbr='POL'
)
select {{ dbt_utils.surrogate_key(['dtl.order_id_bk'])}}                    as order_sk
    ,  comp.company_sk					  									as company_sk
    ,  {{ dbt_utils.surrogate_key(['ord.customer_id_bk'])}}				  	as customer_sk
    ,  {{ dbt_utils.surrogate_key(['ord.billing_address_id_bk','ord.customer_id_bk'])}}	as billing_address_sk
    ,  {{ dbt_utils.surrogate_key(['dtl.product_id_bk'])}}				  	as product_sk
    ,  {{ dbt_utils.surrogate_key(['dtl.order_line_id_bk'])}}				as order_line_sk
    ,  dtl.source_system				  as source_system
    ,  dtl.ord_det_seqno				  as order_line
    ,  ord.ord_date						  as order_date
    ,  rel.request_date::timestamp_ntz    as order_wanted_date
    ,  dtl.total_qty_ord				  as order_qty
    ,  dtl.unit_price 					  as order_unit_price
	,  dtl.total_qty_ord * dtl.unit_price as order_extended_price
	,  dtl.total_qty_ord * dtl.unit_price as order_line_amt
	,  dtl.batch_id 					  as batch_id
	,  dtl.elt_load_datetime			  as elt_load_datetime
	,  sysdate() 						  as elt_process_datetime
from detail dtl left join orders ord
on dtl.orders_id = ord.id
left join releases rel 
on dtl.id = rel.ord_detail_id
left join comp