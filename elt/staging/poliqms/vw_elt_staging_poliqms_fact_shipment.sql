with shipment as (
    select  id
        ,   arcusto_id
        ,   bill_to_id
        ,   concat(id,':',source_system) 				        as shipment_id_bk
        ,   concat(arcusto_id,':',source_system) 				as customer_id_bk
        ,   concat(ship_to_id,':','SHIPPING',':',source_system) as shipment_address_id_bk
        ,   packslipno                                          as shipped_id
        ,   shipdate
        ,   source_system
		,	_rivery_run_id						                as batch_id
		,	_rivery_last_update					                as elt_load_datetime
   from {{ref('vw_stg_poliqms_shipments')}}
),
ord_detail as
(
    select id
        ,   orders_id
        ,   arinvt_id
        ,   concat(orders_id,':',source_system) as order_id_bk
        ,   ord_det_seqno
        ,   unit_price
        ,   source_system
    from {{ref('vw_stg_poliqms_ord_detail')}}
),
shipment_dtl as
(
     select  concat(ps_ticket_dtl_arinvt_id,':',source_system) as product_id_bk
        ,   concat(id,':',source_system) as shipment_line_id_bk
        ,   qtyshipped
        ,   releases_id
        ,   shipments_id
        ,   source_system
    from {{ref('vw_stg_poliqms_shipment_dtl')}}
),
releases as (
    select  id
        ,   ord_detail_id
        ,   source_system
    from {{ref('vw_stg_poliqms_releases')}}
),
comp as (
    select 
        company_sk
    from {{ref('vw_elt_staging_ref_companies')}}
    where company_abbr='POL'
)
select {{ dbt_utils.surrogate_key(['sh.shipment_id_bk'])}}                              as shipment_sk
    ,  {{ dbt_utils.surrogate_key(['or_dtl.order_id_bk'])}}                             as order_sk
    ,  comp.company_sk					  									            as company_sk
    ,  {{ dbt_utils.surrogate_key(['sh.customer_id_bk'])}}				                as customer_sk
    ,  {{ dbt_utils.surrogate_key(['sh.shipment_address_id_bk','sh.customer_id_bk'])}}	as shipped_address_sk
    ,  {{ dbt_utils.surrogate_key(['sh_dtl.product_id_bk'])}}				            as product_sk
    ,  {{ dbt_utils.surrogate_key(['sh_dtl.shipment_line_id_bk'])}}		                as shipment_line_sk
    ,  sh_dtl.source_system				                                                as source_system
    ,  sh.shipped_id                                                                    as shipped_id
    ,  or_dtl.ord_det_seqno                                                             as shipped_line
    ,  sh.shipdate                                                                      as shipped_date
    ,  sh_dtl.qtyshipped                                                                as shipped_qty
    ,  or_dtl.unit_price 				                                                as shipped_unit_price
    ,  sh_dtl.qtyshipped * or_dtl.unit_price                                            as shipped_extend_price
    ,  sh_dtl.qtyshipped * or_dtl.unit_price                                            as shipped_line_amt
	,  sh.batch_id 					                                                    as batch_id
	,  sh.elt_load_datetime			                                                    as elt_load_datetime
	,  sysdate() 						                                                as elt_process_datetime
from shipment sh left join shipment_dtl sh_dtl
on sh.id = sh_dtl.shipments_id
left join releases rel
on sh_dtl.releases_id = rel.id
left join ord_detail or_dtl
on rel.ord_detail_id = or_dtl.id
left join comp