with src as (
    select 
        *   
         ,'{{source('poliqms','shipment_dtl')}}' as etl_src_table_name
         ,'POLIQMS'                              as source_system
    from {{source('poliqms','shipment_dtl')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by id order by _rivery_last_update desc) row_nbr
    from src
)
select
	id,
	releases_id,
	shipments_id,
	order_dtl_id,
	qtyshipped,
	backorder_qty,
	qty_of_cartons,
	qty_per_carton,
	uom,
	cmtline,
	shipment_type,
	from_rma,
	ps_ticket_releases_id,
	vmi_fgmulti_id,
	vmi_consumed,
	ps_ticket_dtl_id,
	vmi_reference,
	cumm_shipped,
	vmi_return,
	ran,
	division_id,
	division_name,
	ict_arinvt_id,
	phantom_ps_ticket_dtl_id,
	ps_ticket_dtl_arinvt_id,
	phantom_ord_detail_id,
	vmi_cons_date,
	overship_shipment_dtl_id,
	qtyshipped_native,
	in_transit,
	in_transit_origin,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
    source_system
from deduped
where row_nbr = 1