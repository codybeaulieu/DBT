with src as (
    select 
        *   
         ,'{{source('poliqms','ship_to')}}' as etl_src_table_name
         ,'POLIQMS'                         as source_system
    from {{source('poliqms','ship_to')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by id order by _rivery_last_update desc) row_nbr
    from src
)
select
	id,
	arcusto_id,
	attn,
	addr1,
	addr2,
	addr3,
	city,
	state,
	country,
	zip,
	ship_time,
	tax_code_id,
	phone_number,
	ext,
	fax,
	prime_contact,
	freight_id,
	notes,
	ecode,
	eid,
	edate_time,
	ecopy,
	fob,
	use_usa_mask,
	eplant_id,
	days_margin,
	qty_pcnt_margin,
	locations_id,
	default_ship_to,
	cuser1,
	cuser2,
	vmi_invoice_on_ship,
	sun_ship,
	mon_ship,
	tue_ship,
	wed_ship,
	thu_ship,
	fri_ship,
	sat_ship,
	use_dockid_pickticket,
	dist_center_id,
	dockid,
	pool_code,
	bol_note,
	pk_tkt_keep_rel_separate,
	packslip_report,
	bol_report,
	bol_header_report,
	division_id,
	vmi_usesysdate_autoinv,
	cuser3,
	cuser4,
	cuser5,
	nuser1,
	nuser2,
	nuser3,
	nuser4,
	nuser5,
	no_printed_invoice,
	preferred_ship_day,
	pk_tkt_exclude_forecast_rel,
	supplier_code,
	billing_supplier_code,
	cuser6,
	cuser7,
	bill_to_id,
	match_release_based_on_ran,
	bol_all_ps_report,
	pk_hide,
	vmi_use_cons_date,
	trailer_id,
	comm_pcnt,
	commissions_id,
	cuser8,
	cuser9,
	cuser10,
	cuser11,
	cuser12,
	cuser13,
	cuser14,
	cuser15,
	consolidator_id,
	priority_level,
	coc_report,
	coc_required,
	importer_id,
	account_number,
	is_make_to_order,
	rebate_params_id,
	freight_rev_pcnt,
	fob_third_party_id,
	info_so,
	territory,
	direct_ship_locatons_id,
	commer_invoice_required,
	commer_invoice_report,
	reval_unit_price_inv_date,
	cust_type_id,
	ps_verify_inventory,
	territory_id,
	shipman_print_box_label,
	tax_exempt_num,
	arcusto_usage_type_id,
	country_id,
	state_id,
	price_book_id,
	price_book_price_type_id,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
    source_system
from deduped
where row_nbr = 1