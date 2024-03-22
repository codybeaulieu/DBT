with src as (
    select 
        *   
         ,'{{source('poliqms','arcusto')}}' as etl_src_table_name
         ,'POLIQMS'                         as source_system
    from {{source('poliqms','arcusto')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by id order by _rivery_last_update desc) row_nbr
    from src
)
select
	id,
	ter_id,
	custno,
	cust_group,
	company,
	addr1,
	addr2,
	addr3,
	city,
	state,
	country,
	zip,
	phone_number,
	ext,
	fax_number,
	prime_contact,
	cust_since,
	ytd_sales,
	territory,
	industry,
	finance_charge,
	terms_id,
	credit_limit,
	status_id,
	status_date,
	statements,
	cuser1,
	cuser2,
	cuser3,
	nuser1,
	nuser2,
	nuser3,
	comm_pcnt,
	source,
	tax_codes_id,
	salespeople_id,
	note,
	discount,
	ecode,
	eid,
	edate_time,
	ecopy,
	cust_type_id,
	cuser4,
	cuser5,
	cuser6,
	nuser4,
	nuser5,
	currency_id,
	autoinvoice_technique,
	ps_date_on_invoice,
	commissions_id,
	glyear_id,
	highest_bal_bucket,
	use_usa_mask,
	statement_date,
	last_finance_charge_date,
	min_change_interval,
	alertmsg,
	crm_prospect,
	pk_hide,
	web_site_url,
	one_po_per_ps,
	overlay_label_report,
	before_overlay_label_report,
	safety_lead_time,
	ship_forecast,
	qty_price_break,
	packslip_report,
	ord_ack_report,
	invoice_report,
	qletter_report,
	tl_rfq_report,
	bol_report,
	bol_header_report,
	jobshop_report,
	one_so_per_ps,
	ar_glacct_id,
	dunning_group_id,
	dunning_exclude,
	info_so,
	distlist,
	ps_preserve_ran,
	is_intercomp,
	bol_all_ps_report,
	coc_required,
	vendor_id_outsource,
	trailer_over_max_instructions,
	trailer_max_length,
	pallet_max_height,
	pallet_min_height,
	is_overhang_allowed,
	pallet_overhang_limit,
	pallet_size_required,
	pallet_grade_required,
	pallet_max_floor_spaces,
	pallet_max_weight,
	is_pallet_labels_required,
	pallet_label_placement,
	carton_max_height,
	carton_max_length,
	carton_max_width,
	is_floor_loading_allowed,
	carton_max_floor_loading,
	is_routing_required,
	routing_time_deadline,
	ship_to_prioritized,
	trailer_min_cube,
	trailer_max_cube,
	pallet_type,
	carton_labels_required,
	carton_label_type,
	carton_label_size,
	carton_label_placement,
	posted_invoice_report,
	cust_credit_days,
	cust_credit_incl_ship,
	cust_credit_incl_so,
	use_discount_tier,
	coc_report,
	is_make_to_order,
	eplant_id,
	credit_limit_autocalc,
	tracking_required,
	ps_convert_info,
	tax_id,
	info_cr,
	crm_quote_report,
	info_ar,
	rebate_params_id,
	web_payment_type,
	csr_pr_emp_id,
	ar_pr_emp_id,
	ref_type,
	created,
	createdby,
	changed,
	changedby,
	cc_retention_days,
	drop_ship_vendor_id,
	webuser_name,
	webuser_log,
	prime_contact_email,
	use_external_labels,
	ps_verify_inventory,
	arcusto_level_id,
	commer_invoice_required,
	commer_invoice_report,
	credit_limit_auto_update,
	credit_limit_past_due_days,
	territory_id,
	crm_opportunity_id,
	sic_code,
	hyperlink,
	hyperlinkaddress,
	hyperlinksubaddress,
	ref_code_id,
	vat_tax_no,
	freight_id,
	account_number,
	ship_complete,
	cash_in_advance,
	overship,
	undership,
	use_discount_params,
	arcusto_usage_type_id,
	tax_exempt_num,
	rev_disc_order,
	gl_plug_value,
	so_discount_type_id,
	country_id,
	state_id,
	price_book_id,
	price_book_price_type_id,
	ds_source,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
    source_system
from deduped
where row_nbr = 1