with src as (
    select 
        *   
         ,'{{source('poliqms','arinvoice')}}' as etl_src_table_name
         ,'POLIQMS'                           as source_system
    from {{source('poliqms','arinvoice')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by id order by _rivery_last_update desc) row_nbr
    from src
)
select
	id,
	glacct_id_ar,
	arcusto_id,
	bill_to_id,
	invoice_no,
	invoice_date,
	terms_id,
	due_date,
	notes,
	glperiods_id_ar,
	glbatchid_id,
	prior_entry,
	arcusto_company,
	arcusto_addr1,
	arcusto_addr2,
	arcusto_addr3,
	arcusto_city,
	arcusto_state,
	arcusto_country,
	arcusto_zip,
	bill_to_attn,
	bill_to_addr1,
	bill_to_addr2,
	bill_to_addr3,
	bill_to_city,
	bill_to_state,
	bill_to_country,
	bill_to_zip,
	terms_descrip,
	terms_days,
	terms_discount,
	terms_discount_days,
	arcusto_custno,
	cashrec_id,
	ecode,
	eid,
	edate_time,
	ecopy,
	inv_prefix,
	currency_id,
	glacct_id_fx,
	fx_rate,
	edi_created,
	currency_code,
	currency_descrip,
	arinvoice_id,
	userid,
	eplant_id,
	ctrl_eplant_id,
	print_count,
	print_last_user,
	print_last_date,
	exclude_from_fin_charge,
	cashrec_detail_id,
	arcusto_credit_card_id,
	arcusto_credit_card_acctno,
	arcusto_credit_card_name_on,
	arcusto_credit_card_exp_mmyy,
	fx_revalue_rate,
	dunning_seq,
	dunning_exclude,
	on_hold,
	num_of_supp_docs,
	cuser1,
	cuser2,
	cuser3,
	do_not_email_upon_post,
	invoicetotal,
	lefttoapply,
	cia_invoice,
	userid_posted,
	arprepost_id,
	tax_doc_uploaded,
	is_retro,
	arcusto_country_id,
	bill_to_country_id,
	arcusto_state_id,
	bill_to_state_id,
	debit,
	credit,
	native_debit,
	native_credit,
	trans_curr_code,
	native_curr_code,
	fx_revalue_period_id,
	ar_no_format_id,
	seq_no,
	tax_exempt_detail_id,
	ext_exemption_no,
	int_exemption_no,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
    source_system
from deduped
where row_nbr = 1