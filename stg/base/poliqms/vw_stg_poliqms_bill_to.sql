with src as (
    select 
        *   
         ,'{{source('poliqms','bill_to')}}' as etl_src_table_name
         ,'POLIQMS'                         as source_system
    from {{source('poliqms','bill_to')}}
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
	phone_number,
	ext,
	fax,
	prime_contact,
	notes,
	company_id,
	ecode,
	eid,
	edate_time,
	ecopy,
	use_usa_mask,
	default_bill_to,
	invoice_report,
	posted_invoice_report,
	pk_hide,
	terms_id,
	status_id,
	country_id,
	state_id,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
    source_system
from deduped
where row_nbr = 1