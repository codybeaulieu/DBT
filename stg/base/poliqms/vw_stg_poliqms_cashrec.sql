with src as (
    select 
        *   
         ,'{{source('poliqms','cashrec')}}' as etl_src_table_name
         ,'POLIQMS'                         as source_system
    from {{source('poliqms','cashrec')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by id order by _rivery_last_update desc) row_nbr
    from src
)
select
	id,
	ref_no,
	ref_date,
	deposit_date,
	ref_amount,
	bankinfo_dtl_id,
	glacct_id_cash,
	glbatchid_id,
	arcusto_id,
	ref_type,
	arcusto_custno,
	arcusto_company,
	arcusto_addr1,
	arcusto_addr2,
	arcusto_addr3,
	arcusto_city,
	arcusto_state,
	arcusto_country,
	arcusto_zip,
	glacct_acct,
	check_status,
	currency_id,
	currency_code,
	currency_descrip,
	fx_rate,
	eplant_id,
	bank_amount,
	exchange_rate,
	bankdep_currency_id,
	bankdep_currency_code,
	bankdep_currency_descrip,
	date_cleared,
	bank_charge,
	glacct_id_bank,
	comment1,
	bank_reconcile_id,
	arcusto_country_id,
	arcusto_state_id,
	reversed_cashrec_id,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
    source_system
from deduped
where row_nbr = 1