with src as (
    select 
        *   
         ,'{{source('poliqms','releases')}}' as etl_src_table_name
         ,'POLIQMS'                          as source_system
    from {{source('poliqms','releases')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by id order by _rivery_last_update desc) row_nbr
    from src
)
select
	id,
	ord_detail_id,
	request_date,
	seq,
	promise_date,
	quan,
	forecast,
	ship_date,
	cuser1,
	unit_price,
	ecode,
	eid,
	edate_time,
	ecopy,
	user_date,
	shipped_crw,
	orig_quan,
	ran,
	ship_to_id,
	shipped_quan,
	date_type,
	cuser2,
	batch_no,
	job_seq,
	must_ship_date,
	ack,
	user_date_2,
	user_date_3,
	internal_update_type,
	lock_must_ship_date,
	cuser3,
	freight_id,
	wo_note,
	phantom_releases_id,
	pending_verification,
	inventory_verified_date,
	exclude_sched,
	expedite,
	cuser4,
	cuser5,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
    source_system
from deduped
where row_nbr = 1