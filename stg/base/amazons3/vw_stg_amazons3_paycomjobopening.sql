with src as (
    select 
        *   
         ,'landing_db.amazons3.paycomjobopening' as etl_src_table_name
         ,'PAYCOM'                               as source_system
         ,sysdate()                              as elt_load_datetime
    from {{source('amazons3','paycomjobopening')}} 
)
,deduped as (
    select
        *,
        row_number() over (partition by requisition_id order by posting_start_date desc) row_nbr
    from src
)
,batch_id as (
select uuid_string() as batch_id
)
select
	job_location, 
    job_title, 
    requisition_id, 
    requisition_status, 
    created_date, 
    posting_start_date, 
    hiring_manager, 
    requisition_closedfilled_date,
 	etl_src_table_name,
 	source_system,
    elt_load_datetime,
    batch_id
from deduped left join batch_id 
where row_nbr = 1
