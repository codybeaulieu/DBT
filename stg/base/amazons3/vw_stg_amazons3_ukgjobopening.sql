with src as (
    select 
        *   
         ,'landing_db.amazons3.paycomjobopening' as etl_src_table_name
         ,'UKG'                                  as source_system
         ,sysdate()                              as elt_load_datetime
    from {{source('amazons3','ukgjobopening')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by requisition_number order by created_date desc) row_nbr
    from src
)
,batch_id as (
select uuid_string() as batch_id
)
select
	company_code, 
    opportunity_title, 
    requisition_number, 
    opportunity_status, 
    created_date, 
    first_published_date, 
    hiring_manager, 
    closed_date, 
    days_between_offer_and_start_dates, 
    closing_reason_name
 	etl_src_table_name,
 	source_system,
    elt_load_datetime,
    batch_id.batch_id
from deduped left join batch_id batch_id
where row_nbr = 1