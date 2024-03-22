-- depends_on: {{ ref('elt_model_statistics') }}
{{
    config(
        unique_key=['job_opening_sk']
    )
}}
select 
	job_opening_sk, 
    company_sk, 
    source_system,
    job_opportunity_title, 
    job_requisition_nbr, 
    job_opportunity_status, 
    job_created_date, 
    job_hiring_manager, 
    job_closed_date, 
    job_days_between_offer_and_start,
    batch_id, 
    etl_load_datetime, 
    elt_process_datetime
from {{ ref('elt_staging_fact_job_openings') }}
