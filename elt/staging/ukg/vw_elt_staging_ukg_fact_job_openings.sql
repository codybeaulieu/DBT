with jobopening as (
    select distinct 
            concat(requisition_number,':',company_code) as job_opening_id_bk,
            company_code,
            source_system,
            opportunity_title,
            requisition_number,
            opportunity_status,
            created_date,
            hiring_manager,
            closed_date,
            days_between_offer_and_start_dates,
            batch_id,
            elt_load_datetime
    from {{ref('vw_stg_amazons3_ukgjobopening')}}
    where requisition_number is not null
),comp as (
select company_sk
    ,    company_abbr
from {{ref('vw_elt_staging_ref_companies')}}
)
select  {{ dbt_utils.surrogate_key(['job_opening_id_bk'])}} as job_opening_sk,
        comp.company_sk                                     as company_sk,
        jobopening.source_system                            as source_system,
        opportunity_title                                   as job_opportunity_title,
        requisition_number                                  as job_requisition_nbr,
        opportunity_status                                  as job_opportunity_status,
        to_date(created_date,'mm/dd/yy')::timestamp_ntz     as job_created_date,
        hiring_manager                                      as job_hiring_manager,
        to_date(closed_date)::timestamp_ntz                 as job_closed_date,
        days_between_offer_and_start_dates                  as job_days_between_offer_and_start,
        batch_id                                            as batch_id,
        elt_load_datetime                                   as etl_load_datetime,
        sysdate()                                           as elt_process_datetime
from jobopening jobopening left join comp comp
     on substr(jobopening.company_code,1,3) = comp.company_abbr
