with jobopening as (
    select distinct 
            concat(requisition_id,':','POL') as job_opening_id_bk,
            'POL' as company_code,
            source_system,
            job_title,
            requisition_id,
            requisition_status,
            posting_start_date::timestamp_ntz as posting_start_date,
            hiring_manager,
            requisition_closedfilled_date::timestamp_ntz as requisition_closedfilled_date,
            batch_id,
            elt_load_datetime
    from {{ref('vw_stg_amazons3_paycomjobopening')}}
    where requisition_id is not null
),comp as (
select   company_sk
    ,    company_abbr
from {{ref('vw_elt_staging_ref_companies')}}
where company_abbr = 'POL'
)
select  {{ dbt_utils.surrogate_key(['job_opening_id_bk'])}} as job_opening_sk,
        comp.company_sk                                     as company_sk,
        source_system                                       as source_system,
        job_title                                           as job_opportunity_title,
        requisition_id                                      as job_requisition_nbr,
        requisition_status                                  as job_opportunity_status,
        to_date(posting_start_date)::timestamp_ntz          as job_created_date,
        hiring_manager                                      as job_hiring_manager,
        to_date(requisition_closedfilled_date)::timestamp_ntz              as job_closed_date,
        null                                                as job_days_between_offer_and_start,
        batch_id                                            as batch_id,
        elt_load_datetime                                   as etl_load_datetime,
        sysdate()                                           as elt_process_datetime
from jobopening jobopening left join comp comp
     on substr(jobopening.company_code,1,3) = comp.company_abbr