-- depends_on: {{ ref('elt_model_statistics') }}
{{
    config(
        unique_key='company_sk'
    )
}}
select 
        company_sk, 
        company, 
        company_abbr, 
        elt_process_datetime
    from {{ ref('elt_staging_ref_companies') }}
