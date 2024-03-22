{{ dbt_utils.union_relations(
    relations = [
            ref('vw_elt_staging_ukg_fact_job_openings'),
            ref('vw_elt_staging_paycom_fact_job_openings')
            ],
    source_column_name=None
    )
}}