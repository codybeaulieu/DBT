{{ dbt_utils.union_relations(
    relations = [
            ref('vw_elt_staging_ukg_dim_employee'),
            ref('vw_elt_staging_paycom_dim_employee')       
            ],
    source_column_name=None
    )
}}