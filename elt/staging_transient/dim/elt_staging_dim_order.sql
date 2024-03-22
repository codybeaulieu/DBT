{{ dbt_utils.union_relations(
    relations = [
            ref('vw_elt_staging_secdeacom_dim_order'),
            ref('vw_elt_staging_svmdeacom_dim_order'),
            ref('vw_elt_staging_cmlsyspro_dim_order'),
            ref('vw_elt_staging_poliqms_dim_order')
            ],
    source_column_name=None
    )
}}