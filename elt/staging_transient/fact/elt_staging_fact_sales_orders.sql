{{ dbt_utils.union_relations(
    relations = [
            ref('vw_elt_staging_secdeacom_fact_sales_orders'),
            ref('vw_elt_staging_svmdeacom_fact_sales_orders'),
            ref('vw_elt_staging_cmlsyspro_fact_sales_orders'),
            ref('vw_elt_staging_poliqms_fact_sales_orders')
            ],
    source_column_name=None
    )
}}