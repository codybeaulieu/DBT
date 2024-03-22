{{ dbt_utils.union_relations(
    relations = [
            ref('vw_elt_staging_secdeacom_fact_shipment'),
            ref('vw_elt_staging_svmdeacom_fact_shipment'),
            ref('vw_elt_staging_cmlsyspro_fact_shipment'),
            ref('vw_elt_staging_poliqms_fact_shipment')
            ],
    source_column_name=None
    )
}}