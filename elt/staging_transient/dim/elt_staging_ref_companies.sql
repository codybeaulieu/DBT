{{ dbt_utils.union_relations(
    relations = [
            ref('vw_elt_staging_ref_companies')
            ],
    source_column_name=None
    )
}}