-- depends_on: {{ ref('elt_model_statistics') }}
{{
    config(
        unique_key= "product_sk"
    )
}}
select 
	    product_sk,
        product_id,
        product_desc,
        product_cat,
        product_sub_cat,
        product_core_tech,
        product_uom,
        source_system,
        batch_id,
        elt_load_datetime,
        elt_process_datetime
from {{ ref('elt_staging_dim_product') }}
