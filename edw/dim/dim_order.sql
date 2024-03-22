-- depends_on: {{ ref('elt_model_statistics') }}
{{
    config(
        unique_key= "order_sk"
    )
}}
select 
	    order_sk,
        source_system,
        order_id,
        order_status,
        batch_id,
        elt_load_datetime,
        elt_process_datetime
from {{ ref('elt_staging_dim_order') }}
