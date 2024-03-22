-- depends_on: {{ ref('elt_model_statistics') }}
{{
    config(
        unique_key=['customer_sk']
    )
}}
select 
	    customer_sk
    ,  source_system
    ,  customer_id
    ,  customer_name
    ,  batch_id
    ,  elt_load_datetime
    ,  elt_process_datetime
from {{ ref('elt_staging_dim_customer') }}
