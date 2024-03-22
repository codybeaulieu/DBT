-- depends_on: {{ ref('elt_model_statistics') }}
{{
    config(
        unique_key=['address_sk','customer_sk']
    )
}}
select 
	    address_sk,
        customer_sk,
        address_type,
        source_system,
        address_name,
        address_country,
        address_state,
        address_city,
        address_1,
        address_2,
        address_3,
        address_zip,
        batch_id,
        elt_load_datetime,
        elt_process_datetime
    from {{ ref('elt_staging_dim_customer_address') }}
