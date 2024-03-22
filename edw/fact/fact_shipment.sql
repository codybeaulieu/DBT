-- depends_on: {{ ref('elt_model_statistics') }}
{{
    config(
        unique_key=['shipment_sk','shipment_line_sk']
    )
}}
select 
	shipment_sk,
	order_sk,
	company_sk,
	customer_sk,
	shipped_address_sk,
	product_sk,
	shipment_line_sk,
	source_system,
	shipped_id,
	shipped_line,
	shipped_date,
	shipped_qty,
	shipped_unit_price,
    shipped_extend_price,
	shipped_line_amt,
	batch_id,
	elt_load_datetime,
    elt_process_datetime
from {{ ref('elt_staging_fact_shipment') }}