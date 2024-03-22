-- depends_on: {{ ref('elt_model_statistics') }}
{{
    config(
        unique_key=['order_sk','order_line_sk']
    )
}}
select 
	order_sk,
	company_sk,
	customer_sk,
	billing_address_sk,
	product_sk,
	order_line_sk,
	source_system,
	order_line,
	order_date,
	order_wanted_date,
	order_qty,
	order_unit_price,
    order_extend_price,
	order_line_amt,
	batch_id,
	elt_load_datetime,
	elt_process_datetime
from {{ ref('elt_staging_fact_sales_orders') }}