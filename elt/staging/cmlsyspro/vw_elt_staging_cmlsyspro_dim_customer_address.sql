select * from (
select *,  row_number() over (partition by address_sk,customer_sk order by elt_load_datetime desc) row_nbr from 
{{ref('vw_elt_staging_cmlsyspro_dim_customer_address_duplicate')}}
) where row_nbr = 1