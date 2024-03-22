with sordetail as (
select 
        concat(salesorder,':',source_system) as order_id_bk
    ,    concat(mstockcode,':',source_system) as product_id_bk
    ,    concat(salesorder,':',salesorderline,':',source_system) as order_line_id_bk
    ,    salesorder
    ,    source_system
    ,    salesorderline
    ,    morderqty
    ,    munitcost
    ,    MPrice
    ,    _rivery_run_id
    ,    _rivery_last_update
from {{ref('vw_stg_cmlsyspro_sordetail')}}
),
sormaster as(
select concat(customer,':',source_system) as customer_id_bk
    ,    customer
    ,    source_system
    ,    salesorder
    ,    orderdate
from {{ref('vw_stg_cmlsyspro_sormaster')}}
),
cussordetmer as(
select   salesorder
    ,    salesorderinitline
    ,    customerlinerqstdt
    ,    source_system
from {{ref('vw_stg_cmlsyspro_cussordetailmerch_plus')}}
),
arcustomer as(
select concat(name,        ':', soldtoaddr5,   ':',soldtoaddr4, ':',
            soldtoaddr3loc,':', soldtoaddr1,   ':',soldtoaddr2,':', 
            soldtoaddr3,   ':', soldpostalcode,':',source_system) as billing_address_id_bk
        ,    customer
        ,    source_system
from {{ref('vw_stg_cmlsyspro_arcustomer')}}
),
companies as (
select company_sk
    ,    company_abbr
from {{ref('vw_elt_staging_ref_companies')}}
where company_abbr = 'CML'
)
select      {{ dbt_utils.surrogate_key(['sordetail.order_id_bk'])}}             as order_sk
    ,       company_sk                                                          as company_sk
    ,       {{ dbt_utils.surrogate_key(['sormaster.customer_id_bk'])}}          as customer_sk
    ,       {{ dbt_utils.surrogate_key(['arcustomer.billing_address_id_bk','sormaster.customer_id_bk'])}}  as billing_address_sk
    ,       {{ dbt_utils.surrogate_key(['sordetail.product_id_bk'])}}           as product_sk
    ,       {{ dbt_utils.surrogate_key(['sordetail.order_line_id_bk'])}}        as order_line_sk
    ,       sordetail.source_system                                             as source_system
    ,       sordetail.salesorderline                                            as order_line
    ,       sormaster.orderdate                                                 as order_date
    ,       cussordetmer.customerlinerqstdt::timestamp_ntz                      as order_wanted_date
    ,       sordetail.morderqty                                                 as order_qty
    ,       sordetail.munitcost                                                 as order_unit_price
    ,      ((sordetail.MPrice - sordetail.MUnitCost) * sordetail.MOrderQty)     as order_extend_price
    ,       (sordetail.morderqty * sordetail.munitcost)                         as order_line_amt
    ,       sordetail._rivery_run_id                                            as batch_id
    ,       sordetail._rivery_last_update                                       as elt_load_datetime
    ,       sysdate()                                                           as elt_process_datetime
from sordetail sordetail left join sormaster sormaster
on sordetail.salesorder = sormaster.salesorder and
   sordetail.source_system = sormaster.source_system
   left join cussordetmer cussordetmer
on sordetail.salesorder = cussordetmer.salesorder and
   sordetail.salesorderline = cussordetmer.salesorderinitline
   left join arcustomer arcustomer
on sormaster.customer = arcustomer.customer
   left join companies companies