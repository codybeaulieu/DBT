with mdndetail as(
select concat(dispatchnote,':',source_system) as shipment_id_bk
    ,    concat(salesorder,':',source_system) as order_id_bk
    ,    concat(mstockcode,':',source_system) as product_id_bk
    ,    concat(dispatchnote,':',dispatchnoteline,':',source_system) as shipment_line_id_bk
    ,    salesorder
    ,    source_system
    ,    dispatchnote
    ,    dispatchnoteline
    ,    mlineshipdate
    ,    mqtytodispatch
    ,    mprice
    ,    MUnitCost
    ,    _rivery_run_id
    ,    _rivery_last_update
from {{ref('vw_stg_cmlsyspro_mdndetail')}}
),
mdnmaster as (
select     concat(customer,':',source_system) as customer_id_bk
    ,      concat(dispatchcustname,':',dispatchaddress5,':', dispatchaddress4,':',dispatchaddrloc,':',
                                       dispatchaddress1,':',dispatchaddress2,':',dispatchaddress3,':',
                                       dispatchpostalcode,':',source_system) as shipped_address_id_bk
    ,      dispatchnote
    ,      source_system
from {{ref('vw_stg_cmlsyspro_mdnmaster')}}
),
companies as (
select company_sk
    ,    company_abbr
from {{ref('vw_elt_staging_ref_companies')}}
where company_abbr = 'CML'
)
select   {{ dbt_utils.surrogate_key(['detail.shipment_id_bk'])}}            as shipment_sk
    ,    {{ dbt_utils.surrogate_key(['detail.order_id_bk'])}}               as order_sk
    ,    companies.company_sk                                               as company_sk
    ,    {{ dbt_utils.surrogate_key(['master.customer_id_bk'])}}            as customer_sk
    ,    {{ dbt_utils.surrogate_key(['master.shipped_address_id_bk','master.customer_id_bk'])}}     as shipped_address_sk
    ,    {{ dbt_utils.surrogate_key(['detail.product_id_bk'])}}             as product_sk
    ,    {{ dbt_utils.surrogate_key(['detail.shipment_line_id_bk'])}}       as shipment_line_sk
    ,    detail.source_system                                               as source_system
    ,    cast(detail.dispatchnote as string)                                as shipped_id
    ,    detail.dispatchnoteline                                            as shipped_line
    ,    detail.mlineshipdate                                               as shipped_date
    ,    detail.mqtytodispatch                                              as shipped_qty
    ,    detail.mprice                                                      as shipped_unit_price
    ,   ((detail.mprice - detail.MUnitCost) * detail.mqtytodispatch)        as shipped_extend_price
    ,    (detail.mprice*mqtytodispatch)                                     as shipped_line_amt
    ,    detail._rivery_run_id                                              as batch_id
    ,    detail._rivery_last_update                                         as elt_load_datetime
    ,    sysdate()                                                          as elt_process_datetime
from mdndetail detail left join mdnmaster master
on detail.dispatchnote = master.dispatchnote and
   detail.source_system = master.source_system
   left join companies