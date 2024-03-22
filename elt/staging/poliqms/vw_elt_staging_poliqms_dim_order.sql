select {{ dbt_utils.surrogate_key(['order_id_bk'])}} as order_sk
    ,   source_system
    ,   order_id
    ,   order_status
    ,   batch_id
    ,   elt_load_datetime
    ,   elt_process_datetime
from
(
select concat(ord.id,':',ord.source_system) as order_id_bk
        ,   ord.source_system               as source_system
        ,   ord.orderno                     as order_id
        ,   case
                when sum(zeroifnull(dtl.cumm_shipped)) = 0          then 'OPEN'
                when sum(dtl.cumm_shipped) < sum(dtl.total_qty_ord) then 'PARTIAL'
                when sum(dtl.cumm_shipped) = sum(dtl.total_qty_ord) then 'CLOSED'
                else                                                'Order Shipped More Than Ordered'
            end                             as order_status
        ,   ord._rivery_run_id              as batch_id
        ,   ord._rivery_last_update         as elt_load_datetime
        ,    sysdate()                      as elt_process_datetime
    from 
    {{ref('vw_stg_poliqms_orders')}} ord left join 
    {{ref('vw_stg_poliqms_ord_detail')}} dtl
    on ord.id = dtl.orders_id
    group by ord.orderno, ord.id, ord.source_system,ord._rivery_run_id,ord._rivery_last_update
)