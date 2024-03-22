with src as (
    select 
        *   
         ,'{{source('poliqms','aka')}}' as etl_src_table_name
         ,'POLIQMS'                     as source_system
    from {{source('poliqms','aka')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by id order by _rivery_last_update desc) row_nbr
    from src
)
select
	id,
    arinvt_id,
    cust_itemno,
    cust_descrip,
    arcusto_id,
    price_per_1000,
    comis_prcnt,
    currency_id,
    ecode,
    eid,
    edate_time,
    ecopy,
    cuser1,
    cuser2,
    cuser3,
    cuser4,
    cuser5,
    nuser1,
    nuser2,
    nuser3,
    nuser4,
    nuser5,
    lm_labels_id,
    cust_rev,
    info_so,
    ship_to_id,
    commissions_id,
    nuser6,
    nuser7,
    nuser8,
    nuser9,
    nuser10,
    is_drop_ship,
    pallet_pattern_id,
    cust_descrip2,
    cuser6,
    cuser7,
    cuser8,
    cuser9,
    cuser10,
    rebate_params_id,
    use_lot_charge,
    lot_charge,
    kind,
    standard_id,
    lead_days,
    tote_qty,
    ilps_aka_group_id,
    uom,
    multiple,
    min_sell_qty,
    plt_wrp_use_qc,
    plt_wrp_loc_id,
    _rivery_river_id,
    _rivery_run_id,
    _rivery_last_update,
    etl_src_table_name,
    source_system
from deduped
where row_nbr = 1