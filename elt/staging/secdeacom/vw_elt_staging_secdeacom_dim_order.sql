with dttord as (
    select 
         concat(left(to_ordnum,9) ,':', source_system) as order_id_bk
    ,    source_system    as source_system
    ,    left(to_ordnum,9)  as order_id
    ,    case 
            when ( right(to_ordnum,2) = '00' and to_shipped is null and to_ordtype = 's') then 'OPEN'
            when ( right(to_ordnum,2) <> '00' and to_shipped is null and to_ordtype = 's') then 'PARTIAL'
            when ( to_shipped is not null ) then 'CLOSED'
         end as order_status
    ,    _rivery_run_id as batch_id
    ,    _rivery_last_update as elt_load_datetime
    ,    sysdate() as elt_process_datetime
    ,   row_number() over (partition by left(to_ordnum,9)  order by to_id desc) as row_num
    from {{ ref('vw_stg_secdeacom_dttord') }}
	where to_ordtype = 's' 
)
select 
         {{ dbt_utils.surrogate_key(['order_id_bk'])}} as order_sk
    ,    source_system
    ,    order_id
    ,    order_status
    ,    batch_id
    ,    elt_load_datetime
    ,    elt_process_datetime
from dttord 
where row_num = 1 