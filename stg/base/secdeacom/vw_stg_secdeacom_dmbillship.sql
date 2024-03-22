with src as (
    select 
        *   
         ,'{{source('secdeacom','dmbillship')}}' as etl_src_table_name
         ,'SECDEACOM'                                as source_system
    from {{source('secdeacom','dmbillship')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by bs_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	bs_id
 ,  bs_biid
 ,  bs_shid
 ,  bs_shipdefault
 ,  bs_billdefault
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,	etl_src_table_name
 ,	source_system
from deduped
where row_nbr = 1