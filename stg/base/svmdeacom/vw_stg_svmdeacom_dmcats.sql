with src as (
    select 
        *   
         ,'{{source('svmdeacom','dmcats')}}' as etl_src_table_name
         ,'SVMDEACOM'                                as source_system
    from {{source('svmdeacom','dmcats')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by ca_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	ca_id
 ,  ca_name
 ,  ca_active
 ,  ca_default
 ,  ca_quota
 ,  ca_restricted
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,	etl_src_table_name 
 ,  source_system 
from deduped
where row_nbr = 1
