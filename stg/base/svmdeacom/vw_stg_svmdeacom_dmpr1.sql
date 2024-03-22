with src as (
    select 
        *   
         ,'{{source('svmdeacom','dmpr1')}}' as etl_src_table_name
         ,'SVMDEACOM'                                as source_system
    from {{source('svmdeacom','dmpr1')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by p1_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	p1_name
 ,  p1_id
 ,  p1_active
 ,  p1_default
 ,  p1_restricted
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,	etl_src_table_name
 ,  source_system
from deduped
where row_nbr = 1
