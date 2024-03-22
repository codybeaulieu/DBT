with src as (
    select 
        *   
         ,'{{source('secdeacom','dmcats2')}}' as etl_src_table_name
         ,'SECDEACOM'                                as source_system
    from {{source('secdeacom','dmcats2')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by c2_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	c2_id, 
	c2_name, 
	c2_active, 
	c2_caid, 
	c2_quota, 
	c2_restricted, 
	_rivery_river_id, 
	_rivery_run_id, 
	_rivery_last_update,
	etl_src_table_name,
	source_system
from deduped
where row_nbr = 1