with src as (
    select 
        *   
         ,'{{source('svmdeacom','dmgrp')}}' as etl_src_table_name
         ,'SVMDEACOM'                                as source_system
    from {{source('svmdeacom','dmgrp')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by gr_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	gr_id
 ,  gr_name
 ,  gr_active
 ,  gr_default
 ,  gr_pastday
 ,  gr_exday
 ,  gr_credlim
 ,  gr_exceed
 ,  gr_credhld
 ,  gr_collect
 ,  gr_lastcred
 ,  gr_credflag
 ,  gr_creddueshipdays
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,	etl_src_table_name
 ,	source_system
from deduped
where row_nbr = 1
