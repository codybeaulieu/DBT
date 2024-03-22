with src as (
    select 
        *   
         ,'{{source('svmdeacom','dmunit')}}' as etl_src_table_name
         ,'SVMDEACOM'                                as source_system
    from {{source('svmdeacom','dmunit')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by un_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	un_id
 ,  un_name
 ,  un_active
 ,  un_default
 ,  un_type
 ,  un_base
 ,  un_factor
 ,  un_shipmins
 ,  un_recmins
 ,  un_restcontunid
 ,  un_fedexfactor
 ,  un_fedexunit
 ,  un_edicode
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,	etl_src_table_name
 ,	source_system
from deduped
where row_nbr = 1