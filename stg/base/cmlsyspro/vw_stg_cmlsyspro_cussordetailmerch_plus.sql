with src as (
    select 
        *   
         ,'{{source('cmlsyspro','CusSorDetailMerch+')}}' as etl_src_table_name
         ,'CMLSYSPRO'                                as source_system
    from landing_db.cmlsyspro."CusSorDetailMerch+"
)
,deduped as (
    select
        *,
        row_number() over (partition by salesorder,salesorderinitline order by _rivery_last_update desc) row_nbr
    from src
)
select
	salesorder,
	salesorderinitline,
	invoicenumber,
	timestamp,
	origconfimshipdate,
	customerlinerqstdt,
	salesordertracking,
	allocationnotes,
	ontimereason,
	ontimenotes,
	nsavtx,
	entusecodlin,
	nstkwh,
	descriptionsamples,
	_rivery_river_id,
	_rivery_run_id,
	_rivery_last_update,
    etl_src_table_name,
 	source_system
from deduped
where row_nbr = 1
