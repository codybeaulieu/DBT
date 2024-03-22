with src as (
    select 
        *   
         ,'{{source('ukg','persondetails')}}' as etl_src_table_name
         ,'UKG'                               as source_system
         ,sysdate()                           as elt_load_datetime
    from {{source('ukg','persondetails')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by employeeid order by dateofbirth desc) row_nbr
    from src
),batch_id as (
select uuid_string() as batch_id
)
select
	employeeid, 
    username, 
    firstname, 
    middlename, 
    lastname, 
    dateofbirth, 
    gender,
    etl_src_table_name,
 	source_system,
    elt_load_datetime,
    batch_id
from deduped left join batch_id 
where row_nbr = 1
