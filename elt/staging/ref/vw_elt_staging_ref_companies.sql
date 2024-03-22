with ref_comp as (
    select 	concat(company, ':' , company_abbr) as company_id_bk
		,	company
		,	company_abbr
    from ref.companies
)
select       {{ dbt_utils.surrogate_key(['company_id_bk'])}} as company_sk
		,	company
		,	company_abbr
        ,    sysdate() as elt_process_datetime
from ref_comp 