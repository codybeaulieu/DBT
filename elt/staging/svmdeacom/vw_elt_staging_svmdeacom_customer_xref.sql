with dmgrp as (
    select  distinct 
			gr_id
		,	gr_name
		,	source_system
		,	_rivery_run_id
		,	_rivery_last_update
    from {{ref('vw_stg_svmdeacom_dmgrp')}}
),
dmbill as (
	select 	distinct 
            bi_id
		,	bi_name
		,	bi_grid
		,	source_system
	from {{ref('vw_stg_svmdeacom_dmbill')}}
),
dmbillship as (
	select  distinct 
            bs_biid
		,	bs_shid
		,	source_system
	from {{ref('vw_stg_svmdeacom_dmbillship')}}
),
dmship as (
	select  distinct
            sh_id
		,	sh_name
		,	source_system
	from {{ref('vw_stg_svmdeacom_dmship')}}
)
select 
		distinct 
        case 
            when dmgrp.gr_name = 'None' then dmbill.bi_id||left(dmbill.bi_name,5)
        else cast(dmgrp.gr_id as string)
        end
        as gr_id
	, 	case 
            when dmgrp.gr_name = 'None' then bi_name
        else dmgrp.gr_name
        end
        as gr_name
	,   dmbill.bi_id	as bi_id
	,   dmbill.bi_name	as bi_name
	,   dmship.sh_id	as sh_id
	,   dmship.sh_name 	as sh_name
	,   dmgrp.source_system as source_system
	,   dmgrp._rivery_run_id as batch_id
    ,   dmgrp._rivery_last_update as elt_load_datetime
    ,   sysdate() as elt_process_datetime
from dmgrp dmgrp 
left outer join dmbill  dmbill 
on dmgrp.gr_id = dmbill.bi_grid
left outer join dmbillship dmbillship 
on dmbill.bi_id = dmbillship.bs_biid
left outer join dmship dmship 
on dmbillship.bs_shid = dmship.sh_id
