with src as (
    select 
        *   
         ,'{{source('svmdeacom','dmship')}}' as etl_src_table_name
         ,'SVMDEACOM'                                as source_system
    from {{source('svmdeacom','dmship')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by sh_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	sh_name
 ,  sh_biid
 ,  sh_id
 ,  sh_brid
 ,  sh_s1id
 ,  sh_s2id
 ,  sh_smid
 ,  sh_street
 ,  sh_street2
 ,  sh_city
 ,  sh_state
 ,  sh_zip
 ,  sh_phone
 ,  sh_fax
 ,  sh_contact
 ,  sh_trid
 ,  sh_waid
 ,  sh_active
 ,  sh_statax
 ,  sh_loctax
 ,  sh_notes
 ,  sh_country
 ,  sh_ccode
 ,  sh_default
 ,  sh_county
 ,  sh_custid
 ,  sh_email
 ,  sh_frid
 ,  sh_s3id
 ,  sh_webname
 ,  sh_webpass
 ,  sh_s4id
 ,  sh_s5id
 ,  sh_credlim
 ,  sh_pastday
 ,  sh_credhld
 ,  sh_collect
 ,  sh_teid
 ,  sh_phext
 ,  sh_lastcred
 ,  sh_nextact
 ,  sh_nextdate
 ,  sh_exid
 ,  sh_dear
 ,  sh_said
 ,  sh_tranwaid
 ,  sh_fcid
 ,  sh_pjid
 ,  sh_popup
 ,  sh_quota
 ,  sh_exempt
 ,  sh_service
 ,  sh_exceed
 ,  sh_exday
 ,  sh_credflag
 ,  sh_dgid
 ,  sh_psid
 ,  sh_poreqd
 ,  sh_pomask
 ,  sh_waretaxover
 ,  sh_shelfpct
 ,  sh_popupship
 ,  sh_dba
 ,  sh_trakid
 ,  sh_trak2id
 ,  sh_shelfdays
 ,  sh_sotrakid
 ,  sh_prior
 ,  sh_crosswaid
 ,  sh_bomon
 ,  sh_bofulltr
 ,  sh_bofullpl
 ,  sh_botue
 ,  sh_bowed
 ,  sh_bothu
 ,  sh_bofri
 ,  sh_bosat
 ,  sh_bosun
 ,  sh_exreserve
 ,  sh_routeacct
 ,  sh_latitude
 ,  sh_longitude
 ,  sh_shortship
 ,  sh_reqdsdsig
 ,  sh_shipzone
 ,  sh_reqcpart
 ,  sh_availall
 ,  sh_street3
 ,  sh_ccid
 ,  sh_prtdgrpto
 ,  sh_caid
 ,  sh_noinvdflt
 ,  sh_noreserve
 ,  sh_retattrib1
 ,  sh_retattrib2
 ,  sh_retattrib3
 ,  sh_retdates
 ,  sh_laststateprint
 ,  sh_creddueshipdays
 ,  sh_ttid
 ,  sh_addressvalid
 ,  sh_svctype
 ,  sh_edishiptopo
 ,  sh_edishiptopodays
 ,  sh_retainreservedbo
 ,  sh_exemptexpires
 ,  sh_linkedjobfinish
 ,  sh_vatid
 ,  sh_cyid
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,	etl_src_table_name
 ,	source_system
from deduped
where row_nbr = 1
