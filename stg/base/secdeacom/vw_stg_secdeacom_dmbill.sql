with src as (
    select 
        *   
         ,'{{source('secdeacom','dmbill')}}' as etl_src_table_name
         ,'SECDEACOM'                                as source_system
    from {{source('secdeacom','dmbill')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by bi_id order by _rivery_last_update desc) row_nbr
    from src
)
select
	bi_name
 ,  bi_id
 ,  bi_grid
 ,  bi_street
 ,  bi_street2
 ,  bi_city
 ,  bi_state
 ,  bi_zip
 ,  bi_phone
 ,  bi_fax
 ,  bi_contact
 ,  bi_credlim
 ,  bi_teid
 ,  bi_credhld
 ,  bi_active
 ,  bi_notes
 ,  bi_collect
 ,  bi_country
 ,  bi_ccode
 ,  bi_user1
 ,  bi_brid
 ,  bi_smid
 ,  bi_s1id
 ,  bi_s2id
 ,  bi_trid
 ,  bi_waid
 ,  bi_statax
 ,  bi_loctax
 ,  bi_highcrd
 ,  bi_county
 ,  bi_custid
 ,  bi_email
 ,  bi_poreqd
 ,  bi_frid
 ,  bi_pastday
 ,  bi_service
 ,  bi_s3id
 ,  bi_webname
 ,  bi_webpass
 ,  bi_backord
 ,  bi_dba
 ,  bi_s4id
 ,  bi_s5id
 ,  bi_credmast
 ,  bi_phext
 ,  bi_lastcred
 ,  bi_nextact
 ,  bi_nextdate
 ,  bi_exid
 ,  bi_dear
 ,  bi_said
 ,  bi_fcid
 ,  bi_popup
 ,  bi_invdest
 ,  bi_statedest
 ,  bi_psid
 ,  bi_quota
 ,  bi_exempt
 ,  bi_exceed
 ,  bi_exday
 ,  bi_credflag
 ,  bi_dgid
 ,  bi_pomask
 ,  bi_shelfpct
 ,  bi_archid
 ,  bi_mobileid
 ,  bi_popupship
 ,  bi_trakid
 ,  bi_trak2id
 ,  bi_shelfdays
 ,  bi_pfuser
 ,  bi_sotrakid
 ,  bi_posprice
 ,  bi_exreserve
 ,  bi_routeacct
 ,  bi_shortship
 ,  bi_pfid
 ,  bi_nopospay
 ,  bi_reqcpart
 ,  bi_availall
 ,  bi_street3
 ,  bi_ccid
 ,  bi_caid
 ,  bi_pdid
 ,  bi_restrictshipfrom
 ,  bi_noinvdflt
 ,  bi_rebill
 ,  bi_rebillworkflow
 ,  bi_shortpayprid
 ,  bi_noreserve
 ,  bi_cardvaultid
 ,  bi_retattrib1
 ,  bi_retattrib2
 ,  bi_retattrib3
 ,  bi_retdates
 ,  bi_laststateprint
 ,  bi_creddueshipdays
 ,  bi_ttid
 ,  bi_addressvalid
 ,  bi_edibilltopo
 ,  bi_edibilltopodays
 ,  bi_c3id
 ,  bi_emailtype
 ,  bi_linkedjobfinish
 ,  bi_vatid
 ,  bi_cyid
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,	etl_src_table_name
 ,	source_system
from deduped
where row_nbr = 1