with src as (
    select 
        *   
         ,'{{source('svmdeacom','dttord')}}' as etl_src_table_name
         ,'SVMDEACOM'                                as source_system
    from {{source('svmdeacom','dttord')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by to_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	to_ordnum
 ,  to_ordtype
 ,  to_quoted
 ,  to_ordered
 ,  to_orddate
 ,  to_biid
 ,  to_shid
 ,  to_smid
 ,  to_brid
 ,  to_s1id
 ,  to_s2id
 ,  to_grid
 ,  to_billpo
 ,  to_shippo
 ,  to_prtpack
 ,  to_shipped
 ,  to_trid
 ,  to_teid
 ,  to_credhld
 ,  to_invdate
 ,  to_totdue
 ,  to_balance
 ,  to_paydate
 ,  to_notes
 ,  to_confirm
 ,  to_waid
 ,  to_header
 ,  to_descrip
 ,  to_prognum
 ,  to_user1
 ,  to_release
 ,  to_linkto
 ,  to_frid
 ,  to_expires
 ,  to_flush
 ,  to_trakid
 ,  to_trak2id
 ,  to_wanted
 ,  to_dueship
 ,  to_archid
 ,  to_discoun
 ,  to_remarks
 ,  to_history
 ,  to_usid
 ,  to_trid2
 ,  to_prepay
 ,  to_s3id
 ,  to_statax
 ,  to_loctax
 ,  to_sgid
 ,  to_prior
 ,  to_s4id
 ,  to_s5id
 ,  to_deltime
 ,  to_promise
 ,  to_condate
 ,  to_id
 ,  to_saved
 ,  to_status
 ,  to_savetime
 ,  to_minquan
 ,  to_tranwaid
 ,  to_tranrecv
 ,  to_fcid
 ,  to_fcrate
 ,  to_totwgt
 ,  to_pjid
 ,  to_saletax
 ,  to_cashsale
 ,  to_overcred
 ,  to_tendered
 ,  to_paysched
 ,  to_distance
 ,  to_auid
 ,  to_signature
 ,  to_recdate
 ,  to_dgid
 ,  to_psid
 ,  to_crosswaid
 ,  to_ccauth
 ,  to_authorize
 ,  to_authc3id
 ,  to_fcrate2
 ,  to_cpid
 ,  to_duedock
 ,  to_intransit
 ,  to_facilitypricing
 ,  to_pickuptime
 ,  to_antcash
 ,  to_credexp
 ,  to_cclast4
 ,  to_doid
 ,  to_trandoid
 ,  to_tottare
 ,  to_deldate
 ,  to_shipfromid
 ,  to_colldate
 ,  to_said
 ,  to_stagcnt
 ,  to_frominvjobnum
 ,  to_prtinv
 ,  to_invpost
 ,  to_coid
 ,  to_recurtype
 ,  to_recurinterval
 ,  to_cardtoken
 ,  to_prtpick
 ,  to_seasonal
 ,  to_easypostrateid
 ,  to_ccinvnum
 ,  to_masterordnum
 ,  to_rgid
 ,  to_ccauthdate
 ,  to_ttid
 ,  to_sodate
 ,  to_ruid
 ,  to_groupnum
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,   etl_src_table_name
 ,	source_system
from deduped
where row_nbr = 1
