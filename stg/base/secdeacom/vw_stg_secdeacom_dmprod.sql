with src as (
    select 
        *   
         ,'{{source('secdeacom','dmprod')}}' as etl_src_table_name
         ,'SECDEACOM'                                as source_system
    from {{source('secdeacom','dmprod')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by pr_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	pr_id
 ,  pr_codenum
 ,  pr_descrip
 ,  pr_level
 ,  pr_buid
 ,  pr_caid
 ,  pr_lispric
 ,  pr_stanlab
 ,  pr_stanmat
 ,  pr_stantot
 ,  pr_active
 ,  pr_taxable
 ,  pr_unitwgt
 ,  pr_ware1
 ,  pr_control
 ,  pr_drwcode
 ,  pr_reorder
 ,  pr_salable
 ,  pr_purable
 ,  pr_stocked
 ,  pr_abc
 ,  pr_user1
 ,  pr_user2
 ,  pr_user3
 ,  pr_user4
 ,  pr_notes
 ,  pr_make
 ,  pr_ordtype
 ,  pr_frtclas
 ,  pr_retail
 ,  pr_burden
 ,  pr_prunid
 ,  pr_prfact
 ,  pr_scrap
 ,  pr_purpric
 ,  pr_c2id
 ,  pr_discoun
 ,  pr_unquant
 ,  pr_singord
 ,  pr_chid
 ,  pr_invchid
 ,  pr_matexp
 ,  pr_invadj
 ,  pr_cogpro
 ,  pr_rdid
 ,  pr_unitvol
 ,  pr_unitcub
 ,  pr_reorder2
 ,  pr_puradj
 ,  pr_hazard
 ,  pr_matbur
 ,  pr_buracct
 ,  pr_salunid
 ,  pr_salfact
 ,  pr_finmat
 ,  pr_finlab
 ,  pr_finbur
 ,  pr_specpar
 ,  pr_density
 ,  pr_counted
 ,  pr_cntflag
 ,  pr_fixstan
 ,  pr_invgain
 ,  pr_user5
 ,  pr_user6
 ,  pr_neginv
 ,  pr_maxquan1
 ,  pr_maxquan2
 ,  pr_fixmat
 ,  pr_fixlab
 ,  pr_fixbur
 ,  pr_fixmbur
 ,  pr_fixupdt
 ,  pr_mrp
 ,  pr_catch
 ,  pr_catchwgt
 ,  pr_tranvar
 ,  pr_xferexp
 ,  pr_loadcalc1
 ,  pr_loadcalc2
 ,  pr_loadcalc3
 ,  pr_burcalc
 ,  pr_matburcalc
 ,  pr_purtype
 ,  pr_taxpo
 ,  pr_popso
 ,  pr_poppo
 ,  pr_custinv
 ,  pr_qcid
 ,  pr_quota
 ,  pr_lifocost
 ,  pr_tgid
 ,  pr_user7
 ,  pr_user8
 ,  pr_user9
 ,  pr_rdid2
 ,  pr_secure
 ,  pr_hazflag
 ,  pr_stanfrt
 ,  pr_fixfrt
 ,  pr_frtchid
 ,  pr_phid
 ,  pr_shelf
 ,  pr_commable
 ,  pr_finwip
 ,  pr_custreq
 ,  pr_poquan
 ,  pr_soquan
 ,  pr_jobquan
 ,  pr_makeord
 ,  pr_inherit
 ,  pr_minmar
 ,  pr_tarmar
 ,  pr_msfactor
 ,  pr_allowbom
 ,  pr_prtlabel
 ,  pr_futmat
 ,  pr_futlab
 ,  pr_futbur
 ,  pr_futmbur
 ,  pr_futfrt
 ,  pr_futstan
 ,  pr_timemrp
 ,  pr_nosub
 ,  pr_finpart
 ,  pr_purunid
 ,  pr_unid
 ,  pr_lotreqd
 ,  pr_orddays
 ,  pr_qcfreq
 ,  pr_lotrecv
 ,  pr_vendreq
 ,  pr_cofaid
 ,  pr_polabid
 ,  pr_solabid
 ,  pr_itemlabid
 ,  pr_msdsid
 ,  pr_joblabid
 ,  pr_finback
 ,  pr_lotlabid
 ,  pr_markup
 ,  pr_xfermarkchid
 ,  pr_stanupdt
 ,  pr_futupdt
 ,  pr_psid
 ,  pr_serial
 ,  pr_featcost
 ,  pr_reqfacility
 ,  pr_routing
 ,  pr_issueoverlimit
 ,  pr_issuelimitenforce
 ,  pr_porecvlimit
 ,  pr_porecvlimitenforce
 ,  pr_secureprice
 ,  pr_xfercost
 ,  pr_s1id
 ,  pr_s2id
 ,  pr_backjob
 ,  pr_splitjobs
 ,  pr_overissue
 ,  pr_unitlen
 ,  pr_loid
 ,  pr_jobmin
 ,  pr_ltid
 ,  pr_qclead
 ,  pr_trakid
 ,  pr_trak2id
 ,  pr_minquant
 ,  pr_qcfreqtype
 ,  pr_serialcont
 ,  pr_jobmgid
 ,  pr_separatejobs
 ,  pr_rollupmats
 ,  pr_rolluplabor
 ,  pr_rollupburden
 ,  pr_tarewgt
 ,  pr_finasissued
 ,  pr_countunid
 ,  pr_palunid
 ,  pr_picture
 ,  pr_popjob
 ,  pr_routesale
 ,  pr_contprid
 ,  pr_finmatwiploc
 ,  pr_scrapcost
 ,  pr_rollupwgt
 ,  pr_xferchid
 ,  pr_measured
 ,  pr_creditcost
 ,  pr_routereturn
 ,  pr_combinepos
 ,  pr_definqty
 ,  pr_incquant
 ,  pr_shoprel
 ,  pr_frominvprid
 ,  pr_minsale
 ,  pr_incsale
 ,  pr_separatepos
 ,  pr_splitpos
 ,  pr_recatrisk
 ,  pr_shipquan
 ,  pr_salediscchid
 ,  pr_mrpjobsubasm
 ,  pr_taretype
 ,  pr_tareexp
 ,  pr_rollupvol
 ,  pr_minwgt
 ,  pr_maxwgt
 ,  pr_haltposting
 ,  pr_contunid
 ,  pr_commexp
 ,  pr_zoneput
 ,  pr_frtexpchid
 ,  pr_jobinc
 ,  pr_dnid
 ,  pr_safedays
 ,  pr_prtjobpl
 ,  pr_restrictjobquant
 ,  pr_frtrevchid
 ,  pr_qcmrpjobplan
 ,  pr_reqexpdate
 ,  pr_minpallet
 ,  pr_qcid2
 ,  pr_pickorder
 ,  pr_wipinv
 ,  pr_splitposby
 ,  pr_issueunderlimit
 ,  pr_issueunderenforce
 ,  pr_iataunit
 ,  pr_roundupbom
 ,  pr_finseqstage
 ,  pr_recalcbomcalcs
 ,  pr_subordjob
 ,  pr_suggestbefore
 ,  pr_dockrel
 ,  pr_restrictjobinc
 ,  pr_deresqty
 ,  pr_restrictloc
 ,  pr_forecastback
 ,  pr_forecastforward
 ,  pr_backordpo
 ,  pr_pickunit
 ,  pr_leadmins
 ,  pr_makemlfinish
 ,  pr_xfacmarkchid
 ,  pr_custreqxfer
 ,  pr_backordso
 ,  pr_finishltid
 ,  pr_finishloid
 ,  pr_receiveloid
 ,  pr_receiveltid
 ,  pr_autofinunid
 ,  pr_autofinish
 ,  pr_rolluprstypes
 ,  pr_purdis
 ,  pr_splitjobson
 ,  pr_autoaltwgt
 ,  pr_totalcatchml
 ,  pr_bomunid
 ,  pr_dfltreserveloc
 ,  pr_autolinkmrpjobs
 ,  pr_separateicxfers
 ,  pr_poallocatable
 ,  pr_makemlreceive
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,  etl_src_table_name 
 ,  source_system  
from deduped
where row_nbr = 1