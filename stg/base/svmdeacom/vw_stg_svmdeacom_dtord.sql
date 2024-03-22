with src as (
    select 
        *   
         ,'{{source('svmdeacom','dtord')}}' as etl_src_table_name
         ,'SVMDEACOM'                                as source_system
    from {{source('svmdeacom','dtord')}}
)
,deduped as (
    select
        *,
        row_number() over (partition by or_id order by _rivery_last_update desc) row_nbr
    from src
)

select
	or_id
 ,  or_ordnum
 ,  or_linenum
 ,  or_chid
 ,  or_cogsid
 ,  or_prid
 ,  or_quant
 ,  or_qship
 ,  or_price
 ,  or_exten
 ,  or_notes
 ,  or_taxable
 ,  or_stocked
 ,  or_control
 ,  or_wanted
 ,  or_promise
 ,  or_dueship
 ,  or_confirm
 ,  or_expires
 ,  or_jobnum
 ,  or_user1
 ,  or_prunid
 ,  or_prfact
 ,  or_unitwgt
 ,  or_taid
 ,  or_unitcos
 ,  or_subtot
 ,  or_discoun
 ,  or_tally
 ,  or_lispric
 ,  or_stantot
 ,  or_purnum
 ,  or_loadcos
 ,  or_prictyp
 ,  or_phid
 ,  or_salunid
 ,  or_salfact
 ,  or_release
 ,  or_toid
 ,  or_special
 ,  or_tranrecv
 ,  or_feattree
 ,  or_override
 ,  or_origprice
 ,  or_dealpric
 ,  or_avgcost
 ,  or_cuid
 ,  or_linedisc
 ,  or_origprod
 ,  or_sizeprod
 ,  or_quotedcost
 ,  or_catchwgt
 ,  or_blanket
 ,  or_blanketid
 ,  or_inclfeat
 ,  or_featpric
 ,  or_pmid
 ,  or_pmfact
 ,  or_p4id
 ,  or_noinv
 ,  or_ordquant
 ,  or_shipquant
 ,  or_duedock
 ,  or_rtid
 ,  or_dockmins
 ,  or_tarewgt
 ,  or_packages
 ,  or_cogsdelta
 ,  or_frtcost
 ,  or_overridedate
 ,  or_overrideuser
 ,  or_priceordnum
 ,  or_planquant
 ,  or_qplan
 ,  or_totalorder
 ,  or_scid
 ,  or_siid
 ,  or_backquant
 ,  or_autoaddfreight
 ,  or_discountid
 ,  or_noreserve
 ,  or_commable
 ,  or_promoamt
 ,  or_poallocatable
 ,  or_pickunit
 ,  or_linejob
 ,  or_repack
 ,  or_shid
 ,  or_masterorid
 ,  or_trid
 ,  or_frid
 ,  or_doid
 ,  or_actualfrtcost
 ,  or_gcid
 ,  or_vaid
 ,  or_laborcogsid
 ,  or_burdencogsid
 ,  _rivery_river_id
 ,  _rivery_run_id
 ,  _rivery_last_update
 ,	etl_src_table_name
 ,	source_system
from deduped
where row_nbr = 1
