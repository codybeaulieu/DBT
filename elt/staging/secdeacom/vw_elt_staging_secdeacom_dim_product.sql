with dmprod as (
    select 
            concat(pr_id,':',source_system)  as product_id_bk
        ,   pr_id
        ,   pr_codenum
        ,   pr_descrip
        ,   pr_caid
        ,   pr_c2id
        ,   pr_user5
        ,   pr_prunid
        ,   source_system
        ,   _rivery_run_id
        ,   _rivery_last_update
    from {{ref('vw_stg_secdeacom_dmprod')}} 
),
dmcats as (
	select ca_id,ca_name
	from {{ref('vw_stg_secdeacom_dmcats')}}
),
dmcats2 as (
	select c2_id,c2_name
	from {{ref('vw_stg_secdeacom_dmcats2')}}
),
dmpr1 as (
	select p1_id,p1_name
	from {{ref('vw_stg_secdeacom_dmpr1')}}
),
dmunit as (
	select un_id, un_name 
	from {{ref('vw_stg_secdeacom_dmunit')}}
)
select      
            {{ dbt_utils.surrogate_key(['product_id_bk'])}} as product_sk
        ,    dmprod.pr_codenum as product_id
        ,    dmprod.pr_descrip as product_desc
        ,    dmcats.ca_name    as product_cat
        ,    dmcats2.c2_name   as product_sub_cat
        ,    dmpr1.p1_name     as product_core_tech
        ,    dmunit.un_name    as product_uom
        ,    dmprod.source_system as source_system
        ,    dmprod._rivery_run_id as batch_id
        ,    dmprod._rivery_last_update as elt_load_datetime
        ,    sysdate() as elt_process_datetime
from dmprod dmprod
left join dmcats dmcats
        on dmprod.pr_caid = dmcats.ca_id
left join dmcats2 dmcats2
        on dmprod.pr_c2id = dmcats2.c2_id
left join dmpr1 dmpr1
        on dmprod.pr_user5 = dmpr1.p1_id
left join dmunit dmunit
        on dmprod.pr_prunid = dmunit.un_id
