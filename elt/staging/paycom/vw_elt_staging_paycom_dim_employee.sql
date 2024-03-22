with  employeedetails as (
    select distinct
       concat(employee_code,':',source_system) as employee_id_bk,
       'POL' as company_code,
       employee_code,
       firstname,
       middlename,
       lastname,
       birth_date::timestamp_ntz as birth_date,
       gender,
       hourly_or_salary,
       department_code,
       position_family_name,
       hire_date::timestamp_ntz as hire_date,
       last_position_change_date::timestamp_ntz as last_position_change_date,
       dol_status,
       location,
       seniority_date::timestamp_ntz as seniority_date,
       exempt_status,
       employee_status,
       business_title,
       termination_reason,
       supervisor_primary_code,
       termination_date::timestamp_ntz as termination_date,
       position_code,
       termination_type,
       source_system,
       elt_load_datetime,
       batch_id
from 
{{ref('vw_stg_paycom_employeedetail')}}
),
comp as (
select company_sk
    ,    company_abbr
from {{ref('vw_elt_staging_ref_companies')}}
where company_abbr = 'POL'
)
select    
        {{ dbt_utils.surrogate_key(['emp.employee_id_bk'])}}        as employee_sk
    ,     comp.company_sk                                           as company_sk
    ,     emp.source_system                                         as source_system
    ,     emp.employee_code                                         as employee_id
    ,     NULL                                                      as employee_user_name
    ,     emp.firstname                                             as employee_first_name
    ,     emp.middlename                                            as employee_middle_name
    ,     emp.lastname                                              as employee_last_name
    ,     emp.birth_date                                            as employee_dob
    ,     case when emp.gender = 1 THEN 'M'
                when emp.gender = 2 THEN 'F'
                else 'Other' end                                    as employee_gender
    ,     NULL                                                      as employee_job_desc
    ,     emp.department_code                                       as employee_org_level1
    ,     NULL                                                      as employee_org_level2
    ,     emp.position_family_name                                  as employee_org_level3
    ,     emp.hire_date                                             as employee_orig_hire_date
    ,     emp.last_position_change_date                             as employee_last_hire_date
    ,     emp.dol_status                                            as employee_full_time 
    ,     emp.location                                              as employee_primary_location
    ,     emp.seniority_date                                        as employee_seniority_date
    ,     emp.employee_status                                       as employee_status
    ,     emp.exempt_status                                         as employee_exempt_status
    ,     emp.hourly_or_salary                                      as employee_hourly_or_salary
    ,     NULL                                                      as employee_job_change_code
    ,     emp.business_title                                        as employee_job_title
    ,     emp.termination_reason                                    as employee_leave_reason_code
    ,     emp.supervisor_primary_code                               as employee_supervisor_id
    ,     NULL                                                      as employee_supervisor_first_name
    ,     NULL                                                      as employee_supervisor_last_name
    ,     emp.termination_date                                      as employee_termination_date
    ,     NULL                                                      as employee_status_start_date
    ,     emp.position_code                                         as employee_position_code
    ,     NULL                                                      as employee_shift
    ,     emp.termination_type                                      as employee_termination_reason
    ,     emp.termination_type                                      as employee_termination_desc
    ,     emp.termination_reason                                    as employee_termination_type
    ,     emp.batch_id                                              as batch_id
    ,     emp.elt_load_datetime                                     as elt_load_datetime
    ,     sysdate()                                                 as elt_process_datetime
 from employeedetails emp   left join comp comp 
    on emp.company_code = comp.company_abbr

