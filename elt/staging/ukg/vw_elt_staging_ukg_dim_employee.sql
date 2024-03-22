with  employeedetails as (
    select distinct 
        concat(employeeid,':',source_system) as employee_id_bk,
        employeeid,
       companycode,
       jobdescription,
       orglevel1code,
       orglevel2code,
       orglevel3code,
       orglevel4code,
       originalhiredate::timestamp_ntz  as originalhiredate,
       lasthiredate::timestamp_ntz      as lasthiredate,
       fulltimeorparttimecode,
       primaryworklocationcode,
       dateofseniority::timestamp_ntz   as dateofseniority,
       employeestatuscode,
       jobchangereasoncode,
       jobtitle,
       salaryorhourly,
       leavereasoncode,
       supervisorid,
       supervisorfirstname,
       supervisorlastname,
       dateoftermination::timestamp_ntz as dateoftermination,
       statusstartdate::timestamp_ntz   as statusstartdate,
       positioncode,
       shift,
       termreason,
       terminationreasondescription,
       termtype,
       source_system,
       elt_load_datetime,
       batch_id
from 
{{ref('vw_stg_ukg_employeedetails')}}
), persondetails as 
(select employeeid,
       username,
       firstname,
       middlename,
       lastname,
       dateofbirth::timestamp_ntz as dateofbirth,
       gender
from 
{{ref('vw_stg_ukg_persondetails')}}
),
comp as (
select company_sk
    ,    company_abbr
from {{ref('vw_elt_staging_ref_companies')}}
)
select    
        {{ dbt_utils.surrogate_key(['emp.employee_id_bk'])}}        as employee_sk
    ,     case 
                when emp.orglevel1code = 'CML' then comp.company_sk
                when emp.orglevel1code = 'SEC' then comp.company_sk
                when substr(emp.orglevel1code,1,3) = 'SOL' then comp.company_sk
                when emp.orglevel1code = 'SVM' then comp.company_sk
                when emp.orglevel1code = 'POL' then comp.company_sk
                else 'NA'
          end          as company_sk
    ,     emp.source_system
    ,     persondetails.employeeid         as employee_id
    ,     persondetails.username           as employee_user_name
    ,     persondetails.firstname          as employee_first_name
    ,     persondetails.middlename         as employee_middle_name
    ,     persondetails.lastname           as employee_last_name
    ,     persondetails.dateofbirth        as employee_dob
    ,     persondetails.gender             as employee_gender
    ,     emp.jobdescription               as employee_job_desc
    ,     emp.orglevel1code                as employee_org_level1
    ,     emp.orglevel2code                as employee_org_level2
    ,     emp.orglevel3code                as employee_org_level3
    ,     emp.originalhiredate             as employee_orig_hire_date
    ,     emp.lasthiredate                 as employee_last_hire_date
    ,     case when emp.fulltimeorparttimecode = 'F' then 'Full Time'
                when emp.fulltimeorparttimecode = 'P' then 'Part Time'
                else 'Other' end           as employee_full_time 
    ,     emp.primaryworklocationcode      as employee_primary_location
    ,     emp.dateofseniority              as employee_seniority_date
    ,     emp.employeestatuscode           as employee_status
    ,     emp.ORGLEVEL4CODE                as employee_exempt_status
    ,     emp.salaryorhourly               as employee_hourly_or_salary
    ,     emp.jobchangereasoncode          as employee_job_change_code
    ,     emp.jobtitle                     as employee_job_title
    ,     emp.leavereasoncode              as employee_leave_reason_code
    ,     emp.supervisorid                 as employee_supervisor_id
    ,     emp.supervisorfirstname          as employee_supervisor_first_name
    ,     emp.supervisorlastname           as employee_supervisor_last_name
    ,     emp.dateoftermination            as employee_termination_date
    ,     emp.statusstartdate              as employee_status_start_date
    ,     emp.positioncode                 as employee_position_code
    ,     emp.shift                        as employee_shift
    ,     emp.termreason                   as employee_termination_reason
    ,     emp.terminationreasondescription as employee_termination_desc
    ,     case when emp.termtype = 'V' then 'Voluntary'
                when emp.termtype = 'I' then 'Involuntary'
                else 'Other' end           as employee_termination_type
    ,     emp.batch_id                     as batch_id
    ,     emp.elt_load_datetime            as elt_load_datetime
    ,     sysdate()                        as elt_process_datetime
 from employeedetails emp left join persondetails persondetails
 on emp.employeeid = persondetails.employeeid
 left join comp comp
 on substr(emp.orglevel1code,1,3) = comp.company_abbr
