{{ config(materialized='table') }}

with dxccsr_vertical_codes as (
    
    select * from {{ ref('ccsr__dx_vertical_pivot')}}

), condition_records as (
    
    select * from {{ var('condition')}}

)

select 
    condition_records.encounter_id,
    condition_records.claim_id,
    condition_records.patient_id,
    condition_records.code as icd10_code,
    condition_records.diagnosis_rank,
    dxccsr_vertical_codes.ccsr_category,
    dxccsr_vertical_codes.ccsr_category_rank,
    dxccsr_vertical_codes.is_ip_default_category,
    dxccsr_vertical_codes.is_op_default_category
from condition_records 
left join dxccsr_vertical_codes using(code)

    