with ccsr__dx_vertical_pivot as (
    
    select * from {{ ref('ccsr__dx_vertical_pivot') }} 

), condition as (

    select
          encounter_id
        , claim_id
        , person_id
        , normalized_code as code
        , condition_rank as diagnosis_rank
        , data_source
    from {{ ref('core__condition') }}
    where lower(code_system) = 'icd-10-cm'
      and normalized_code is not null

), dxccsr_body_systems as (

    select * from {{ ref('dxccsr_v2023_1_body_systems') }}

)

select 
    condition.encounter_id,
    condition.claim_id,
    condition.person_id,
    condition.code,
    ccsr__dx_vertical_pivot.code_description,
    condition.diagnosis_rank,
    ccsr__dx_vertical_pivot.ccsr_parent_category,
    dxccsr_body_systems.body_system,
    dxccsr_body_systems.parent_category_description,
    ccsr__dx_vertical_pivot.ccsr_category,
    ccsr__dx_vertical_pivot.ccsr_category_description,
    ccsr__dx_vertical_pivot.ccsr_category_rank,
    ccsr__dx_vertical_pivot.is_ip_default_category,
    ccsr__dx_vertical_pivot.is_op_default_category,
    {{ var('dxccsr_version') }} as dxccsr_version,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from condition
left join ccsr__dx_vertical_pivot
    on condition.code = ccsr__dx_vertical_pivot.code
left join dxccsr_body_systems
    on ccsr__dx_vertical_pivot.ccsr_parent_category = dxccsr_body_systems.ccsr_parent_category

    
