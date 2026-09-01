with procedure_records as (

    select
          procedure_id
        , encounter_id
        , claim_id
        , person_id
        , normalized_code as code
        , data_source
    from {{ ref('core__procedure') }}
    where lower(code_system) = 'icd-10-pcs'
      and normalized_code is not null

), ccsr__procedure_category_map as (

    select * from {{ ref ('ccsr__procedure_category_map') }}

)

select distinct
    procedure_records.procedure_id,
    procedure_records.encounter_id,
    procedure_records.claim_id,
    procedure_records.person_id,
    procedure_records.data_source,
    procedure_records.code,
    ccsr__procedure_category_map.code_description,
    ccsr__procedure_category_map.ccsr_parent_category,
    ccsr__procedure_category_map.ccsr_category,
    ccsr__procedure_category_map.ccsr_category_description,
    ccsr__procedure_category_map.clinical_domain,
    ccsr__procedure_category_map.procedure_section,
    ccsr__procedure_category_map.operation,
    ccsr__procedure_category_map.approach,
    ccsr__procedure_category_map.device,
    ccsr__procedure_category_map.qualifier,
    {{ var('prccsr_version') }} as prccsr_version,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from procedure_records
left join ccsr__procedure_category_map
    on procedure_records.code = ccsr__procedure_category_map.code
