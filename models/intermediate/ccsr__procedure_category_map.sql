select
    ccsr_map.icd_10_pcs as code,
    ccsr_map.icd_10_pcs_description as code_description,
    ccsr_map.prccsr as ccsr_category,
    left(ccsr_map.prccsr, 3) as ccsr_parent_category,
    ccsr_map.prccsr_description as ccsr_category_description,
    ccsr_map.clinical_domain,
    ontology.section as procedure_section,
    ontology.operation,
    ontology.approach,
    ontology.device,
    ontology.qualifier,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from {{ ref('prccsr_v2023_1_cleaned_map') }} as ccsr_map
left join {{ ref('terminology__icd10_pcs_cms_ontology') }} as ontology
    on ccsr_map.icd_10_pcs = ontology.icd10pcs_code
