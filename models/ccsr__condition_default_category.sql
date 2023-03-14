select 
    claim_id,
    ccsr_category,
    {{ var('dxccsr_version') }} as dxccsr_version
 from {{ref('ccsr__condition_ccsr_vertical')}}
 where 
    is_ip_default_category = true
    and diagnosis_rank = 1