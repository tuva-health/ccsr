select 
    claim_id,
    {{ var('dxccsr_version') }} as dxccsr_version
 from {{ref('ccsr__condition_ccsr_vertical')}}
 where 
    is_ip_default_category = true