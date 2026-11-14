select distinct dx.ccsr_parent_category
from {{ ref('ccsr__dx_vertical_pivot') }} as dx
left join {{ ref('dxccsr_v2027_1_body_systems') }} as body
    on dx.ccsr_parent_category = body.ccsr_parent_category
where body.ccsr_parent_category is null
