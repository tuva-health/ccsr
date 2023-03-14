{{ config(materialized='table') }}

with codes as (
    
    select
        icd_10_cm_code as code, 
        {%- for i in range(1,7) %}
        ccsr_category_{{ i }},
        {%- endfor %}
        default_ccsr_category_ip,
        default_ccsr_category_op
    from {{ ref('dxccsr_v2023_1_cleaned_map') }}

), long_union as (

    {% for i in range(1,7,1) %}
    select 
        code,
        ccsr_category_{{ i }} as ccsr_category,
        {{ i }} as ccsr_category_rank,
        (ccsr_category_{{ i }} = default_ccsr_category_ip) as is_ip_default_category,
        (ccsr_category_{{ i }} = default_ccsr_category_ip) as is_op_default_category
    from codes 
    {{ "union" if not loop.last else "" }}
    {%- endfor %}

)

select distinct
    *
from long_union
where ccsr_category is not null