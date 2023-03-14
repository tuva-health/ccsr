
{{ config(materialized='ephemeral') }}
with codes as (
    
    select
        icd_10_cm_code as code, 
        {%- for i in range(1,7) %}
        ccsr_category_{{ i }},
        {%- endfor %}
        default_ccsr_category_ip,
        default_ccsr_category_op
    from {{ ref('dxccsr_v2023_1_cleaned_map')}}

), long_union as (

    select 
        code,
        ccsr_category_1 as ccsr_category,
        1 as ccsr_category_rank,
        (ccsr_category_1 = default_ccsr_category_ip) as is_ip_default_category,
        (ccsr_category_1 = default_ccsr_category_ip) as is_op_default_category
    from codes 
    union
    select 
        code,
        ccsr_category_2 as ccsr_category,
        2 as ccsr_category_rank,
        (ccsr_category_2 = default_ccsr_category_ip) as is_ip_default_category,
        (ccsr_category_2 = default_ccsr_category_op) as is_op_default_category
    from codes 
    union
    select 
        code,
        ccsr_category_3 as ccsr_category,
        3 as ccsr_category_rank,
        (ccsr_category_3 = default_ccsr_category_ip) as is_ip_default_category,
        (ccsr_category_3 = default_ccsr_category_op) as is_op_default_category
    from codes 
    union
    select 
        code,
        ccsr_category_4 as ccsr_category,
        4 as ccsr_category_rank,
        (ccsr_category_4 = default_ccsr_category_ip) as is_ip_default_category,
        (ccsr_category_4 = default_ccsr_category_op) as is_op_default_category
    from codes 
    union
    select 
        code,
        ccsr_category_5 as ccsr_category,
        5 as ccsr_category_rank,
        (ccsr_category_5 = default_ccsr_category_ip) as is_ip_default_category,
        (ccsr_category_5 = default_ccsr_category_op) as is_op_default_category
    from codes 
    union
    select 
        code,
        ccsr_category_6 as ccsr_category,
        6 as ccsr_category_rank,
        (ccsr_category_6 = default_ccsr_category_ip) as is_ip_default_category,
        (ccsr_category_6 = default_ccsr_category_op) as is_op_default_category
    from codes 

)

select distinct
    *
from long_union
where ccsr_category is not null