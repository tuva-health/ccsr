{{ config(materialized='table') }}

{% set category_query %}
select distinct 
    ccsr_category
from {{ ref('ccsr__condition_ccsr_vertical') }}
{% endset %}

{% set categories = run_query(category_query) %}
{% if execute %}
{% set categories_list = categories.columns[0].values() %}
{% endif %}


with bool_ranks as (

    select 
        encounter_id,
        ccsr_category,
        ccsr_category like 'XXX%' as is_excluded,
        booland_agg(diagnosis_rank = 1) as is_only_first,
        boolor_agg(diagnosis_rank = 1) as is_first,
        boolor_agg(diagnosis_rank >= 1) as is_nth,
        boolor_agg(diagnosis_rank > 1) as not_first
    from {{ ref('ccsr__condition_ccsr_vertical') }}
    {{ dbt_utils.group_by(n=3) }}

), bool_logic as (

    select distinct
        encounter_id,
        ccsr_category,
        case 
            when not is_nth then 0
            when is_only_first and not is_excluded then 1
            when is_first and is_nth and not is_excluded then 2
            when not_first then 3 
            else -1 
            end as dx_code
    from bool_ranks 

)

select distinct
    encounter_id,
    {% for category in categories_list %}
    sum(case when ccsr_category = '{{ category }}' then dx_code else 0 end) as DXCCSR_{{ category }},
    {% endfor %}
    'version' as dxccsr_version
from bool_logic
group by encounter_id,dxccsr_version


