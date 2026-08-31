{% set categories_list = dbt_utils.get_column_values(
        table=ref("ccsr__dx_vertical_pivot"),
        column="ccsr_category",
        order_by="ccsr_category"
) %}


with boolean_ranks as (

    -- Reduce the long table to one row per CCSR category per encounter.
    -- Integer min/max preserves boolean AND/OR semantics across adapters.
    select 
        encounter_id,
        claim_id,
        person_id,
        ccsr_category,
        min(case when diagnosis_rank = 1 then 1 else 0 end) as is_only_first,
        max(case when diagnosis_rank = 1 then 1 else 0 end) as is_first,
        max(case when diagnosis_rank >= 1 then 1 else 0 end) as is_nth,
        max(case when diagnosis_rank > 1 then 1 else 0 end) as not_first
    from {{ ref('ccsr__long_condition_category') }}
    group by encounter_id, claim_id, person_id, ccsr_category

), bool_logic as (

    select distinct
        encounter_id,
        claim_id,
        person_id,
        ccsr_category,
        -- assigns one of four values for each DXCCSR data element as per pg 25 of DXCCSR User guide v2023.1
        case 
            when is_nth = 0 then 0
            when is_only_first = 1 and ccsr_category not like 'XXX%' then 1
            when is_first = 1 and is_nth = 1 and ccsr_category not like 'XXX%' then 2
            when not_first = 1 then 3
            else -99 
            end as dx_code
    from boolean_ranks

)

select distinct
    encounter_id,
    claim_id,
    person_id,
    -- pivot rows into column values for each possible CCSR category
    {% for category in categories_list %}
    sum(case when ccsr_category = '{{ category }}' then dx_code else 0 end) as dxccsr_{{ category }},
    {% endfor %}
    {{ var('dxccsr_version') }} as dxccsr_version,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from bool_logic
group by encounter_id, claim_id, person_id
