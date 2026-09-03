with procedure_base as (

    select
        data_source,
        ccsr_category,
        operation,
        approach,
        claim_id,
        count(claim_id) over (
            partition by data_source, ccsr_category, operation
        ) as n_total_occurrences
    from {{ ref('ccsr__long_procedure_category') }}
    where ccsr_category is not null

), procedures_aggregated as (

    select
        data_source,
        ccsr_category,
        operation,
        approach,
        count(claim_id) as n_occurrences_with_approach,
        max(n_total_occurrences) as n_total_occurrences
    from procedure_base
    group by
        data_source,
        ccsr_category,
        operation,
        approach

), category_descriptions as (

    -- Description variants must not split a category's procedure counts.
    select
        ccsr_category,
        min(ccsr_category_description) as ccsr_category_description
    from {{ ref('ccsr__procedure_category_map') }}
    group by ccsr_category

)

select
    procedures_aggregated.data_source,
    procedures_aggregated.ccsr_category,
    category_descriptions.ccsr_category_description,
    procedures_aggregated.operation,
    procedures_aggregated.approach,
    procedures_aggregated.n_occurrences_with_approach,
    procedures_aggregated.n_total_occurrences,
    100.0 * procedures_aggregated.n_occurrences_with_approach
        / nullif(procedures_aggregated.n_total_occurrences, 0) as approach_rate,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from procedures_aggregated
left join category_descriptions
    on procedures_aggregated.ccsr_category = category_descriptions.ccsr_category
