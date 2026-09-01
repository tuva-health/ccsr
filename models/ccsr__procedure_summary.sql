with procedure_base as (

    select
        data_source,
        ccsr_category,
        ccsr_category_description,
        operation,
        approach,
        count(*) over (
            partition by data_source, ccsr_category, operation
        ) as n_total_occurrences
    from {{ ref('ccsr__long_procedure_category') }}
    where ccsr_category is not null

), procedures_aggregated as (

    select
        data_source,
        ccsr_category,
        ccsr_category_description,
        operation,
        approach,
        count(*) as n_occurrences_with_approach,
        n_total_occurrences
    from procedure_base
    group by
        data_source,
        ccsr_category,
        ccsr_category_description,
        operation,
        approach,
        n_total_occurrences

)

select
    data_source,
    ccsr_category,
    ccsr_category_description,
    operation,
    approach,
    n_occurrences_with_approach,
    n_total_occurrences,
    100.0 * n_occurrences_with_approach
        / nullif(n_total_occurrences, 0) as approach_rate,
    '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as _model_run_time
from procedures_aggregated
