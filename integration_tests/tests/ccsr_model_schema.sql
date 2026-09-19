-- Run with ccsr_expected_schema set to the expected schema when testing
-- tuva_schema_prefix or a root-project schema override.
{% set expected_schema = var('ccsr_expected_schema', 'ccsr') %}
{% set model_names = [
    'ccsr__dx_vertical_pivot',
    'ccsr__procedure_category_map',
    'ccsr__long_condition_category',
    'ccsr__singular_condition_category',
    'ccsr__wide_condition_category',
    'ccsr__long_procedure_category',
    'ccsr__wide_procedure_category',
    'ccsr__procedure_summary'
] %}

{% for model_name in model_names %}
select
    '{{ model_name }}' as model_name,
    '{{ ref(model_name).schema }}' as actual_schema,
    '{{ expected_schema }}' as expected_schema
where '{{ ref(model_name).schema }}' <> '{{ expected_schema }}'
{% if not loop.last %}union all{% endif %}
{% endfor %}
