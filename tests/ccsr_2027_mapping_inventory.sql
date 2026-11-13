-- Exact inventory of the reviewed AHRQ v2027.1 snapshot. A changed payload
-- requires a new asset version and a deliberate update of these expectations.
with inventory as (
    select 'diagnosis_codes' as asset, count(*) as actual, 75915 as expected
    from {{ ref('dxccsr_v2027_1_cleaned_map') }}
    union all
    select 'procedure_codes', count(*), 82510
    from {{ ref('prccsr_v2027_1_cleaned_map') }}
    union all
    select 'body_systems', count(*), 22
    from {{ ref('dxccsr_v2027_1_body_systems') }}
    union all
    select 'diagnosis_categories', count(distinct ccsr_category), 554
    from {{ ref('ccsr__dx_vertical_pivot') }}
    union all
    select 'procedure_categories', count(distinct ccsr_category), 328
    from {{ ref('ccsr__procedure_category_map') }}
)
select * from inventory where actual <> expected
