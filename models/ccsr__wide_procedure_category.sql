{{ config(materialized='table',enabled=false) }}

{% set category_query %}
select distinct 
    prccsr
from {{ ref('dxccsr_v2023_1_cleaned_map') }}
{% endset %}

{% set categories = run_query(category_query) %}
{% if execute %}
{% set categories_list = categories.columns[0].values() %}
{% endif %}

select * from {{ var('procedure') }}