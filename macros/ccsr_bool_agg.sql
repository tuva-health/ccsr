{% macro ccsr_bool_and(expression) -%}
    {%- if target.type == 'duckdb' -%}
        bool_and({{ expression }})
    {%- else -%}
        booland_agg({{ expression }})
    {%- endif -%}
{%- endmacro %}

{% macro ccsr_bool_or(expression) -%}
    {%- if target.type == 'duckdb' -%}
        bool_or({{ expression }})
    {%- else -%}
        boolor_agg({{ expression }})
    {%- endif -%}
{%- endmacro %}
