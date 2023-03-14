{{ config(materialized='table',enabled=false) }}
select * from {{ var('procedure') }}