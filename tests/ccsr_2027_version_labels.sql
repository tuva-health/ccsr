-- Root-project overrides must not mislabel the v2027.1 mapping snapshot.
select 'diagnosis' as mapping, dxccsr_version as actual_version
from {{ ref('ccsr__long_condition_category') }}
where dxccsr_version is null or dxccsr_version <> '2027.1'
group by dxccsr_version
union all
select 'procedure', prccsr_version
from {{ ref('ccsr__long_procedure_category') }}
where prccsr_version is null or prccsr_version <> '2027.1'
group by prccsr_version
