-- Cover a new leading-zero PCS code, an existing code reassigned since
-- v2023.1, and the new ESA012 procedure category.
with expected as (
    select '00503Z4' as code, 'CNS007' as category
    union all select '05H00DZ', 'CAR029'
    union all select '5A05A0L', 'ESA012'
)
select expected.*
from expected
left join {{ ref('prccsr_v2027_1_cleaned_map') }} as actual
    on expected.code = actual.icd_10_pcs
where actual.icd_10_pcs is null
   or actual.prccsr is null
   or actual.prccsr <> expected.category
