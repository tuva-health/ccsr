-- Newer codes reported missing from the v2023.1 crosswalk. Assert the
-- published mapping payload, including the distinct IP/OP default for Z2989.
with expected as (
    select 'E66811' as code, 'END009' as category, 'END009' as ip_default, 'END009' as op_default
    union all select 'E66812', 'END009', 'END009', 'END009'
    union all select 'E66813', 'END009', 'END009', 'END009'
    union all select 'R7689', 'SYM017', 'SYM017', 'SYM017'
    union all select 'Z2989', 'FAC005', 'XXX000', 'FAC005'
    union all select 'R1020', 'SYM006', 'SYM006', 'SYM006'
    union all select 'E88819', 'END016', 'END016', 'END016'
    union all select 'M51369', 'MUS011', 'MUS011', 'MUS011'
    union all select 'E88810', 'END016', 'END016', 'END016'
    union all select 'E78019', 'END010', 'END010', 'END010'
)
select expected.*
from expected
left join {{ ref('dxccsr_v2027_1_cleaned_map') }} as actual
    on expected.code = actual.icd_10_cm_code
where actual.icd_10_cm_code is null
   or actual.ccsr_category_1 <> expected.category
   or actual.default_ccsr_category_ip <> expected.ip_default
   or actual.default_ccsr_category_op <> expected.op_default
   or actual.ccsr_category_1 is null
   or actual.default_ccsr_category_ip is null
   or actual.default_ccsr_category_op is null
