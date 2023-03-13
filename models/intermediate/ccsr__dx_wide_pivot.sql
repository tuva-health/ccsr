with unique_ccsr_categories as (
    
    select distinct ccsr_category from {{ ref('ccsr__dx_vertical_pivot')}}

)
-- jinja for loop to create a column for each row in CTE
select distinct  
*
from unique_ccsr_categories