with date_raw as( 
    {{dbt_date.get_date_dimension('1994-01-01','2025-01-01')}} --populate dates from the first project renewables operation date through next year

),
date_distinct as (
Select distinct month_name, month_name_short as month_abbv, month_of_year as month_num, quarter_of_year as quarter_num, year_number as year

From date_raw
)
select *,
{{dbt_utils.generate_surrogate_key(['month_num','year'])}} as date_id
from date_distinct