
with a as(
Select 
plant_unit_id,
report_plant_id, 
eia_id,
plant_name,
state,
county,
unit_name,
op_month,
op_year,
technology,
prime_mover,
fuel_source,
capacity_mw,
report_year,
report_month
From {{ref('stg_clean')}}
),
b as (
Select a.*, date.date_id as report_date_id
From a
Left Join {{ref('dim_date')}} as date
ON a.report_month = date.month_name and a.report_year = date.year
)
Select 
report_plant_id,
plant_unit_id,
report_date_id,
capacity_mw as unit_capacity,
CASE WHEN report_plant_id IS NOT NULL THEN 1 ELSE 0 END as unit_count
From b