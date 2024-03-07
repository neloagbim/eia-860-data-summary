
with a -- cte of necessary columns from stg table
as(
Select 
plant_unit_id,
report_plant_id, 
eia_id,
iso,
state,
county,
unit_name,
op_month,
op_year,
technology,
prime_mover,
fuel_source
From {{ref('stg_clean')}}
),
b as -- execute join on op date. use  dim date to get date_id
(
Select a.*,date.date_id as op_date_id
From a
Left Join {{ref('dim_date')}} as date
ON a.op_month = date.month_num and a.op_year = date.year)

Select 
Distinct
plant_unit_id,
eia_id,
unit_name,
fuel_source,
prime_mover,
technology,
iso,
state,
county,
op_date_id
FROM b
