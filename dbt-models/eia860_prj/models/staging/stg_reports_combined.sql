With y2023 as ( --create CTE for each report year
    select *
    from {{source('raw','reports_2023')}} --reference source 2023 table in raw schema
),
y2022 as(
    select * 
    from {{source('raw','reports_2022')}}
),
y2021 as (
    select *
    from {{source('raw','reports_2021')}}
),
y2020 as (
    select * 
    from {{source('raw','reports_2020')}}
),
y2019 as (
    select *
    from {{source('raw','reports_2019')}}
),
-- union all CTEs making sure to keep all records
y_all as (
select * from y2023
union all
select * from y2022
union all
select * from y2021
union all
select * from y2020
union all
select * from y2019)
-- combine all tables and add unique keys

select *, 
-- add surrogate key for distinct power plant unit per report release 
{{dbt_utils.generate_surrogate_key(['plant_id','unit_id','report_date'])}} as report_plant_id,
-- add surrogate key for distinct power plant unit
{{dbt_utils.generate_surrogate_key(['plant_id','unit_id','state','county','balancing_auth','op_month','op_year',
'technology','prime_mover','fuel_source','nameplate_capacity_mw'])}} as plant_unit_id
from y_all