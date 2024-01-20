With y2023 as (
    select *, Concat(plant_id, '-', report_date) as report_plant_id 
    from {{source('eia860_stg','reports_2023')}}
),
y2022 as(
    select * , Concat(plant_id, '-', report_date) as report_plant_id
    from {{source('eia860_stg','reports_2022')}}
)
select * from y2023
union all
select * from y2022