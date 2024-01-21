With y2023 as (
    select *, Concat(plant_id, '-', unit_id, '-',report_date) as report_plant_id 
    from {{source('raw','reports_2023')}}
),
y2022 as(
    select * , Concat(plant_id, '-', unit_id, '-',report_date) as report_plant_id
    from {{source('raw','reports_2022')}}
),
y2021 as (
    select * , Concat(plant_id, '-', unit_id, '-',report_date) as report_plant_id 
    from {{source('raw','reports_2021')}}
),
y2020 as (
    select * , Concat(plant_id, '-', unit_id, '-',report_date) as report_plant_id
    from {{source('raw','reports_2020')}}
),
y2019 as (
    select * , Concat(plant_id, '-', unit_id, '-',report_date) as report_plant_id
    from {{source('raw','reports_2019')}}
)

select * from y2023
union all
select * from y2022
union all
select * from y2021
union all
select * from y2020
union all
select * from y2019