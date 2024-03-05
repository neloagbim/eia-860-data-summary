SELECT
plant_unit_id,
report_plant_id, 
plant_id as eia_id,
plant_name,
state,
county,
unit_id as unit_name,
CASE 
    WHEN balancing_auth = 'CISO' THEN 'CAISO'
    WHEN balancing_auth = 'ERCO' THEN 'ERCOT'
    WHEN balancing_auth = 'ISNE' THEN 'ISONE'
    WHEN balancing_auth = 'PJM' THEN 'PJM'
    WHEN balancing_auth = 'MISO' THEN 'MISO'
    WHEN balancing_auth = 'NYIS' THEN 'NYISO'
    WHEN balancing_auth = 'SWPP' THEN 'SPP'
    ELSE 'Not RTO Balancing Autority' END as iso,
op_month,
op_year,
technology,
prime_mover,
CASE 
    WHEN fuel_source = 'SUN' THEN 'SUN'
    WHEN fuel_source = 'WND' THEN 'WIND'
    ELSE 'Not Renewable Energy Source' END as fuel_source,
--sector, --sector is only utilities
nameplate_capacity_mw as capacity_mw,
-- split report date column into 2 columns: month and year
SPLIT_PART(report_date, '-',2)::integer as report_year,
SPLIT_PART(report_date,'-',1)::text as report_month
FROM {{ref('stg_reports_combined')}}

WHERE
fuel_source in ('WND','SUN')