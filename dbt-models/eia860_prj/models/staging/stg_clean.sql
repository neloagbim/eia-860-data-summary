select 
plant_id,
state,
county,
CASE 
    WHEN balancing_auth = 'CISO' THEN 'CAISO'
    WHEN balancing_auth = 'ERCO' THEN 'ERCOT'
    WHEN balancing_auth = 'ISNE' THEN 'ISONE'
    WHEN balancing_auth = 'PJM' THEN 'PJM'
    WHEN balancing_auth = 'MISO' THEN 'MISO'
    WHEN balancing_auth = 'NYIS' THEN 'NYISO'
    WHEN balancing_auth = 'SWPP' THEN 'SPP'
    ELSE 'Not RTO Balancing Autority'
SPLIT_PART(report_date, '-',2)::integer as report_year,
SPLIT_PART(report_date,'-',1)::text as report_month
from {{ref('stg_reports_combined')}}