# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 15:47:09 2024

@author: NeloAgbim
"""

from sqlalchemy import create_engine,text
import pandas as pd
import json
# query data and upload to google sheets
# read in secrets file
secrets = open(r'C:\Users\NeloAgbim\Documents\PythonPrjEnvs\eiasummary\github\secrets.json')

# save as a credentials dictionary
cred = json.load(secrets)

# credentials for sql connection
user = cred['db_creds']['user']
password = cred['db_creds']['password']
host= cred['db_creds']['host']
port = cred['db_creds']['port']
database = cred['db_creds']['database']

# string together connction details
connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'

# create engine
engine = create_engine(connection_string)

# establish connection
conn = engine.connect()

#result = conn.execute(text('Select * From chinelo_warehouse.dim_date')).fetchall()

df = pd.read_sql('''
                 with a as (
                 Select p.*,d.month_num as op_month_num, d.month_name as op_month_name,d.year as op_year, d.quarter_num as op_quarter 
                 FROM chinelo_warehouse.dim_plant_unit as p
                 Left Join chinelo_warehouse.dim_date as d ON p.op_date_id = d.date_id),
                 b as (
                 Select f.*, d.month_num as r_month_num, d.month_name as r_month_name,d.year as r_year, d.quarter_num as r_quarter
                 From chinelo_warehouse.fct_unit_capacity as f
                 Left Join chinelo_warehouse.dim_date as d ON f.report_date_id = d.date_id)
                 Select * 
                 From b
                 Left Join a
                 ON a.plant_unit_id = b.plant_unit_id''',conn)

conn.close()