# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 15:47:09 2024

@author: NeloAgbim
"""

from sqlalchemy import create_engine
import pandas as pd
import json


# import libraries for connecting to google sheets
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
#from pydrive.auth import GoogleAuth
#from pydrive.drive import GoogleDrive

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
# remove duplicate column and reorder columns

df = df[[ 'plant_unit_id','report_plant_id', 'report_date_id','eia_id', 'unit_name', 'unit_capacity',
       'unit_count', 'r_month_num', 'r_month_name', 'r_year', 'r_quarter','fuel_source', 'prime_mover',
       'technology', 'iso', 'state', 'county', 'op_date_id', 'op_month_num',
       'op_month_name', 'op_year', 'op_quarter']]

df = df.iloc[:,1:]
#%% Connect to google drive and upload query as a sheet

# scope of where api will work
scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

# read in credentials
credentials = Credentials.from_service_account_file(r'C:\Users\NeloAgbim\Documents\Google Sheets Python Json\my-github-projects-410915-057cbdd1bc31.json',  scopes=scopes)
gc = gspread.authorize(credentials)

# google sheet url
url = 'https://docs.google.com/spreadsheets/d/1ddigiLsVZdPGpCO0IAeFFgjaQcAguABD3n9HaQzgL4E/edit#gid=0'
# open the google sheet
gs = gc.open_by_url(url)
# select a work sheet from its name
worksheet = gs.worksheet('dataset')


# write to google sheet
# clear out any contents
worksheet.clear()
# set google sheet to query from earlier
set_with_dataframe(worksheet, dataframe=df, include_index=False,include_column_header=True,resize=True)

# practice getting data from google sheet
#sheet1 = worksheet.get_all_records()

#df1 = pd.DataFrame(sheet1)