# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:59:30 2024

@author: NeloAgbim
"""

import pandas as pd
import tqdm
import requests
import openpyxl
import io
import urllib.parse
import psycopg2
from sqlalchemy import create_engine

#request download
url = 'https://www.eia.gov/electricity/data/eia860m/xls/november_generator2023.xlsx'
response = requests.get(url)

# wrap request response in IO wrapper to get xlsx read in
df = pd.read_excel(io.BytesIO(response.content), skiprows=2, sheet_name = 0)

df.dtypes

file_name = url.split('xls/',2)[1]
file_date = file_name.replace("_generator", "-").replace(".xlsx", "")

# save eia file date as 
df["Report Date"] = file_date

# list necessary columns
cols = ['Entity ID', 'Entity Name', 'Plant ID', 'Plant Name', 'Plant State',
        'County', 'Balancing Authority Code',
       'Sector', 'Generator ID', 'Unit Code', 'Nameplate Capacity (MW)',
       'Technology','Energy Source Code', 'Prime Mover Code', 'Operating Month',
       'Operating Year', 'Planned Retirement Month', 'Planned Retirement Year',
       'Status', 'Report Date']
# keep only listed columns
df = df[cols]

# remove rows without an entity id
df = df[df["Entity ID"].isna()==False].reset_index(drop=True)

#rempve row with notes
df = df[df["Entity ID"].str.contains("NOTE")!=True].reset_index(drop=True)

# only operating plants
df = df[df["Status"]=='(OP) Operating'].reset_index(drop=True)
# only want utility sector
df = df[df["Sector"]=='Electric Utility'].reset_index(drop=True)

# rename column with ()
df = df.rename(columns = {'Nameplate Capacity (MW)':'Nameplate Capacity - MW'})

# connect to sql
user = 'postgres'
password = 'password'
host= 'localhost'
port = '5432'
database = 'raw'

connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'

#%%

# generate list of all years of data we want to collect

years = list(range(2019,2024))

#median nameplate capacity for each prime mover/gen type - wind and sun only.
# cannot add together capacity