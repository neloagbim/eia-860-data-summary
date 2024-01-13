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
from sqlalchemy import (MetaData,Table, Column, Text, Integer, Numeric, Boolean, VARCHAR)

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
       'Sector', 'Generator ID', 'Nameplate Capacity (MW)',
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

# string together connction details
connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'

#create sql engine and metadata object
engine = create_engine(connection_string)
metadata = MetaData()

reports_2013 = Table('reports_2013', 
                     metadata, 
                     Column('entity_id', Integer()),
                     Column('plant_nane', Text()),
                     Column('plant_id', Integer()),
                     Column('state',VARCHAR(2)),
                     Column('county',Text()),
                     Column('balancing_auth',Text()),
                     Column('sector',Text()),
                     Column('unit_id',Text()),
                     Column('nameplate_capacity_mw',Numeric()),
                     Column('technology',Text()),
                     Column('fuel_source',Text()),
                     Column('prime_mover',Text()),
                     Column('op_month',Integer()), 
                     Column('op_year',Integer()),
                     Column('retire_month',Integer()),
                     Column('retire_year', Integer()),
                     Column('op_status', Text()),
                     Column('report_date', Text()),
                     schema= 'eia860'
                     )
# create table
metadata.create_all(engine)
#%%

# generate list of all years of data we want to collect

years = list(range(2019,2024))

months = ['january',
'february',
'march',
'april',
'may',
'june',
'July' ,
'august',
'september',
'october',
'november',
'december']

for yr in range(0,len(years)):
    for mnth in range(0,len(months)):
        if years[yr]==2023 and months[mnth]=="november":
            #url to download
            url = 'https://www.eia.gov/electricity/data/eia860m/xls/november_generator2023.xlsx'

#median nameplate capacity for each prime mover/gen type - wind and sun only.
# cannot add together capacity