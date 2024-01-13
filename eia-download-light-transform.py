# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:59:30 2024

@author: NeloAgbim
"""

import pandas as pd
import tqdm
import requests
import json
import openpyxl
import io
import urllib.parse
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import (MetaData,Table, Column, Text, Integer, Numeric, Boolean, VARCHAR)

# #request download
# url = 'https://www.eia.gov/electricity/data/eia860m/xls/november_generator2023.xlsx'
# response = requests.get(url)

# # wrap request response in IO wrapper to get xlsx read in
# df = pd.read_excel(io.BytesIO(response.content), skiprows=2, sheet_name = 0)

# df.dtypes

# file_name = url.split('xls/',2)[1]
# file_date = file_name.replace("_generator", "-").replace(".xlsx", "")

# # save eia file date as 
# df["Report Date"] = file_date


# # keep only listed columns
# df = df[cols]

# # remove rows without an entity id
# df = df[df["Entity ID"].isna()==False].reset_index(drop=True)

# #rempve row with notes
# df = df[df["Entity ID"].str.contains("NOTE")!=True].reset_index(drop=True)

# # only operating plants
# df = df[df["Status"]=='(OP) Operating'].reset_index(drop=True)
# # only want utility sector
# df = df[df["Sector"]=='Electric Utility'].reset_index(drop=True)

# # rename column with ()
# df = df.rename(columns = {'Nameplate Capacity (MW)':'Nameplate Capacity - MW'})

# read in secrets file
secrets = open(r'C:\Users\NeloAgbim\Documents\PythonPrjEnvs\eiasummary\secrets.json')

# save as a credentials dictionary
cred = json.load(secrets)

# connect to sql
user = cred['raw']['user']
password = cred['raw']['password']
host= cred['raw']['host']
port = cred['raw']['port']
database = cred['raw']['database']

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

engine.close()
#%%

# define columns to go in final dataframe for sql upload

final_cols = ['entity_id', 
'plant_nane', 
'plant_id', 
'state',
'county',
'balancing_auth',
'sector',
'unit_id',
'nameplate_capacity_mw',
'technology',
'fuel_source',
'prime_mover',
'op_month', 
'op_year',
'retire_month',
'retire_year', 
'op_status', 
'report_date' ]


# create dataframe to hold eia data
df_eia = pd.DataFrame(columns=final_cols)


# generate list of all years of data we want to collect

years = list(range(2019,2024))

# generate list of all months
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

#  necessary columns from download
cols = ['Entity ID', 'Entity Name', 'Plant ID', 'Plant Name', 'Plant State',
        'County', 'Balancing Authority Code',
       'Sector', 'Generator ID', 'Nameplate Capacity (MW)',
       'Technology','Energy Source Code', 'Prime Mover Code', 'Operating Month',
       'Operating Year', 'Planned Retirement Month', 'Planned Retirement Year',
       'Status', 'Report Date']

# create a dataframe of eia data for each year
for yr in range(0,len(years)):
    for mnth in range(0,len(months)):
        if years[yr]==2023 and months[mnth]=="november":
            #url to download
            url = 'https://www.eia.gov/electricity/data/eia860m/xls/november_generator2023.xlsx'
        else:
            #url to download
            url = 'https://www.eia.gov/electricity/data/eia860m/archive/xls/'+months[mnth] + '_generator'+str(years[yr])+'.xlsx'
        # request download
        response = requests.get(url)
        # wrap request response in IO wrapper to get xlsx read in
        df = pd.read_excel(io.BytesIO(response.content), skiprows=2, sheet_name = 0)
        # get file name
        file_name = url.split('xls/',2)[1]
        # extract month and year
        file_date = file_name.replace("_generator", "-").replace(".xlsx", "")

        # save eia file date as 
        df["Report Date"] = file_date
        # keep only listed columns
        df = df[cols]

        # remove rows without an entity id
        df = df[df["Entity ID"].isna()==False].reset_index(drop=True)
        #remove row with notes
        df = df[df["Entity ID"].str.contains("NOTE")!=True].reset_index(drop=True)
        # only operating plants
        df = df[df["Status"]=='(OP) Operating'].reset_index(drop=True)
        # only want utility sector
        df = df[df["Sector"]=='Electric Utility'].reset_index(drop=True)
        # rename column with ()
        df = df.rename(columns = {'Nameplate Capacity (MW)':'Nameplate Capacity - MW'})
    # after cleaning data for a given month, add it to the dataframe for the year
    df_eia.append(df)
    
    del df

#median nameplate capacity for each prime mover/gen type - wind and sun only.
# cannot add together capacity