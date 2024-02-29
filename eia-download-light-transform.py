# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 14:59:30 2024

@author: NeloAgbim
"""
#REFACTOR NEEDED
#%% Install libraries

import pandas as pd
import numpy as np
from tqdm import tqdm
import requests
import json
import io
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import (MetaData,Table, Column, Text, Integer, Numeric, VARCHAR)
from datetime import datetime
import time

#%%
# setup

# get current date
current_date = datetime.today()

# day december 2023 data will be released
release_date = "2024-01-24"
release_date = datetime.strptime(release_date,"%Y-%m-%d")

# date jan data will prob be released
feb_release = "2024-02-22"
feb_release = datetime.strptime(feb_release, "%Y-%m-%d")

# generate list of all years of data we want to collect
years = list(range(2019,2024))

# generate list of all months
months = ['january',
'february',
'march',
'april',
'may',
'june',
'july' ,
'august',
'september',
'october',
'november',
'december']

#%% Connecting to Postgresql
# read in secrets file
secrets = open(r'C:\Users\NeloAgbim\Documents\PythonPrjEnvs\eiasummary\github\secrets.json')

# save as a credentials dictionary
cred = json.load(secrets)

# connect to sql
user = cred['db_creds']['user']
password = cred['db_creds']['password']
host= cred['db_creds']['host']
port = cred['db_creds']['port']
database = cred['db_creds']['database']

# string together connction details
connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'

#create sql engine and metadata object
engine = create_engine(connection_string)
metadata = MetaData()
#if the release date hasn't come (i.e. first run) and the feb release date hasn't come either
if current_date < release_date and current_date < feb_release: 
    # create 5 tables needed for 2019-2023
    for x in range(0,len(years)):
        reports = Table('reports_'+str(years[x]), 
                             metadata, 
                             Column('entity_id', Integer()),
                             Column('entity_name',Text(())),
                             Column('plant_id', Integer()),
                             Column('plant_name', Text()),
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
        #delete report variable
        del reports

#%%

#  necessary columns from download
cols = ['Entity ID', 'Entity Name', 'Plant ID', 'Plant Name', 'Plant State',
        'County', 'Balancing Authority Code',
       'Sector', 'Generator ID', 'Nameplate Capacity (MW)',
       'Technology','Energy Source Code', 'Prime Mover Code', 'Operating Month',
       'Operating Year', 'Planned Retirement Month', 'Planned Retirement Year',
       'Status', 'Report Date']

yr=0
mnth=0
#if the release date hasn't come (i.e. first run)
if current_date < release_date: 
    # create a dataframe of eia data for each year
    for yr in tqdm(range(0,len(years))):
        for mnth in tqdm(range(0,len(months))):
            # END inner loop if the before release date and the loop is going to execute dec 2023 data scrape
            if years[yr]==2023 and months[mnth]=="december":
                break
            # if before dec release, nov will not have 'archive' in url
            if years[yr]==2023 and months[mnth]=="november":
                #url to download
                url = 'https://www.eia.gov/electricity/data/eia860m/xls/november_generator2023.xlsx'
            else:
                #url to download
                url = 'https://www.eia.gov/electricity/data/eia860m/archive/xls/'+months[mnth] + '_generator'+str(years[yr])+'.xlsx'
            #test url
            r = requests.head(url)
            # if http connection worked
            if r.status_code == 200:
                # request download and timeout after 3 seconds
                response = requests.get(url,timeout = 3)
                # wrap request response in IO wrapper to get xlsx read in
                df = pd.read_excel(io.BytesIO(response.content), skiprows=1, sheet_name = 0) # skiprows=2,
    
                # if column headers are the 1st row (2nd before importing)
                if df.iloc[0,0] == 'Entity ID':
                    # then set the 1st row as the column header
                    df.columns = df.iloc[0]
                    # delete the row from the df
                    df = df.iloc[1:,:]
                # Then get file name
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
                
                #rename columns
                df = df.rename( columns= {'Entity ID':'entity_id', 
                    'Entity Name':'entity_name', 
                    'Plant ID':'plant_id', 
                    'Plant Name':'plant_name',
                    'Plant State':'state',
                    'County':'county',
                    'Balancing Authority Code':'balancing_auth',
                    'Sector':'sector',
                    'Generator ID':'unit_id',
                    'Nameplate Capacity (MW)':'nameplate_capacity_mw',
                    'Technology':'technology',
                    'Energy Source Code':'fuel_source',
                    'Prime Mover Code':'prime_mover',
                    'Operating Month':'op_month', 
                    'Operating Year':'op_year',
                    'Planned Retirement Month':'retire_month',
                    'Planned Retirement Year':'retire_year', 
                    'Status':'op_status', 
                    'Report Date':'report_date' })
                # replace blanks
                df['retire_year'] = df['retire_year'].replace(' ','0')
                df['retire_year'] =df['retire_year'].astype('str')
                df['retire_year'] = df['retire_year'].str.replace('.0','')
                
                #eplace space in  month
                df['retire_month'] = df['retire_month'].replace(' ','0')
                # get rid of decimals
                df['retire_month'] =df['retire_month'].astype('str')
                df['retire_month'] = df['retire_month'].str.replace('.0','')
                # remove 'nan' string
                df['retire_month'] = df['retire_month'].str.replace('nan','0')
                
                #nameplate capacity
                df['nameplate_capacity_mw'] = df['nameplate_capacity_mw'].replace(' ',np.nan)
                
                df = df.astype({'entity_id':'int', 'entity_name':'str', 'plant_id':'int', 'plant_name':'str', 'state':'str', 'county':'str',
                       'balancing_auth':'str', 'sector':'str', 'unit_id':'str', 'nameplate_capacity_mw':'float',
                       'technology':'str', 'fuel_source':'str', 'prime_mover':'str', 'op_month':'int', 'op_year':'int',
                       'retire_month':'int', 'retire_year':'int', 'op_status':'str', 'report_date':'str'})
            # if the loop is on the first month of the year
            if mnth == 0:
                # initiate df_eia wchich will hold all eia data for that year
                df_eia = df
            
            # otherwise append the df    
            else:
                # after cleaning data for a given month, add it to the dataframe for the year
                df_eia = pd.concat([df_eia,df],axis=0,ignore_index=True) #ignore index to ensure reset
    
            # delete df
            del df, file_date
            # wait a few seconds
            time.sleep(2.5)
        # put df in table
        df_eia.to_sql('reports_'+str(years[yr]),schema='eia860', con=engine,if_exists = 'append', index=False)
else: # the december 2023 data released
    url = 'https://www.eia.gov/electricity/data/eia860m/xls/december_generator2023.xlsx'
    # test url
    r = requests.head(url)
    # if http connection worked
    if r.status_code == 200:
        # request download and timeout after 3 seconds
        response = requests.get(url,timeout = 3)
        # wrap request response in IO wrapper to get xlsx read in
        df = pd.read_excel(io.BytesIO(response.content), skiprows=1, sheet_name = 0) # skiprows=2,

        # if column headers are the 1st row (2nd before importing)
        if df.iloc[0,0] == 'Entity ID':
            # then set the 1st row as the column header
            df.columns = df.iloc[0]
            # delete the row from the df
            df = df.iloc[1:,:]
        # Then get file name
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
        
        #rename columns
        df = df.rename( columns= {'Entity ID':'entity_id', 
            'Entity Name':'entity_name', 
            'Plant ID':'plant_id', 
            'Plant Name':'plant_name',
            'Plant State':'state',
            'County':'county',
            'Balancing Authority Code':'balancing_auth',
            'Sector':'sector',
            'Generator ID':'unit_id',
            'Nameplate Capacity (MW)':'nameplate_capacity_mw',
            'Technology':'technology',
            'Energy Source Code':'fuel_source',
            'Prime Mover Code':'prime_mover',
            'Operating Month':'op_month', 
            'Operating Year':'op_year',
            'Planned Retirement Month':'retire_month',
            'Planned Retirement Year':'retire_year', 
            'Status':'op_status', 
            'Report Date':'report_date' })
        # replace blanks
        df['retire_year'] = df['retire_year'].replace(' ','0')
        df['retire_year'] =df['retire_year'].astype('str')
        df['retire_year'] = df['retire_year'].str.replace('.0','')
        
        #eplace space in  month
        df['retire_month'] = df['retire_month'].replace(' ','0')
        # get rid of decimals
        df['retire_month'] =df['retire_month'].astype('str')
        df['retire_month'] = df['retire_month'].str.replace('.0','')
        # remove 'nan' string
        df['retire_month'] = df['retire_month'].str.replace('nan','0')
        
        #nameplate capacity
        df['nameplate_capacity_mw'] = df['nameplate_capacity_mw'].replace(' ',np.nan)
        
        df = df.astype({'entity_id':'int', 'entity_name':'str', 'plant_id':'int', 'plant_name':'str', 'state':'str', 'county':'str',
               'balancing_auth':'str', 'sector':'str', 'unit_id':'str', 'nameplate_capacity_mw':'float',
               'technology':'str', 'fuel_source':'str', 'prime_mover':'str', 'op_month':'int', 'op_year':'int',
               'retire_month':'int', 'retire_year':'int', 'op_status':'str', 'report_date':'str'})

        # put df in table
        df.to_sql('reports_2023',schema='raw', con=engine,if_exists = 'append', index=False)
    