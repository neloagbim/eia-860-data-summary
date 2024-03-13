# -*- coding: utf-8 -*-
"""
Created on Mon Mar 11 16:08:01 2024

@author: NeloAgbim
"""

import pandas as pd
import streamlit as st
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# page configuration
st.set_page_config(
    page_title="Nelo's EIA860 Dashboard",
    page_icon= ":bulb:",
    layout= "wide"
)

st.title("EIA860 2019-2023 Plant Summary")
st.write("by Nelo Agbim")

# connect to google to get data 
# scope of where api will work
scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

# read in google sheet credentials
#credentials = Credentials.from_service_account_file(r'C:\Users\NeloAgbim\Documents\PythonPrjEnvs\eiasummary\github\copy-of-google-sheets-secrets.json',  scopes=scopes)
credentials = Credentials.from_service_account_info(st.secrets["google_sheet_creds"],  scopes=scopes)

gc = gspread.authorize(credentials)

#@st.cache_data
# write function for retrieving google sheet data
def load_sheet_data(url,sheetname):
    # open the google sheet
    gs = gc.open_by_url(url)
    # select a sheet by its name
    worksheet = gs.worksheet(sheetname)
    # get data from google sheet
    sheet1 = worksheet.get_all_records() 
    df = pd.DataFrame(sheet1)
    return df

# get data from google sheet

url = 'https://docs.google.com/spreadsheets/d/1ddigiLsVZdPGpCO0IAeFFgjaQcAguABD3n9HaQzgL4E/edit#gid=0'
df = load_sheet_data(url, sheetname = 'dataset')

df= df[df["op_year"]>2015].reset_index(drop=True)


# create 2 columns
col1,col2 = st.columns(2) 

with col1:
    # capacity parameter
    capacity = st.radio("Choose Namplate Capacity Calculation",["Median Capacity","Average Capacity"])
with col2:
    # time period selection
    period = st.selectbox ("Aggregate Data Yearly or Quarterly?", ("Yearly","Quarterly"))

# display streamlit df object
st.dataframe(df)