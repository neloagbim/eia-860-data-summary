# -*- coding: utf-8 -*-
"""
Created on Sun Jan 14 17:08:29 2024

@author: NeloAgbim
"""

# refactor

import pandas as pd
import numpy as np
from tqdm import tqdm
import requests
import json



#%% refactor with functions
# try error handling function

def request_xlsx(url):
    try:
        r = requests.head(url)
        if r.status_code == 200:
            #response = requests.get(url, 3)
            #df = pd.read_excel(io.BytesIO(response))
            return ("Site exists")
        else:
            return "Site does not exist or server down"
    except r.ConnectionError as e:
        return e

url = 'https://www.eia.gov/electricity/data/eia860m/xls/november_generator2023.xlsx'
url2 = 'https://www.eia.gov/electricity/data/eia860m/xls/november_generator2023.xlsx'

request_xlsx(url)