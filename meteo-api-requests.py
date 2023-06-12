#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
import pandas as pd
import numpy as np
import json
import time
import psycopg2
from sqlalchemy import create_engine


# In[3]:


conn = psycopg2.connect(
    database="database",
    user="postgres",
    password="password",
    host="localhost",
    port="0000"
)


# In[4]:


engine = create_engine("postgresql://postgres:password@localhost:password/database")


# In[5]:


stations_response = requests.get('https://api.meteo.lt/v1/stations')


# In[6]:


stations_json = stations_response.json()
df_stations = pd.DataFrame(stations_json)


# In[8]:


def get_stations_observations(station_name, date_from, date_to):
    dates_list = [d.strftime('%Y-%m-%d') for d in pd.date_range(date_from, date_to)]
    append_data = []
    for date in dates_list:
        retries = 0
        response = None
        while retries < 5:
            response = requests.get(f'https://api.meteo.lt/v1/stations/{station_name}/observations/{date}')
            if response.status_code == 200:
                # Request was successful
                break
            else:
                # Request failed
                retries += 1
                time.sleep(1)  # Wait for 1 second before retrying
        
        if response is not None and response.status_code == 200:
            observations = response.json()
            df = pd.DataFrame.from_dict(observations.get('observations'))
            df['station_code'] = station_name
            append_data.append(df)
            time.sleep(1.0)
        else:
            print(f"Max retry limit reached for date {date}. Unable to make a successful request.")

    df_observations = pd.concat(append_data)
    return df_observations


# In[9]:


utena_obs = get_stations_observations('utenos-ams', '2014-01-01', '2023-01-01')


# In[10]:


utena_obs.to_sql('utena', engine, if_exists='replace', index=False)


# In[ ]:




