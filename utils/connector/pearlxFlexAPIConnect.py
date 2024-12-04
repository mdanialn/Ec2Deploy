import pandas as pd
import requests

import json
import pandas as pd
import datetime as dt
import numpy as np
import pytz 

from .. import getAWSSecret

def getPearlXFlexAnalysisData(id_list, start_date, end_date):

    credentials = getAWSSecret.get_secret("")

    auth = pearlXFlexToken(credentials)
    site_df_list = []

    for site_id, der_id in id_list:

        start_datetime_obj = dt.datetime.strptime(start_date, "%Y-%m-%d").replace(hour=23, minute=00, second=00)

        start_datetime_str = start_datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
        end_datetime_str = end_date + " 23:59:59"

        df = pearlXFlexGetProductionMeterData(auth, site_id, der_id, start_datetime_str, end_datetime_str)

        df['timestamp'] = pd.to_datetime(df['timestamp'],infer_datetime_format=True)

        df['inverter_production_meter'] = df['inverter_production_meter'].astype(float)
        
        # Calculate Wh production for current interval
        df['Wh'] = df['inverter_production_meter'].diff()
        
        df.rename(columns = {'timestamp':'timestamp_end'},inplace = True)
        
        # Add timestamp start
        df['timestamp_start'] = df.shift(1)

        # filter for only intervals that are within the analysis time range
        df = df.loc[df['timestamp_start']>= start_date]

        # drop empty rows
        df.dropna(subset='Wh',inplace = True)
        
        df = df[['timestamp_start','timestamp_end','Wh']]
        
        site_df_list.append(df)

    out_df = pd.concat(site_df_list)

    return out_df

def pearlXFlexToken(credentials):

    url = 'https://flextrons.io/api/v1/auth/login'

    headers = {'accept': 'application/json','Content-Type': 'application/json'}
    data = {"email": "", 'password':""}
    auth = requests.post(url, headers = headers, data = json.dumps(data)).json()

    return auth



def pearlXFlexGetSites(auth):

    sites_url = 'https://flextrons.io/api/v1/sites'

    headers = {'accept': 'application/json', 'Authorization':'Bearer {}'.format(auth['token'])}

    # Get sites list
    sites_response = requests.get(sites_url,headers = headers).json()

    return pd.DataFrame(sites_response)

def pearlXFlexDERId(auth, site_id):

    headers = {'accept': 'application/json', 'Authorization':'Bearer {}'.format(auth['token'])}
    ders_url = f'https://flextrons.io/api/v1/sites/{site_id}/ders'

    # get der ids
    der_response = requests.get(ders_url,headers = headers)
    der_response_data = der_response.json()

    return pd.DataFrame(der_response_data)

def pearlXFlexGetProductionMeterData(auth, site_id, der_id, data_type, start_date, end_date):

    # Local time
    headers = {'accept': 'application/json', 'Authorization':'Bearer {}'.format(auth['token'])}
    
    ## Datetime format %Y-%m-%d %H:%M:%S
    start_time_obj = dt.datetime.strptime(start_date,"%Y-%m-%d")
    end_time_obj = dt.datetime.strptime(end_date,"%Y-%m-%d").replace(hour = 0, minute=0, second=0)

    ## offset by 1 hour because data is in meter read time ending
    start_time_obj = start_time_obj -  dt.timedelta(hours = 1)
    end_time_obj = end_time_obj + dt.timedelta(hours = 1)

    response_df_list = []
    
    start_time = start_time_obj.strftime('%Y-%m-%d %H:%M:%S')

    # initialize end_time_pull_obj
    end_time_pull_obj = start_time_obj

    while end_time_pull_obj < end_time_obj:

        start_time_pull_obj = dt.datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S")

        end_time_pull_obj = start_time_pull_obj + dt.timedelta(days = 31)
        end_time_pull_obj = np.min([end_time_pull_obj,end_time_obj])

        start_time = start_time_pull_obj.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time_pull_obj.strftime("%Y-%m-%d %H:%M:%S")
        
        data_pull_url = 'https://flextrons.io/api/v1/sites/{site_id}/ders/{der_id}/{data_type}?start_time={start_time}&end_time={end_time}'.format(
            site_id = site_id,der_id=der_id,start_time=start_time,end_time=end_time, data_type = data_type)

        data_response = requests.get(data_pull_url,headers = headers).json()
        df_response = pd.DataFrame(data_response)
        
        response_df_list.append(df_response)
        
        start_time = end_time
        
    output_df = pd.concat(response_df_list)
    output_df.drop_duplicates(subset=['timestamp'],inplace = True)
    return output_df

def pearlXFlexGetEvents(auth, site_id, storage_id, start_date, end_date):

    # Local time
    headers = {'accept': 'application/json', 'Authorization':'Bearer {}'.format(auth['token'])}    
    
    # Add start and end datae for pulling events
    analysis_start_local_datetime = pd.to_datetime(start_date,format = '%Y-%m-%d').tz_localize('US/Pacific')
    analysis_start_utc_datetime = analysis_start_local_datetime.astimezone(pytz.utc)
    analysis_start_utc_str = analysis_start_utc_datetime.strftime('%Y-%m-%dT%H:%M:%S') + "Z"

    analysis_end_local_datetime = pd.to_datetime(end_date,format = '%Y-%m-%d').tz_localize('US/Pacific')
    analysis_end_utc_datetime = analysis_end_local_datetime.astimezone(pytz.utc)
    analysis_end_utc_str = analysis_end_utc_datetime.strftime('%Y-%m-%dT%H:%M:%S') + "Z"
    der_events_url = f"https://flextrons.io/api/v1/sites/{site_id}/ders/{storage_id}/der-events?from={analysis_start_utc_str}&to={analysis_end_utc_str}"

    data_response = requests.get(der_events_url,headers = headers).json()
    
    df_response = pd.DataFrame(data_response)
    if not df_response.empty:
        df_response['startTime'] = pd.to_datetime(df_response['startTime'], format = 'ISO8601')
        df_response['endTime'] = pd.to_datetime(df_response['endTime'], format = 'ISO8601')
        df_response['startTimeLocal'] = df_response['startTime'].dt.tz_convert("US/Pacific")
        df_response['endTimeLocal'] = df_response['endTime'].dt.tz_convert("US/Pacific")
        df_response['startTimeLocal'] = df_response['startTimeLocal'].dt.tz_localize(None)
        df_response['endTimeLocal'] = df_response['endTimeLocal'].dt.tz_localize(None)

    return df_response

