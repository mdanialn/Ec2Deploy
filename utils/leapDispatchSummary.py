import pandas as pd
import json
import requests
import pytz

def runMonthly(credentials, account_id, analysis_start_local, analysis_end_local, community_street_address, meters_df):

    api_key = credentials['api-key']

    headers = {
        "accept": "application/json", 
        "authorization": "Bearer " + api_key,
        "content-type": "application/json"
    }

    # Convert analysis local time to utc
    analysis_start_local_datetime = pd.to_datetime(analysis_start_local).tz_localize('US/Pacific')
    analysis_start_utc_datetime = analysis_start_local_datetime.astimezone(pytz.utc)
    analysis_start_utc_str = analysis_start_utc_datetime.isoformat().replace("+00:00", "Z")

    analysis_end_local_datetime = pd.to_datetime(analysis_end_local).tz_localize('US/Pacific')
    analysis_end_utc_datetime = analysis_end_local_datetime.astimezone(pytz.utc)
    analysis_end_utc_str = analysis_end_utc_datetime.isoformat().replace("+00:00", "Z")

    # # Search meters in leap api
    # meters_df = getMeterInfo(headers, account_id)

    # get all events within time range from leap api
    response_events = getEvents(headers, analysis_start_utc_str,analysis_end_utc_str)

    if response_events['results']:
    # Get meter ids of all events
        dispatches_df = pd.DataFrame(response_events['results'])
        meter_ids = dispatches_df['meter_id'].tolist()

        # Get event performance data from leap api
        event_performance = getEventPerformance(headers, meter_ids, analysis_start_utc_str, analysis_end_utc_str)

        # get performance data from leap
        event_performance_summary = eventPerformanceSummary(event_performance)

        # map meter id's in performance data over to meter id's and service address in meter dataframe
        out_df = event_performance_summary.merge(meters_df[['meter_id','service_address_full']], left_on = ['meter_id'], right_on = ['meter_id'], how = 'left')

        # only return rows for the specific street address input
        out_df = out_df.loc[out_df['service_address_full'].str.contains(community_street_address)]
        
        return out_df
    else:
        return pd.DataFrame()


def getEvents(headers, start_time_utc_str, end_time_utc_str):
    
        url = "https://api.leap.energy/v2/dispatch/meter/search"

        payload = {
            "start_date": start_time_utc_str,
            "end_date": end_time_utc_str
        }

        response = requests.post(url, json=payload, headers=headers)
    
        response_dict = response.json()

        return response_dict

def getEventPerformance(headers, meter_ids, start_time_utc_str, end_time_utc_str):

    url = "https://api.leap.energy/v1.1/analytics/meter-performance"

    payload = {'date_range_start': start_time_utc_str,
            'date_range_end':end_time_utc_str,
            'meter_ids':meter_ids}

    response = requests.post(url, headers = headers,json = payload)
    response_dict = response.json()

    return response_dict

def eventPerformanceSummary(performance_dict):
     
    meter_perf_list = []
    for meter in performance_dict['meters']:
        meter_id = meter['meter_id']
        summary = meter['summary']
        summary['meter_id'] = meter_id
        meter_perf_list.append(summary)

    return pd.DataFrame(meter_perf_list)

def getMeterInfo(headers, account_id):
     
    # Get meters
    url = "https://api.leap.energy/v1.1/account/{account_id}/meters/search".format(account_id = account_id)

    response = requests.post(url, headers=headers)
    meters_df = pd.DataFrame(response.json()['results'])

    return meters_df



if __name__ == "__main__":

    with open("credentials_leap.json", "r") as f:
        my_dict = json.load(f)
    api_key = my_dict['api-key']
    account_id = "050b8f48-2c38-4fd7-8368-cd14fa5d07f8"

    headers = {
        "accept": "application/json", 
        "authorization": "Bearer " + api_key,
        "content-type": "application/json"
    }

    # starttime hardcoded
    analysis_start_local = '2024-01-01 00:00:00'
    analysis_end_local = '2024-02-01 00:00:00'

    street_address = '16850 JASMINE ST'
    meters_df = getMeterInfo(headers, account_id)
    out_df = runMonthly(my_dict, account_id, analysis_start_local, analysis_end_local, street_address, meters_df)
    print(out_df)


