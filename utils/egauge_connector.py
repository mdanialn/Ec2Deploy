import pandas as pd
import requests
import xmltodict
import calendar
from datetime import datetime, timezone
import pytz

def get_query_time(timestart,timeend,timezone_str):
    
    format = '%Y-%m-%d %H:%M:%S'

    try:
        assert bool(datetime.strptime(timestart, format))
    except ValueError:
        raise

    try:
        assert bool(datetime.strptime(timestart, format))
    except ValueError:
        raise
    
    timestartUTC = pd.to_datetime(timestart)
    
    timestartUTC = timestartUTC.replace(tzinfo = pytz.timezone(timezone_str))
    tz = timezone.utc

    timestartUTC = timestartUTC.astimezone(tz)
    
    timestarttuple = calendar.timegm(timestartUTC.timetuple())
    
    timeendUTC = pd.to_datetime(timeend)
    timeendUTC = timeendUTC.replace(tzinfo = pytz.timezone(timezone_str))
    tz = timezone.utc

    timeendUTC = timeendUTC.astimezone(tz)

    timeendtuple = calendar.timegm(timeendUTC.timetuple())
    
    try:
        assert timestarttuple<=timeendtuple
    except:
        print("Starttime entered greater than Endtime")
        raise
    
    return timestarttuple, timeendtuple, timezone_str, timestart, timeend


def get_usage_consumption_data(timestarttuple,timeendtuple,timezone_string,egauge_name):
    
    try:
        assert timestarttuple is not None and timeendtuple is not None and timezone_string is not None
    except:
        raise
    
    i = 0

    timeval = timestarttuple

    values_list = list()

    epoch_time = list()
    
    while timeval<=timeendtuple:
        url = "https://{}.d.egauge.net/cgi-bin/egauge-show?a&E&T={}".format(egauge_name,timeval)
        
        data = requests.get(url)
        
        try:
            assert data.status_code == 200
        except:
            print("Device {} not found on this server".format(egauge_name))
            raise  
        
        tree = xmltodict.parse(data.content)
        
        if i == 0:
        
            columns = list()

            for dictval in tree['group']['data']['cname']:
            
                columns.append(dictval['#text'])
        
        values1 = tree['group']['data']['r']['c']

        values1 = [float(x) for x in values1]

        values1_kWh = [x/3600000 for x in values1]

        values_list.append(values1_kWh)

        epoch_time.append(timeval)
        
        timeval += 900
        
        i += 1

    df = pd.DataFrame(values_list)
    df.columns = columns

    df['timestamp_utc'] = epoch_time
    
    df['datetime_{}'.format(timezone_string)] = pd.to_datetime(df['timestamp_utc'],unit='s').apply(lambda x: x.tz_localize('UTC').astimezone(timezone_string))
    
    # return df
    
    return df[list(df.columns[-2:]) + list(df.columns[:-2])],egauge_name

if __name__ == "__main__":
    ##EXAMPLE
    #Format: YYYY-MM-DD hh:mm:ss
    t_start='2023-08-11 00:00:00'
    t_end='2023-08-11 01:00:00'
    #1 for 'America/Los_Angeles', 2 for 'America/New_York' or 3 for 'America/Chicago'
    t_zone='America/Los_Angeles'
    #Egauge Name (Example: egauge90245):
    name='egauge90242'

    timestarttuple, timeendtuple, timezone_string,timestart, timeend=get_query_time(t_start,t_end,t_zone)
    df,egauge_name = get_usage_consumption_data(timestarttuple, timeendtuple, timezone_string,name)
    
