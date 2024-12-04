import requests

import json
import pandas as pd
from datetime import datetime
import calendar
from pytz import timezone, utc
from datetime import timedelta
 
class Enphase:
    
    def __init__(self,user,creds_file,enphase_username,enphase_password):
        """_summary_

        Args:
            user (str, optional): _description_. Defaults to "Enphase API".
            creds_file (str, optional): _description_. Defaults to 'creds.json'.
        """        
        
        f = open(creds_file)
        creds = json.load(f)
        
        self.url = 'https://api.enphaseenergy.com'
        
        username = creds[user]['user']
        password = creds[user]['password']
        
        endpoint = f'/oauth/token?grant_type=password&username={enphase_username}&password={enphase_password}'
        
        self.auth_headers = {"Content-Type": "application/json; charset=utf-8"}
        
        access_info = requests.post(self.url + endpoint, auth = requests.auth.HTTPBasicAuth(username, password), headers = self.auth_headers)
        
        access_creds = json.loads(access_info.text)
        
        self.access_token = access_creds['access_token']
        
        self.expires_in = access_creds['expires_in']
        
        self.api_key = creds[user]['api_key']
        
        self.headers = {"Content-Type": "application/json; charset=utf-8;","authorization":"Bearer " + self.access_token}
        
        
    def list_systems(self,size=500,page=1):
        """_summary_

        Args:
            size (int, optional): _description_. Defaults to 500.
            page (int, optional): _description_. Defaults to 1.

        Returns:
            _type_: _description_
        """                       
        
        endpoint = f'/api/v4/systems?key={self.api_key}&size={size}&page={page}'
        
        systems_data = requests.get(self.url + endpoint,headers = self.headers)
        
        systems_json = json.loads(systems_data.text)
        
        df_systems = pd.DataFrame(systems_json['systems'])
        
        df_systems['datetime_last_energy_site_local'] = pd.to_datetime(df_systems['last_energy_at'],unit='s')
        df_systems['datetime_last_report_site_local'] = pd.to_datetime(df_systems['last_report_at'],unit='s')
        df_systems['datetime_operational_site_local'] = pd.to_datetime(df_systems['operational_at'],unit='s')

        df_systems['datetime_last_energy_site_local'] = df_systems.apply(lambda x: x['datetime_last_energy_site_local'].tz_localize('utc').tz_convert(tz=x['timezone']),axis=1)
        df_systems['datetime_last_report_site_local'] = df_systems.apply(lambda x: x['datetime_last_report_site_local'].tz_localize('utc').tz_convert(tz=x['timezone']),axis=1)
        df_systems['datetime_operational_site_local'] = df_systems.apply(lambda x: x['datetime_operational_site_local'].tz_localize('utc').tz_convert(tz=x['timezone']),axis=1)
        
        return df_systems
    
    
    def meter_telemetry(self,system_id,start_timestamp,end_timestamp,meter_type='production',granularity='week',site_tz='US/Pacific'):
        """_summary_

        Args:
            system_id (_type_): _description_
            start_timestamp (_type_): _description_
            end_timestamp (_type_): _description_
            meter_type (str, optional): _description_. Defaults to 'production'.
            granularity (str, optional): _description_. Defaults to 'week'.

        Returns:
            _type_: _description_
        """        
        
        assert meter_type == 'production' or meter_type == 'consumption'
        
        if meter_type == 'production':
            meter_input = 'telemetry/production_meter'
            col_names = ['inverter_production_meter','timestamp']
            col_pos = ['timestamp','inverter_production_meter']
        elif meter_type =='consumption':
            meter_input = 'telemetry/consumption_meter'
            col_names = ['inverter_consumption_meter','timestamp']
            col_pos = ['timestamp','inverter_consumption_meter']
            
        site_timezone = timezone(site_tz)
        
        try:
            start_timestamp = datetime.strptime(start_timestamp,'%Y-%m-%d %H:%M:%S')
            start_timestamp_local = site_timezone.localize(start_timestamp).astimezone(utc)
            start_epoch = calendar.timegm(start_timestamp_local.timetuple())
        except ValueError:
            raise
        
        try:
            end_timestamp = datetime.strptime(end_timestamp,'%Y-%m-%d %H:%M:%S')
            end_timestamp_local = site_timezone.localize(end_timestamp).astimezone(utc)
            end_epoch = calendar.timegm(end_timestamp_local.timetuple())
        except ValueError:
            raise
            
        endpoint = f'/api/v4/systems/{system_id}/{meter_input}?key={self.api_key}&start_at={start_epoch}&end_at={end_epoch}&granularity={granularity}'
        
        meter_telemetry_data = requests.get(self.url + endpoint,headers = self.headers)
        
        meter_telemetry_data = json.loads(meter_telemetry_data.text)
        
        df_meter_telemetry = pd.DataFrame(meter_telemetry_data['intervals'])
        
        df_meter_telemetry['end_at_datetime'] = pd.to_datetime(df_meter_telemetry['end_at'],unit='s')
        
        df_meter_telemetry['end_at_datetime_local'] = df_meter_telemetry['end_at_datetime'].apply(lambda x: x.tz_localize('utc').tz_convert(tz=site_tz))
        
        df_meter_telemetry['end_at_datetime_local'] = df_meter_telemetry['end_at_datetime_local'].apply(lambda x: x.replace(tzinfo=None))
        
        df_meter_telemetry.drop(['end_at_datetime','end_at','devices_reporting'],axis=1,inplace=True)
        
        df_meter_telemetry.columns = col_names
        
        df_meter_telemetry = df_meter_telemetry[col_pos]
        
        return df_meter_telemetry
    
    
    def meter_telemetry_exp_imp(self,system_id,start_timestamp,end_timestamp,meter_type='export',granularity='week',site_tz='US/Pacific'):
        """_summary_

        Args:
            system_id (_type_): _description_
            start_timestamp (_type_): _description_
            end_timestamp (_type_): _description_
            meter_type (str, optional): _description_. Defaults to 'export'.
            granularity (str, optional): _description_. Defaults to 'week'.
        """        
        
        assert meter_type == 'export' or meter_type == 'import'
        
        if meter_type =='export':
            meter_input ='energy_export_telemetry'
            col_names = ['inverter_feedin_meter','timestamp']
            col_pos = ['timestamp','inverter_feedin_meter']
        else:
            meter_input ='energy_import_telemetry'
            col_names = ['inverter_purchased_meter','timestamp']
            col_pos = ['timestamp','inverter_purchased_meter']
            
            
        site_timezone = timezone(site_tz)
        
        try:
            start_timestamp = datetime.strptime(start_timestamp,'%Y-%m-%d %H:%M:%S')
            start_timestamp_local = site_timezone.localize(start_timestamp).astimezone(utc)
            # start_timestamp_local = start_timestamp.tz_localize(site_tz).tz_convert(tz='utc')
            start_epoch = calendar.timegm(start_timestamp_local.timetuple())
        except ValueError:
            raise
        
        try:
            end_timestamp = datetime.strptime(end_timestamp,'%Y-%m-%d %H:%M:%S')
            end_timestamp_local = site_timezone.localize(end_timestamp).astimezone(utc)
            # end_timestamp_local = start_timestamp.tz_localize(site_tz).tz_convert(tz='utc')
            end_epoch = calendar.timegm(end_timestamp_local.timetuple())
        except ValueError:
            raise
        
        endpoint = f'/api/v4/systems/{system_id}/{meter_input}?key={self.api_key}&start_at={start_epoch}&end_at={end_epoch}&granularity={granularity}'
        
        meter_telemetry_data = requests.get(self.url + endpoint,headers = self.headers)
        
        meter_telemetry_data = json.loads(meter_telemetry_data.text)['intervals']
        
        meter_telemetry_data = [x for lists in meter_telemetry_data for x in lists]
        
        df_meter_telemetry = pd.DataFrame(meter_telemetry_data)
        
        df_meter_telemetry['end_at_datetime'] = pd.to_datetime(df_meter_telemetry['end_at'],unit='s')
        
        df_meter_telemetry['end_at_datetime_local'] = df_meter_telemetry['end_at_datetime'].apply(lambda x: x.tz_localize('utc').tz_convert(tz=site_tz))
        
        df_meter_telemetry['end_at_datetime_local'] = df_meter_telemetry['end_at_datetime_local'].apply(lambda x: x.replace(tzinfo=None))
        
        df_meter_telemetry.drop(['end_at_datetime','end_at'],axis=1,inplace=True)
        
        df_meter_telemetry.columns = col_names
        
        df_meter_telemetry = df_meter_telemetry[col_pos]
        
        return df_meter_telemetry
    
    
    def production_calc(self,system_id,end_timestamp):
        """_summary_

        Args:
            system_id (_type_): _description_
            end_timestamp (_type_): _description_

        Returns:
            _type_: _description_
        """        
        
        try:
            end_timestamp = datetime.strptime(end_timestamp,'%Y-%m-%d %H:%M:%S')
            end_epoch = calendar.timegm(end_timestamp.timetuple())
        except ValueError:
            raise
        
        endpoint = f'/api/v4/systems/{system_id}/production_meter_readings?key={self.api_key}&end_at={end_epoch}'
        
        production_data = requests.get(self.url + endpoint,headers = self.headers)
        
        production_data = json.loads(production_data.text)
        
        try:
            production_data_value = production_data['meter_readings'][0]['value']
            production_data_readat = production_data['meter_readings'][0]['read_at']
            production_data_readat=datetime.fromtimestamp(production_data_readat)
        except Exception as e:
            print(e)
            production_data_value = 0
            production_data_readat = ''
            
        return production_data_value,production_data_readat
    
    
    def meter_info_lifetime(self,system_id,end_date,meter_type):
        """_summary_

        Args:
            system_id (_type_): _description_
            end_date (_type_): _description_
            meter_type (_type_): _description_

        Returns:
            _type_: _description_
        """                
        
        try:
            end_date_validate = datetime.strptime(end_date,'%Y-%m-%d')
        except ValueError:
            raise
        
        assert meter_type == 'export' or meter_type == 'import' or meter_type == 'production' or meter_type == 'consumption'
        
        endpoint_dict = {
            'export': 'energy_export_lifetime',
            'import': 'energy_import_lifetime',
            'production': 'energy_lifetime',
            'consumption': 'consumption_lifetime'
        }
               
        endpoint_name = endpoint_dict[meter_type]
        
        endpoint = f'/api/v4/systems/{system_id}/{endpoint_name}?key={self.api_key}&end_date={end_date}'
        
        lifetime_data = requests.get(self.url + endpoint,headers = self.headers)
        
        lifetime_dict = json.loads(lifetime_data.text)
        
        lifetime_reading_value = sum(lifetime_dict[meter_type])
        
        return lifetime_reading_value
    
    
    def meter_interval_data_aggregated(self,system_id,start_timestamp,end_timestamp,meter_type='production',granularity='week',site_tz='US/Pacific'):
        
        assert meter_type == 'export' or meter_type == 'import' or meter_type == 'production' or meter_type == 'consumption'
        
        if meter_type == 'production' or meter_type == 'consumption':
            df_meter_telemetry = self.meter_telemetry(system_id,start_timestamp,end_timestamp,meter_type,granularity,site_tz)
        else:
            df_meter_telemetry = self.meter_telemetry_exp_imp(system_id,start_timestamp,end_timestamp,meter_type,granularity,site_tz)
        
        try:
            start_timestamp_val = datetime.strptime(start_timestamp,'%Y-%m-%d %H:%M:%S')
            end_date = start_timestamp_val.date().strftime('%Y-%m-%d')
        except ValueError: 
            raise
        
        lifetime_info = self.meter_info_lifetime(system_id,end_date,meter_type)
        
        telemetry_col = df_meter_telemetry.columns
        
        df_meter_telemetry[telemetry_col[1]] =  df_meter_telemetry[telemetry_col[1]].cumsum()
        
        df_meter_telemetry[telemetry_col[1]] = df_meter_telemetry[telemetry_col[1]] + lifetime_info
        
        return df_meter_telemetry
    
    
    def meter_interval_data_all(self,system_id,start_timestamp,end_timestamp,granularity='week',site_tz='US/Pacific'):
        """_summary_

        Args:
            system_id (_type_): _description_
            start_timestamp (_type_): _description_
            end_timestamp (_type_): _description_
            granularity (str, optional): _description_. Defaults to 'week'.
            site_tz (str, optional): _description_. Defaults to 'US/Pacific'.

        Returns:
            _type_: _description_
        """        
        
        meter_type_list = ['production','consumption','export','import']
        
        df_meter_vals = list()
        
        for meter_type in meter_type_list:
        
            time_start = pd.to_datetime(start_timestamp) - timedelta(minutes=15)

            time_end_org = pd.to_datetime(end_timestamp) + timedelta(minutes=15)

            time_diff = time_end_org - time_start

            time_end = time_end_org

            df_list = list()

            while time_start <= time_end_org:
                
                time_diff = time_end_org - time_start
                
                total_timeval = (time_diff.days)*24*60*60 + (time_diff.seconds)
                
                if total_timeval>=86400*7:
                    time_end = time_start + timedelta(days=7)
                elif time_start == time_end_org: 
                    time_end = time_start + timedelta(minutes=15)
                else:
                    time_end = time_end_org

                time_end_str_time = time_end.strftime(format='%Y-%m-%d %H:%M:%S')
                
                time_start_str_time = time_start.strftime(format='%Y-%m-%d %H:%M:%S')

                time_start = time_end + timedelta(minutes=15)
                
                df_list.append(self.meter_interval_data_aggregated(system_id=system_id,start_timestamp=time_start_str_time,end_timestamp=time_end_str_time,meter_type=meter_type,granularity=granularity,site_tz=site_tz))
            
            int_df = pd.concat(df_list)
            
            int_df.columns = ['Date/Time','Cummulative_Meter']
            
            if meter_type == 'production':
                
                int_df['Energy Produced (Wh)'] =  int_df['Cummulative_Meter'].diff(1)
               
            elif meter_type == 'consumption':
                
                int_df['Energy Consumed (Wh)'] =  int_df['Cummulative_Meter'].diff(1)
                
            elif meter_type == 'export':
                
                int_df['Exported to Grid (Wh)'] =  int_df['Cummulative_Meter'].diff(1)
                
            else:
                
                int_df['Imported from Grid (Wh)'] =  int_df['Cummulative_Meter'].diff(1)
                
            int_df.drop(['Cummulative_Meter'],axis=1,inplace=True)
            
            df_meter_vals.append(int_df)
            
        df_final = df_meter_vals[0].merge(df_meter_vals[1],on='Date/Time',how='left').merge(df_meter_vals[2],on='Date/Time',how='left').merge(df_meter_vals[3],on='Date/Time',how='left')
        
        df_final['Energy Produced (Wh)'] = df_final['Energy Produced (Wh)'].shift(-1)
        df_final['Energy Consumed (Wh)'] = df_final['Energy Consumed (Wh)'].shift(-1)
        df_final['Exported to Grid (Wh)'] = df_final['Exported to Grid (Wh)'].shift(-1)
        df_final['Imported from Grid (Wh)'] = df_final['Imported from Grid (Wh)'].shift(-1)
        
        df_final.dropna(inplace=True)
            
        return df_final
        