# import pdfkit
import pandas as pd
import calendar
import numpy as np
import datetime as dt
from dateutil.relativedelta import relativedelta
# import pytz

from utils import historicalRateSummary
from utils import subscriber_db_calcs as subscriber_db_calcs
from utils import leapDispatchSummary
from utils import eia_get
from utils import productionAnalysis
# import reformatSettlementData

# from utils.connector import pearlxFlexAPIConnect

# import getAWSSecret

# import s3Functions
import json

def create_rev_metric_report(community, start_date, end_date, s3_client, leap_creds,account_id, db_creds, px_flex_creds, street_address, meters_df):
        
    date_obj = dt.datetime.strptime(start_date, "%Y-%m-%d")
    report_year = date_obj.year
    report_month = date_obj.month

    # Run subscriber database calcs
    subscriber_df,commission_occupied_df = subscriber_db_calcs.runSubscriberCalcs(db_creds, community,start_date,end_date)

    # Process Production Data
    production_data_summary, eventPerformanceSummary_df = productionAnalysis.productionAnalysis(px_flex_creds, community, commission_occupied_df, start_date, end_date)
    
    # Run CARE and Occupancy rates
    subscriber_df['% on CARE'] = subscriber_df['Days with Units on CARE'] / (subscriber_df['Days with Units on CARE'] + subscriber_df['Days with Units Not on CARE'])
    subscriber_df['Occupancy'] = subscriber_df['Commissioned & Occupied Days'] / subscriber_df['Commissioned Days']

    # Format dataframe for output
    subscriber_df['Month'] = subscriber_df['Month'] + ' ' + str(report_year)
    subscriber_df.set_index('Month',inplace = True)
    subscriber_df.replace({np.nan:0},inplace = True)

    subscriber_df['% on CARE'] = subscriber_df['% on CARE'].apply(lambda x: "{:.0%}".format(x))
    subscriber_df['Occupancy'] = subscriber_df['Occupancy'].apply(lambda x: "{:.0%}".format(x))

    subscriber_columns = ['Starting Units Commissioned',
                   'Ending Units Commissioned',
                   'Commissioned & Occupied Days',
                   'Commissioned Days',
                   'Days with Units on CARE',
                   'Days with Units Not on CARE',]
                   
    subscriber_df[subscriber_columns] = subscriber_df[subscriber_columns].astype(int).astype(str)

    subscriber_df = subscriber_df.transpose()

    # get rates
    rate_table_df = historicalRateSummary.runSummarySingle(s3_client, report_year, report_month,'sce','toud4-9').transpose()
    
    # Add day to endtime so that it is exclusive
    analysis_end_local_datetime = pd.to_datetime(end_date,format = '%Y-%m-%d').tz_localize('US/Pacific') + pd.Timedelta(days = 1)
    end_date_exclusive_str = analysis_end_local_datetime.strftime('%Y-%m-%d')

    # Run dispatch summary
    dispatch_summary = leapDispatchSummary.runMonthly(leap_creds, account_id, start_date, end_date_exclusive_str, street_address, meters_df)
    if dispatch_summary.empty:
        dispatch_summary_dict = {'event_energy_wh':'no dispatches this month'}
        dispatch_summary = pd.DataFrame(dispatch_summary_dict, index = [0])

    # Run eia cooling degrees

    cooling_degrees_df = eia_get.getEIACoolingDays(start_date, end_date)
    if cooling_degrees_df.empty:
        cooling_dict= {'seriesDescription':'unable to retrieve cooling degree days'}
        cooling_degrees_df = pd.DataFrame(cooling_dict, index = [0]) 

    if eventPerformanceSummary_df.empty:
        eventPerformanceSummary_dict= {'events':'no PearlX dispatch events returned'}
        eventPerformanceSummary_df = pd.DataFrame(eventPerformanceSummary_dict, index = [0]) 

    # output data into excel file
    fn = "Reports/Monthly Operations " + community + ' ' + start_date.replace('-','') + '_' + end_date.replace('-','') + '.xlsx'
    with pd.ExcelWriter(fn) as writer:
        subscriber_df.to_excel(writer,sheet_name = 'subsSummary')
        rate_table_df.to_excel(writer,sheet_name = 'rateSummary')
        dispatch_summary.to_excel(writer,sheet_name = 'leapSummary')
        production_data_summary.to_excel(writer,sheet_name = 'productionSummary')
        eventPerformanceSummary_df.to_excel(writer,sheet_name = 'dispatchSummary')
        cooling_degrees_df.to_excel(writer, sheet_name = 'CDD Trend - Pacific')
    

    return subscriber_df, rate_table_df, dispatch_summary,production_data_summary, cooling_degrees_df, eventPerformanceSummary_df
if __name__ == "__main__":
    
    import json
    import boto3

    with open("creds.json") as f:
        data=f.read()
    js = json.loads(data)

    client = boto3.client('s3')
    # community = 'Quail Ridge'
    community = 'High Desert Villas'

    start_date_str = '2024-04-01'
    
    # street_adddress = '409 E THORNTON AVE'
    street_adddress = '16850 JASMINE ST'

    with open("credentials_leap.json", "r") as f:
        leap_creds_dict = json.load(f)
    
    account_id = ""
    api_key = leap_creds_dict['api-key']

    headers = {
        "accept": "application/json", 
        "authorization": "Bearer " + api_key,
        "content-type": "application/json"
    }

    meters_df = leapDispatchSummary.getMeterInfo(headers, account_id)
    try:
        for i in range(0,1):
            start_datetime_obj = dt.datetime.strptime(start_date_str, '%Y-%m-%d')
            start_year = start_datetime_obj.year
            start_month = start_datetime_obj.month

            month_day_range = calendar.monthrange(start_datetime_obj.year, start_datetime_obj.month)
            end_date_str = str(start_year) + '-' + str(start_month) + '-' + str(month_day_range[-1])
            end_date_obj = dt.datetime.strptime(end_date_str, '%Y-%m-%d')
            end_date_str = end_date_obj.strftime('%Y-%m-%d')

            start_date_str = start_date_str
            print(end_date_str)
            # create_rev_metric_report(community, start_date_str, end_date_str, client, leap_creds_dict, account_id, street_adddress, meters_df)

            start_datetime_obj = start_datetime_obj + relativedelta(months = 1)
            start_date_str = start_datetime_obj.strftime('%Y-%m-%d')
    except:
        print(start_date_str)
        pass


