# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 12:08:21 2023

@author: kshin
"""

import pandas as pd
import io
import boto3
import datetime as dt
import numpy as np


def pull_csv(s3_client, bucket_name, key):
    
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    csv_body = csv_obj['Body'].read()
    csv_df = pd.read_csv(io.BytesIO(csv_body))
    
    return csv_df


def getHistoricalReportFilesS3(s3_client, year, month, community):

    """ get most recent report data to current report
    
    Arguments:

        year - int as YYYY
        month - int as M
        community - str

    Keyword Arguments:

    Returns:
        [pandas dataframe] -- df with data from most recent report run to the report year and month
    """  
    
    prefix = 'report-data/' + community 

    bucket_name = 'revenue-tracking-dev'

    response = s3_client.list_objects(
        Bucket = bucket_name,Prefix = prefix
     )
    all_files = [file for file in response['Contents'] if '.csv' in file['Key']]
    report_date_list = [file['Key'].split('/')[2].split('.csv')[0] for file in all_files]

    dates_list = list(map(lambda x: dt.datetime.strptime(x,'%Y-%m'), report_date_list))
    dates_array = np.array(dates_list)

    date_str = str(year) + "-" + str(month)
    date_obj = dt.datetime.strptime(date_str, "%Y-%m")

    dates_array = max(dates_array[dates_array<=date_obj])
    historical_report_date = dates_array.strftime("%Y-%m")

    fn = [file['Key'] for file in all_files if historical_report_date in file['Key']][0]
    
    df = pull_csv(s3_client=s3_client, bucket_name=bucket_name, key=fn)
    df.drop(columns = ['Unnamed: 0'],inplace = True)

    return df


def getS3Rates(s3_client, start_date,end_date, iou, rate):

    """ get rates from s3
    
    Arguments:

        start_date  (inclusive)- str as %m/%d/%y
        end_date (inclusive) - str as %m/%d/%y
        iou - {sce, pge, sdge}
        rate - {toud4-9, toud5-8, e-elec}

    Keyword Arguments:

    Returns:
        [pandas dataframe] -- df with all rates from start date to end date
    """  

    start_date_obj = dt.datetime.strptime(start_date, '%m/%d/%Y')
    end_date_obj = dt.datetime.strptime(end_date,'%m/%d/%Y')

    rate_prefix = 'iou-rates/' + iou + '/' + rate

    bucket_name = 'processedbucket-iou-prod'

    response = s3_client.list_objects(
        Bucket = bucket_name,Prefix = rate_prefix
     )

    all_files = [file for file in response['Contents'] if '.csv' in file['Key']]
    
    rate_date_list = [file['Key'].split('/')[3].split('.csv')[0] for file in all_files]
    rate_date_list = list(map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d'),rate_date_list))

    dates_array = np.array(rate_date_list)

    dates_array = list(dates_array[(dates_array>=start_date_obj) & (dates_array<=end_date_obj)])
    dates_list = list(map(lambda x: x.strftime('%Y-%m-%d'), dates_array))

    # Get latest rates
    keys_list = [rate_prefix + '/' + date + '.csv' for date in dates_list][-1:]

    rate_df_list = []
    
    for key in keys_list:

        df = pull_csv(s3_client=s3_client, bucket_name=bucket_name, key=key)
        rate_df_list.append(df)

    output_df = pd.concat(rate_df_list)

    return output_df

def df_concat(startDate, endDate, accountNumber, s3_client, bucket_name):
    
    '''
        this function takes a start and end date, and pulls the needed usage data for the given accountNumber
        from s3 and creates a concatenated df of the usage data for the given months
        it then returns that df
        
        Arguments:
            startDate: datetime
            endDate: datetime
            accountNumber: int
            bucket_name: string
            
        Returns:
            concat_df: dataframe
    '''
    
    start_month = startDate.replace(day = 1).strftime("%m/%d/%y")
    end_month = endDate.replace(day = 1).strftime("%m/%d/%y")

    month_list = pd.date_range(start_month,end_month, 
          freq='MS').strftime("%Y-%m").tolist()
    
    # use list of months to pull needed parquet files from s3
    prefix = f'interval-data/quail-ridge/{str(accountNumber)}/'

    month_df_list = []
    for month in month_list:
        # month key
        month_key = prefix + month + ".parquet"
        
        # get parquet file and turn to df
        parquet_obj = s3_client.get_object(Bucket=bucket_name, Key=month_key)
        body = parquet_obj['Body'].read()
        
        month_df = pd.read_parquet(io.BytesIO(body))
        
        month_df_list.append(month_df)
    
    # concat parquet data into one dataframe
    concat_df = pd.concat(month_df_list)
    
    return concat_df


if __name__ == "__main__":
    client = boto3.client('s3')

    year = 2023
    month = 7
    
    # print(getHistoricalReportFilesS3(client, 2023, 7, 'High Desert Villas'))
    print(getS3Rates(client,'1/1/2023','3/1/2024','sce','toud4-9'))
