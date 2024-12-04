import pandas as pd
import numpy as np
import datetime as dt
import calendar

from utils import s3Functions

def runSummaryAnnual(s3_client, year,month,iou,rate):

    # Year and Month inclusive
    # Returns rates for entire year up to year and month

    startdate_str = '1/1/' + str(int(year))

    day_range = calendar.monthrange(int(year),int(month))
    enddate_str = str(int(month)) + '/' + str(day_range[-1]) + '/' + str(int(year))

    rates_df = s3Functions.getS3Rates(s3_client= s3_client, start_date=startdate_str, end_date=enddate_str, iou=iou, rate=rate)
    summary_df = rateSummary(startdate_str, enddate_str, rates_df=rates_df)
    summary_df.set_index('Month',inplace = True)
    return summary_df

def runSummarySingle(s3_client, year,month,iou,rate):

    # Year and Month inclusive
    # Returns latest applicable rates for current year and month

    startdate_str = '1/1/' + str(int(year))
    day_range = calendar.monthrange(int(year),int(month))
    enddate_str = str(int(month)) + '/' + str(day_range[-1]) + '/' + str(int(year))
    rates_df = s3Functions.getS3Rates(s3_client= s3_client, start_date=startdate_str, end_date=enddate_str, iou=iou, rate=rate)
    summary_df = single_rate(rates_df=rates_df)

    return summary_df

def single_rate(rates_df):
    rates_df['rate_date'] = pd.to_datetime(rates_df['rate_date']).dt.date
    rates_df['On Peak'] = rates_df[['del_on_peak','gen_on_peak','wfc','dwra']].sum(axis = 1)
    rates_df['Mid Peak'] = rates_df[['del_mid_peak','gen_mid_peak','wfc','dwra']].sum(axis = 1)
    rates_df['Off Peak'] = rates_df[['del_off_peak','gen_off_peak','wfc','dwra']].sum(axis = 1)
    rates_df = rates_df[['rate_date','On Peak','Mid Peak','Off Peak','baseline_credit']].copy()

    return rates_df

def rateSummary(start_date, end_date, rates_df):

    rates_df['rate_date'] = pd.to_datetime(rates_df['rate_date'],format = '%m/%d/%Y')
    rates_df['On Peak'] = rates_df[['del_on_peak','gen_on_peak','wfc','dwra']].sum(axis = 1)
    rates_df['Mid Peak'] = rates_df[['del_mid_peak','gen_mid_peak','wfc','dwra']].sum(axis = 1)
    rates_df['Off Peak'] = rates_df[['del_off_peak','gen_off_peak','wfc','dwra']].sum(axis = 1)
    rates_df = rates_df[['rate_date','On Peak','Mid Peak','Off Peak','baseline_credit']]

    start_date_all = start_date

    date_range = pd.date_range(start_date_all,periods = 12, freq = '1M')
    template_df = pd.DataFrame(index=date_range).reset_index()
    template_df.rename(columns = {'index':'EOM'},inplace = True)
    template_df['EOM'] = pd.to_datetime(template_df['EOM'].dt.date)
    template_df['Month'] = template_df['EOM'].dt.month.apply(lambda x: calendar.month_abbr[x])

    # map over applicable rate to each month
    for date_end in template_df['EOM'].unique():
        template_df.loc[template_df['EOM']==date_end,'rate_date'] = rates_df.loc[rates_df['rate_date']<=date_end,'rate_date'].max()

    template_df = template_df.merge(rates_df,left_on='rate_date',right_on='rate_date', how = 'left')
    template_df = template_df.round(3)
    template_df.loc[template_df['EOM']>end_date,['On Peak','Mid Peak','Off Peak','baseline_credit']] = 0
    
    return template_df[['Month','On Peak','Mid Peak','Off Peak','baseline_credit']]


if __name__ == '__main__':
    
    import boto3

    client = boto3.client('s3')

    year = '2023'
    month = '6'
    # rates_table_df = runSummaryAnnual(client, year, month, 'sce', 'toud4-9')
    rates_table_df = runSummarySingle(client, year, month, 'sce', 'toud4-9')
    print(rates_table_df)
    # print(runSummarySingle)
