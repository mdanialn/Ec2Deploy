# -*- coding: utf-8 -*-
"""
Created on Wed Feb  8 10:50:14 2023

@author: kshin
"""

import pandas as pd

from pandas.tseries.holiday import AbstractHolidayCalendar, sunday_to_monday, Holiday

def touMapper(tou_path, df, sceNum, start_date, end_date):

    """ TOU Mapper for Wh data
    
    Arguments:
        df {pandas dataframe} -- processed pandas dataframe with Wh data {'timestamp_start', 'timestamp_end', 'Wh'}
            timestamp_start - timestamp
            timestamp_end - timestamp
            Wh - in Watt-hours

    Keyword Arguments:
        sceNum {str} -- SCE account number starting with a 7
        start_date {str} -- start_date of the interval data (inclusive)
        end_date {str} -- end_date of the interval data (inclusive)
    
    Returns:
        [pandas dataframe] -- Dispatch schedule
    """

    # initialize holiday calendar for SCE
    dr = pd.date_range(start='1/1/2020', end='1/1/2030')
    class HolidayCalendar(AbstractHolidayCalendar):
        rules = [
            Holiday('New Years Day', month=1, day=1, observance=sunday_to_monday),
            pd.tseries.holiday.USPresidentsDay,
            pd.tseries.holiday.USMemorialDay,
            Holiday('July 4th', month=7, day=4, observance=sunday_to_monday),
            pd.tseries.holiday.USLaborDay,
            Holiday('Veterans Day', month=11, day=11, observance=sunday_to_monday),
            pd.tseries.holiday.USThanksgivingDay,
            Holiday('Christmas', month=12, day=25, observance=sunday_to_monday)
        ]
    cal = HolidayCalendar()

    holidays = cal.holidays(start=dr.min().date(), end=dr.max().date(),return_name = True)
    holidays_df = holidays.to_frame().reset_index()
    holidays_df.rename(columns = {"index":"date",0:"holiday name"},inplace = True)
    holidays_df['date'] = pd.to_datetime(holidays_df['date'], format = '%Y-%m-%d')

    # Import data
    temp_df = df.copy()

    # Reading in TOU Mapping Sheet
    seasons_df = pd.read_excel(tou_path, sheet_name = 'Season')
    hours_df = pd.read_excel(tou_path, sheet_name = 'TOUHours')

    # create column with settlement time interval
    temp_df['timestamp_start'] = pd.to_datetime(temp_df['timestamp_start']).dt.floor('H')
    temp_df['timestamp_end'] = pd.to_datetime(temp_df['timestamp_end']).dt.ceil('H')

    # convert Wh data to settlement time interval
    df = temp_df[['timestamp_start','timestamp_end','Wh']].copy()
    df = df.groupby(by = ['timestamp_start','timestamp_end']).sum().reset_index()
    
    # add date column
    df['date'] = df['timestamp_start'].dt.date
    df['date'] = pd.to_datetime(df['date'],format='%Y-%m-%d')

    # Filter for data within start_date and end_date
    df = df.loc[(df['date'] >= start_date) & (df['date'] <= end_date)]

    # Add Month and HE columns
    df['Month'] = df['timestamp_start'].dt.month
    df['Hour Ending'] = df['timestamp_end'].dt.hour

    # Add Season flag
    df = df.merge(seasons_df,left_on = ['Month'],right_on = ['Month'],how = 'outer')

    # Add holiday Flag
    df = df.merge(holidays_df, left_on = ['date'],right_on = ['date'], how = 'outer')

    # Add Weekend column flag
    df['weekend'] = df['date'].dt.weekday

    # Add weekend holiday combined flag
    df['weekend/holiday flag'] = 0
    df.loc[(df['weekend']>=5) | (~df['holiday name'].isnull()),'weekend/holiday flag'] = 1

    # Add TOU Flag based on tariff mapping
    df = df.merge(hours_df,left_on = ['Hour Ending','Season','weekend/holiday flag'],right_on = ['HE','Season','Weekend/Holiday Flag'],how = 'outer')
    
    # Drop any empty rows
    df.dropna(subset = ['Wh'],inplace = True)

    # Get rows on interest
    df = df[['timestamp_start','timestamp_end','Season','TOU','Wh']]
    
    # sort
    df.sort_values(by = 'timestamp_start',inplace = True)

    # Final output dataframe with TOU Wh summary
    final_df =  df[['Season','TOU','Wh']].copy()
    final_df = final_df.groupby(by = ['Season','TOU']).sum().reset_index()
    final_df.rename(columns = {'Wh':sceNum},inplace = True)

    final_df = final_df.set_index(["Season","TOU"]).transpose()
    final_df.columns = final_df.columns.map(" ".join)

    try:
        final_df['start'] = df['timestamp_start'].iloc[0]
        final_df['end'] = df['timestamp_end'].iloc[-1]

    except:
        return None
    
    return final_df


if __name__ == "__main__":
    pass