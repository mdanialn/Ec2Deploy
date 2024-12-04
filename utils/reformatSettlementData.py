import pandas as pd

def settlementFormat(df, settlement_netting_interval = 15, time_gap = 30):

    """ TOU Mapper for usage data
    
    Arguments:
        df {pandas dataframe} -- processed pandas dataframe with usage data {'timestamp_end','Wh}
            timestamp_end - datetime HE
            timestamp_start - datetime HE
            Wh - in watt-hours

    Keyword Arguments:

    Returns:
        [pandas dataframe] -- df with data formatted to settlement intervals
    """  

    # Read CSV File
    df_temp = df.copy()

    # Production data settlement dataframe
    df_temp['timestamp_end'] = pd.to_datetime(df_temp['timestamp_end'],format = "%m/%d/%y %H:%M:%S")
    df_temp['timestamp_start'] = pd.to_datetime(df_temp['timestamp_start'],format = "%m/%d/%y %H:%M:%S")

    df_temp['time delta'] = (df_temp['timestamp_end'] - df_temp['timestamp_start']).diff().dt.total_seconds()
    # df_temp = df_temp.loc[df['time delta'] <= time_gap*60]
    
    # determine which metered readings are part of two different hour endings
    # add timestamp for hour beginning using previous row reading
    df_temp['timestamp_start_hour'] = df_temp['timestamp_start'].dt.floor(str(settlement_netting_interval)+'min')
    
    # Time beginning that actual metered reading occured at
    df_temp['timestamp_end_hour'] = df_temp['timestamp_end'].dt.floor(str(settlement_netting_interval)+'min')

    # Determine which intervals has leak based on start hour of the reading and end hour of the reading
    df_temp['start end diff'] = (df_temp['timestamp_end_hour'] - df_temp['timestamp_start_hour']).dt.total_seconds()

    # Remove any intervals where there is more than 2x intervals missing between the data reporting periods
    df_temp.loc[df_temp['start end diff'] >= settlement_netting_interval*60*2, 'Wh'] = 0

    # determine how many seconds over the reading is over the end hour and under the end hour
    df_temp['end overage'] = (df_temp.loc[df_temp['start end diff'] != 0,'timestamp_end'] - df_temp.loc[df_temp['start end diff'] != 0,'timestamp_end_hour']).dt.total_seconds()
    df_temp['start underage'] = (df_temp.loc[df_temp['start end diff'] != 0,'timestamp_end_hour'] - df_temp.loc[df_temp['start end diff'] != 0,'timestamp_start']).dt.total_seconds()
    df_temp['timestamp delta'] = df_temp['end overage'] + df_temp['start underage']

    # convert overage into percentages
    df_temp['start portion'] = df_temp['start underage'] / df_temp['timestamp delta']
    df_temp['end portion'] = df_temp['end overage'] / df_temp['timestamp delta']

    # convert overage and underage into kWh
    df_temp['start production'] = df_temp['start portion'] * df_temp['Wh']
    df_temp['end production'] = df_temp['end portion'] * df_temp['Wh']
    # Time start hour - time beginning
    df_main = df_temp.loc[df_temp['start end diff'] == 0,['timestamp_start_hour','Wh']].groupby('timestamp_start_hour').sum().reset_index()
    
    df_overage_start = df_temp.loc[df_temp['start end diff'] != 0,['timestamp_start_hour','start production']].groupby('timestamp_start_hour').sum().reset_index()
    
    df_overage_end = df_temp.loc[df_temp['start end diff'] != 0,['timestamp_end_hour','end production']].groupby('timestamp_end_hour').sum().reset_index()

    settlement_df = df_main.merge(df_overage_start,left_on = ['timestamp_start_hour'],right_on = ['timestamp_start_hour'],how = 'outer')

    settlement_df = settlement_df.merge(df_overage_end,left_on = ['timestamp_start_hour'],right_on = ['timestamp_end_hour'],how = 'outer')
    settlement_df['total_production'] = settlement_df[['Wh','start production','end production']].sum(axis = 1)
    settlement_df['timestamp_end_hour'] = settlement_df['timestamp_start_hour'] + pd.Timedelta(hours = settlement_netting_interval/60)

    return settlement_df

def settlementFormatShoulders(df, startTime, endTime):

    """ TOU Mapper for usage data
    
    Arguments:
        df {pandas dataframe} -- processed pandas dataframe with usage data {'timestamp_end','Wh}
            timestamp_end - datetime HE
            timestamp_start - datetime HE
            Wh - in watt-hours

    Keyword Arguments:

    Returns:
        [pandas dataframe] -- df with data formatted to settlement intervals
    """  

    # Read CSV File
    df_start_temp = df.iloc[0].copy()
    df_end_temp = df.iloc[-1].copy()
    out_df = df.iloc[1:-2].copy()
    
    # Calculate the portion of the production that does not occur within the event start time
    df_start_temp['event_startTime'] = startTime
    df_start_temp['event_startTime'] = pd.to_datetime(df_start_temp['event_startTime'])

    df_start_temp['end_overage'] = (df_start_temp['timestamp_end'] - df_start_temp['event_startTime']).total_seconds()
    df_start_temp['start_underage'] = (df_start_temp['event_startTime'] - df_start_temp['timestamp_start']).total_seconds()
    df_start_temp['time delta'] = df_start_temp['end_overage'] + df_start_temp['start_underage']

    df_start_temp['start_portion'] = df_start_temp['start_underage'] / df_start_temp['time delta']
    df_start_temp['end_portion'] = df_start_temp['end_overage'] / df_start_temp['time delta']
    df_start_temp['total_production'] = df_start_temp['end_portion'] * df_start_temp['Wh']

    # Calculate the portion of the production that does not occur within the event end time
    df_end_temp['event_endTime'] = endTime
    df_end_temp['event_endTime'] = pd.to_datetime(df_end_temp['event_endTime'])

    df_end_temp['end_overage'] = (df_end_temp['timestamp_end'] - df_end_temp['event_endTime']).total_seconds()
    df_end_temp['start_underage'] = (df_end_temp['event_endTime'] - df_end_temp['timestamp_start']).total_seconds()
    df_end_temp['time delta'] = df_end_temp['end_overage'] + df_end_temp['start_underage']

    df_end_temp['start_portion'] = df_end_temp['start_underage'] / df_end_temp['time delta']
    df_end_temp['end_portion'] = df_end_temp['end_overage'] / df_end_temp['time delta']
    df_end_temp['total_production'] = df_end_temp['start_portion'] * df_end_temp['Wh']

    df_start_temp = df_start_temp[['event_startTime','timestamp_end','total_production']].to_frame().transpose()
    df_end_temp = df_end_temp[['timestamp_start','event_endTime','total_production']].to_frame().transpose()

    df_start_temp.rename(columns = {'event_startTime':'timestamp_start','total_production':'Wh'},inplace = True)
    df_end_temp.rename(columns = {'event_endTime':'timestamp_end','total_production':'Wh'},inplace = True)

    out_df = pd.concat([out_df, df_start_temp, df_end_temp])
    out_df.sort_values(by = ['timestamp_start'],inplace = True)

    return out_df

if __name__ == "__main__":

    ## SampleProductionData.csv df - [timestamp_end, timestamp_start,inverter_production_meter [wh]]
    df = pd.read_csv("l-201 inverter data.csv", index_col=0)
    
    resettle_data = settlementFormat(df,settlement_netting_interval=15)
    print(resettle_data)