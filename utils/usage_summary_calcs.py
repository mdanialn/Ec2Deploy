import datetime as dt
from utils import touMapper
import pandas as pd
import calendar

def usage_summary_calc(fn, df_consum,df_gen,output_col_name,year, month):

    """
        Arguments:
            df_consum - [timestamp_start, timestamp_end, Wh]
                - timestamp_start and timestamp_end should already be in hourly settlement intervals
            df_gen - [timestamp_start, timestamp_end, Wh]
            col_name - "string of column name"
            startdate (inclusive) - 1/1/23 string
            endate (exclusive) - 1/31/23 string
        
        Returns:
            dataframe - dataframe with generation and consumption summary

    """

    startdate_str = str(int(month))  + '/1/' + str(int(year))

    day_range = calendar.monthrange(int(year),int(month))
    enddate_str = str(int(month)) + '/' + str(day_range[-1]) + '/' + str(int(year))
    
    df_consum['timestamp_start'] = pd.to_datetime(df_consum['timestamp_start'],infer_datetime_format=True)
    df_gen['timestamp_start'] = pd.to_datetime(df_gen['timestamp_start'],infer_datetime_format=True)

    df_consum_filter_time = df_consum.loc[(df_consum['timestamp_start']>= startdate_str) & (df_consum['timestamp_start']<= enddate_str + " 23:59:59")]

    df_gen_filter_time = df_gen.loc[(df_gen['timestamp_start']>= startdate_str) & (df_gen['timestamp_start']<= enddate_str + " 23:59:59")]

    if df_consum_filter_time.empty or df_gen_filter_time.empty:
        raise ValueError("Data does not contain startdate and endate time ranges")

    date_format= '%m/%d/%Y'

    date_obj= dt.datetime.strptime(startdate_str,date_format)

    TOU_consumption_df = touMapper.touMapper(fn, df_consum,output_col_name, startdate_str, enddate_str)
    TOU_gen_df = touMapper.touMapper(fn, df_gen,output_col_name,startdate_str, enddate_str)

    touCon_df = TOU_consumption_df.drop(columns = ['start','end']) / 1000 / 1000 # Wh to kWh to MWh
    touGen_df = TOU_gen_df.drop(columns = ['start','end']) / 1000 / 1000 # Wh to kWh to MWh

    touCon_df.rename(columns = {
        "S Peak": "Peak Consumption (MWh)",
        "S Mid Peak": "Mid Peak Consumption (MWh)",
        "S Off Peak": "Off Peak Consumption (MWh)",
        "W Mid Peak": "Peak Consumption (MWh)",
        "W Off Peak": "Mid Peak Consumption (MWh)",
        "W Super Off": "Off Peak Consumption (MWh)"
    },inplace = True)

    touGen_df.rename(columns = {
        "S Peak": "Peak Production (MWh)",
        "S Mid Peak": "Mid Peak Production (MWh)",
        "S Off Peak": "Off Peak Production (MWh)",
        "W Mid Peak": "Peak Production (MWh)",
        "W Off Peak": "Mid Peak Production (MWh)",
        "W Super Off": "Off Peak Production (MWh)"
    },inplace = True)
    
    con_output_dict = touCon_df.to_dict('records')[0]
    gen_output_dict = touGen_df.to_dict('records')[0]

    output_dict = con_output_dict.copy()
    output_dict.update(gen_output_dict)

    expected_keys = ['Peak Consumption (MWh)','Mid Peak Consumption (MWh)','Off Peak Consumption (MWh)','Peak Production (MWh)','Mid Peak Production (MWh)','Off Peak Production (MWh)']
    for key in expected_keys:
        if key not in output_dict:
            output_dict[key] = 0
   
    return output_dict

if __name__ == "__main__":
    
    df_consum = pd.read_csv("sampleConsumptionData.csv")
    df_gen = pd.read_csv("sampleGenerationData.csv")
    
    output_col_name = "sample data"
    year = 2023
    month = 8
    fn = "tariffMap - TOUD4-9.xlsx"
    usage_calc = usage_summary_calc(fn, df_consum=df_consum, df_gen=df_gen, output_col_name=output_col_name,year=year, month=month)
