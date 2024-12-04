import pandas as pd
import os
from datetime import datetime, timedelta
import calendar

from subscriber_DB_connector import *
from subscriber_calcs import *
from reformatSettlementData import *
import touMapper

def run_calcs_enphase_local(commissioned_df,month_year):
    
    #create date variables
    date_format= '%Y-%m'
    date_obj= datetime.strptime(month_year,date_format).date()
    date_str = date_obj.strftime("%Y-%m-%d")
    month=date_obj.month
    year=date_obj.year

    # Convert month num to month name
    monthname=calendar.month_name[month]
    monthname=monthname.lower()

    # end date of the analysis
    enddate_obj = date_obj + timedelta(days=calendar.monthrange(year,month)[1]-1)
    enddate = enddate_obj.strftime("%Y-%m-%d")

    # init empty list for storing summary of each unit
    summary_list = []

    #calc for each file
    for index, row in commissioned_df['unit_number'].iterrows():
        
        #set file name
        unit_file = path+'/'+row['unit_number'] + '_net_energy_' + monthname + '_' + str(year) + '.csv'
        unit_commdate=row['unit_number']
        
        # Get file
        df_consum, df_gen = get_local_enphase_data(unit_file, commission_date_str = unit_commdate)

        # run usage calcs
        consum_gen_summary = usage_calcs(df_consum, df_gen,unit_file ,date_str, enddate)
    
    summary_list.append(consum_gen_summary)

    # calculate sum of all units for each rate period and convert to dictionary output format
    consum_gen_summary_df = pd.DataFrame(summary_list)
    consum_gen_summary_dict = consum_gen_summary_df.sum().to_dict()

    return consum_gen_summary_dict

#setting inputs (LOCAL ONLY - not set up for S3 yet) : fn is the TOUmap file (stays constant) - path is the path to all the months file folder - month_year = yyyy-mm
#this iteration only set up for Jan23-Jun23 
def get_local_enphase_data(file,commission_date_str):
    #set directory
    os.chdir(path)

    #if file DNE then pass
    try:
        #read file in and filter out non-commissioned days
        df = pd.read_csv(file)
        df['Date/Time']=df['Date/Time'].str[:-6]
        df['Date/Time'] = pd.to_datetime(df['Date/Time'],infer_datetime_format = True)
        df = df.loc[(df['Date/Time']>=commission_date_str)]
    
        # Separate out to consumption and generation dataframes, Date/Time should be hour ending
        df_consum=df[["Date/Time","Energy Consumed (Wh)"]].copy()
        df_gen=df[["Date/Time","Energy Produced (Wh)"]].copy()

        # Rename columns to match format of settlementformat function
        df_consum.rename(columns = {"Date/Time":"timestamp","Energy Consumed (Wh)":"Wh"},inplace = True)
        df_gen.rename(columns = {"Date/Time":"timestamp","Energy Produced (Wh)":"Wh"},inplace = True)

        #set up file for toumapper
        settlement_netting_interval = 60
        df_consum_formatted = settlementFormat(df_consum, settlement_netting_interval)
        df_gen_formatted = settlementFormat(df_gen, settlement_netting_interval)

    except:
        pass

    return df_consum_formatted, df_gen_formatted
    

if __name__ == "__main__"
    #log in to DB and get units table
    conn = subscriberDB_connector()
    subscriber_table,units_table = get_data_DB('High Desert Villas')
    #drop non-commissioned units
    commissioned_df = units_table.dropna(subset=['commission_date'])

    #INPUTS
    #set for mapping
    fn = 'tariffMap - TOUD4-9.xlsx'
    #set directory
    path = ''
    #set month YYYY-MM

    startdate = '1/1/23'
    enddate = '1/31/23'
    #OUTPUT
    TOU_output=usage_summary_calc(fn,path,startdate, enddate)
    print(TOU_output)
