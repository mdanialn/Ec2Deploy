import pdfkit
import pandas as pd
import calendar
import numpy as np
import datetime as dt

import historicalRateSummary
import utils.subscriber_db_calcs as subscriber_db_calcs

import getAWSSecret

import s3Functions

def create_rev_metric_report(community, start_date, end_date, s3_client):
        
    date_obj = dt.datetime.strptime(start_date, "%Y-%m-%d")
    year = date_obj.year
    month = date_obj.month

    # Template df with months
    month_list = list(map(lambda x: calendar.month_abbr[x], range(1,13)))
    template_df = pd.DataFrame(index=month_list).reset_index()
    template_df.rename(columns = {'index':'Month'},inplace = True)    

    # Init connector to subscriber database
    credentials = getAWSSecret.get_secret("database3_rds_cred")

    # Run subscriber database calcs
    subscriber_df = subscriber_db_calcs.runSubscriberCalcs(credentials, community,start_date,end_date)

    # Merge subscriber database calc table with template table
    subscriber_df = template_df.merge(subscriber_df, left_on='Month',right_on='Month',how='outer')

    # Run CARE and Occupancy rates
    subscriber_df['% on CARE'] = subscriber_df['Days with Units on CARE'] / (subscriber_df['Days with Units on CARE'] + subscriber_df['Days with Units Not on CARE'])
    subscriber_df['Occupancy'] = subscriber_df['Commissioned & Occupied Days'] / subscriber_df['Commissioned Days']

    # Format dataframe for output
    subscriber_df.set_index('Month',inplace = True)
    subscriber_df.replace({np.nan:0},inplace = True)

    subscriber_df['% on CARE'] = subscriber_df['% on CARE'].apply(lambda x: "{:.0%}".format(x))
    subscriber_df['Occupancy'] = subscriber_df['Occupancy'].apply(lambda x: "{:.0%}".format(x))

    value_columns = ['Starting Units Commissioned',
                   'Ending Units Commissioned',
                   'Commissioned & Occupied Days',
                   'Commissioned Days',
                   'Days with Units on CARE',
                   'Days with Units Not on CARE',]
                   
    subscriber_df[value_columns] = subscriber_df[value_columns].astype(int).astype(str)

    subscriber_df = subscriber_df.transpose()
    subscriber_df.replace({'0':'-'},inplace = True)
    subscriber_df.replace({'0%':'-'},inplace = True)

    # Rate Table
    rate_table_df = historicalRateSummary.runSummary(s3_client, year, month,'sce','toud4-9').transpose()

    # Get current month con gen calc
    out_df = pd.DataFrame({'Peak Consumption (MWh)':0,
                           'Mid Peak Consumption (MWh)':0,
                           'Off Peak Consumption (MWh)':0,
                           'Peak Production (MWh)':0,
                           'Mid Peak Production (MWh)':0,
                           'Off Peak Production (MWh)':0,
                           'Month':'Aug'
                           },index=[0])

    month_abrv = out_df['Month'].iloc[0]

    # Read in historical data for consumption + generation overview from s3
    historical_con_data = s3Functions.getHistoricalReportFilesS3(s3_client, year, month, community)
    historical_con_data = historical_con_data.iloc[:month]

    # merge template with historical data
    con_gen_df = template_df.merge(historical_con_data,left_on = ['Month'],right_on=['Month'],how = 'outer')
    
    # Set rows in the template df to values in the latest dataframe
    con_gen_df.loc[con_gen_df['Month'] == month_abrv, 'Peak Consumption (MWh)'] = out_df['Peak Consumption (MWh)'].iloc[0]
    con_gen_df.loc[con_gen_df['Month'] == month_abrv, 'Mid Peak Consumption (MWh)'] = out_df['Mid Peak Consumption (MWh)'].iloc[0]
    con_gen_df.loc[con_gen_df['Month'] == month_abrv, 'Off Peak Consumption (MWh)'] = out_df['Off Peak Consumption (MWh)'].iloc[0]
    con_gen_df.loc[con_gen_df['Month'] == month_abrv, 'Peak Production (MWh)'] = out_df['Peak Production (MWh)'].iloc[0]
    con_gen_df.loc[con_gen_df['Month'] == month_abrv, 'Mid Peak Production (MWh)'] = out_df['Mid Peak Production (MWh)'].iloc[0]
    con_gen_df.loc[con_gen_df['Month'] == month_abrv, 'Off Peak Production (MWh)'] = out_df['Off Peak Production (MWh)'].iloc[0]

    # Write in latest values to s3
    con_gen_df.to_csv("Con Summary v2 updated.csv")

    con_gen_df['Total Consumption (MWh)'] = con_gen_df[['Peak Consumption (MWh)','Mid Peak Consumption (MWh)','Off Peak Consumption (MWh)']].sum(axis = 1)
    con_gen_df['Total Production (MWh)'] = con_gen_df[['Peak Production (MWh)','Mid Peak Production (MWh)','Off Peak Production (MWh)']].sum(axis = 1)

    con_gen_df.set_index('Month',inplace = True)

    con_gen_df = con_gen_df.transpose()
    con_gen_df  = con_gen_df.round(2)
    con_gen_df.replace({np.nan:'-',0:'-'},inplace = True)

    # generating the pdf
    style_html = """
    <style> table, th {
        border-collapse:collapse;
        font-size:12px
    }
    td {font-size:12px}
    </style>
    """
    
    f = open('assets/image_url.txt', 'r')
    image_url = f.read()
    
    image_html = '<img src="{image_url}">'.format(image_url = image_url)
    
    header_html = image_html + f"<h2>SCE {community}: {year}</h2>"
    
    subscriber_table = "<center style='font-family:Arial'>" + subscriber_df.to_html(col_space = "60px").replace('<td>', '<td align="center">').replace('<th>','<thead align ="center">').replace('<th','<th align="center"') + "</center>"
    subscriber_table.replace('<table border="1" class="dataframe">',"<table border-spacing='0'>")
    
    rate_table = "<center style='font-family:Arial'>" + rate_table_df.to_html(col_space = "60px").replace('<td>', '<td align="center">').replace('<th>','<thead align ="center">').replace('<th','<th align="center"') + "</center>"
    rate_table.replace('<table border="1" class="dataframe">',"<table border-spacing='0'>")
    
    con_gen_table = "<center style='font-family:Arial'>" + con_gen_df.to_html(col_space = "60px").replace('<td>', '<td align="center">').replace('<th>','<thead align ="center">').replace('<th','<th align="center"') + "</center>"
    con_gen_table.replace('<table border="1" class="dataframe">',"<table border-spacing='0'>")
    
    html = style_html + header_html + "<hr>" + "<h2> Subscriber Overview</h2>" + subscriber_table.replace("\n","") + "<br>" + "<h2> Rate Overview</h2>" + rate_table.replace("\n","") + "<br>" + "<h2> Consumption + Generation Overview</h2>" + con_gen_table

    
    # create pdf and store as temp
    tmp_folder = '/tmp/'
    filepath = tmp_folder + f'{community} {year}_{month}.pdf'
    pdfkit.from_string(html,filepath)

    return filepath
    
if __name__ == "__main__":
    
    import json
    import boto3

    with open("creds.json") as f:
        data=f.read()
    js = json.loads(data)

    client = boto3.client('s3')
    community = 'High Desert Villas'
    start_date = '2023-06-01'
    end_date = '2023-06-30'
    
    create_rev_metric_report(community, start_date, end_date, client)

