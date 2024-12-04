import pandas as pd
import calendar
import datetime as dt
import psycopg2

from dateutil.relativedelta import relativedelta


def runSubscriberCalcs(db_credentials, community, start_date, end_date):

    """
    Run subscription database query and database summary calcs

    Arguments:
        conn - psycopg connection to database
        community - name of community
        start_date - analysis start date
        end_date - analysis end date (inclusive)

    Returns:
        dataframe - dataframe with subscription database summary
        
    """
    conn = subscriberDB_connector(db_credentials)

    subscriber_table, units_table, communities_table = get_data_DB(conn, community, start_date, end_date)
    units_table_all = units_table.copy()

    units_table = units_table.loc[~units_table['commission_date'].isnull()].copy()

    commission_occupied_df = units_table.merge(subscriber_table, left_on = ['unit_number','community'], right_on = ['unit_number','community'], how = 'left')

    month_range_list = pd.date_range(start=start_date, end=end_date, freq=pd.offsets.MonthEnd(1))
    month_range_list = list(month_range_list) + [pd.Timestamp(end_date)]
    month_range_list = list(set(month_range_list))

    db_calcs_list = []
    for eomoth in month_range_list:
        start_date_eval = eomoth.replace(day = 1).strftime('%Y-%m-%d')
        end_date_eval = eomoth.strftime('%Y-%m-%d')
        
        dbCalcsOutput = DB_calcs(subscriber_table, units_table, start_date_eval, end_date_eval)
        dbCalcsOutput['startDate'] = start_date_eval
        dbCalcsOutput['endDate'] = end_date_eval
        db_calcs_list.append(dbCalcsOutput)
    
    monthlyDbOutput_df = pd.concat(db_calcs_list)


    return monthlyDbOutput_df, commission_occupied_df, communities_table, units_table_all

def subscriberDB_connector(js):

    #init subscriber database connection
    conn = psycopg2.connect(
            host=js['host'],
            dbname=js['dbname'],
            user=js['user'],
            password=js['password'],
            port = js['port'])

    return conn

def get_data_DB(conn, community,report_startdate_str, report_enddate_str):
    
    """
    Query database for subscriber information and units information

    Arguments:
        conn - psycopg connection to database
        community - name of community
        report_startdate_str - inclusive
        report_enddate_str - inclusive


    Returns:
        dataframe - database with subscribers and units
    
    """

    cur = conn.cursor()

    query = f"""SELECT * FROM subscribers subs
                WHERE community = '{community}' 
                AND (CAST(subs.move_out AS DATE) >= '{report_startdate_str}' OR subs.move_out IS NULL)
                AND CAST(subs.move_in AS DATE) <= '{report_enddate_str}'
                """

    cur.execute(query)
    data=cur.fetchall()
    #output into dataframe
    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    
    subscriber_table = pd.DataFrame(data=data,columns=cols)

    #units table
    query = f"""SELECT * FROM units
                WHERE community = '{community}'"""
    cur.execute(query)
    data=cur.fetchall()
    #output into dataframe
    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    
    units_table = pd.DataFrame(data=data,columns=cols)

    #communities table
    query = f"""SELECT * FROM communities
                WHERE community = '{community}'"""
    cur.execute(query)
    data=cur.fetchall()
    #output into dataframe
    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    
    communities_table = pd.DataFrame(data=data,columns=cols)
    
    cur.close()

    return subscriber_table,units_table, communities_table

def DB_calcs(subscriber_table,units_table,start_date,end_date):
    
    """
    Run subscriber database calculation

    Arguments:
        conn - psycopg connection to database
        community - name of community

    Returns
        dataframe - dataframe with subscription database summary
        
    """

    output_list = []

    # Convert start_date string to datetime object
    date_obj = dt.datetime.strptime(start_date, "%Y-%m-%d")
    month_num = date_obj.month
        
    # convert month num to short month text
    short_month = calendar.month_abbr[int(month_num)]

    #Total active units that are fully subscribed and reside int he community
    active_subs_df = subscriber_table.loc[(subscriber_table['subscriber_status']=='Active')].copy()
    active_subs_df['move_in']=pd.to_datetime(active_subs_df['move_in'],format = '%m/%d/%y')
    active_subs_df = active_subs_df.loc[(active_subs_df['move_in']<=end_date)]
    
    #Actual # of days that commision units were occupied
    #Total # of days that the commissioned units was operating for
    #Actual # of days unit was occupied Total # of days that unit was operating for
    #preprocess
    merged = subscriber_table.merge(units_table,left_on='unit_number',right_on='unit_number', how = 'left')
    occup = merged.dropna(subset=['commission_date']).copy()
    occup['move_in'] = pd.to_datetime(occup['move_in'],format = '%m/%d/%y')
    occup['move_out'] = pd.to_datetime(occup['move_out'],format = '%m/%d/%y')
    occup['commission_date'] = pd.to_datetime(occup['commission_date'],format = '%m/%d/%y')

    #add month cols
    occup['start_month']=pd.Timestamp(start_date)
    occup['end_month']=pd.Timestamp(end_date)

    #find max min dates
    occup['start_date']=occup[['move_in','commission_date','start_month']].max(axis=1)
    occup['end_date']=occup[['move_out','end_month']].min(axis=1)

    #calc occupancy days
    occup['occup_delta'] = (occup['end_date'] - occup['start_date']).dt.days + 1
    occup2 = occup.loc[(occup['occup_delta']>=0)]
    occup_days=occup2['occup_delta'].sum()

    #calc total days
    units_table['start_month'] = pd.Timestamp(start_date)
    units_table['end_month'] = pd.Timestamp(end_date)
    units_table['commission_date'] = pd.to_datetime(units_table['commission_date'], format = '%m/%d/%y')
    units_table['start_date']=units_table[['commission_date','start_month']].max(axis=1)
    units_table['total_days'] =  (units_table['end_month'] - units_table['start_date']).dt.days + 1

    commission_df = units_table.loc[(units_table['total_days']>=0)].copy()
    commission_df=commission_df.drop_duplicates(subset=['unit_number'])
    total_days=commission_df['total_days'].sum()
    
    #Start # of units at beginning of the month
    #Ending # of the units at end of month
    start_df = units_table.dropna(subset = ['commission_date']).copy()
    start_df['commission_date'] = pd.to_datetime(start_df['commission_date'],format = '%m/%d/%y')
    begin_df=start_df.loc[(start_df['commission_date']<= start_date)]
    comm_start=len(begin_df['commission_date'])
    end_df=start_df.loc[(start_df['commission_date']<= end_date)]
    comm_end=len(end_df['commission_date'])
    
    #number of days someone is billed on CARE
    #number of days someone is not billed on CARE
    CARE_df=occup2.loc[(occup2['utility_discount_program']=='CARE')]
    nonCARE_df=occup2.loc[(occup2['utility_discount_program']!='CARE')]
    careDays = CARE_df['occup_delta'].sum()
    nonCAREDays = nonCARE_df['occup_delta'].sum()
        
    DB_calcs_output = {'Starting Units Commissioned':comm_start,
                    'Ending Units Commissioned':comm_end,
                    'Commissioned & Occupied Days':occup_days,
                    'Commissioned Days':total_days,
                    'Days with Units on CARE':careDays,
                    'Days with Units Not on CARE':nonCAREDays,
                    'Month':short_month}
    
    output_list.append(DB_calcs_output)

    output_df = pd.DataFrame(output_list)

    return output_df

if __name__ == "__main__":
    #connect to DB
    import json

    with open("creds.json") as f:
        data=f.read()
    credentials = json.loads(data)

    #get data
    print(runSubscriberCalcs(credentials, 'Quail Ridge','2024-05-23', '2024-05-29')[0])
