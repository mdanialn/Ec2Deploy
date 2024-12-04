import pandas as pd
import datetime as dt
import psycopg2
import json

def runSubscriberCalcs(db_credentials, community, start_date, end_date, nem_ix, vnem_btm):

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

    summary_df, units_df, subscriber_df = get_data_DB(conn, community, start_date, end_date, nem_ix, vnem_btm)

    commission_occupied_df = units_df.merge(subscriber_df, left_on = ['unit_number','community'], right_on = ['unit_number','community'], how = 'left')

    return summary_df, units_df, subscriber_df, commission_occupied_df

def subscriberDB_connector(js):

    #init subscriber database connection
    conn = psycopg2.connect(
            host=js['host'],
            dbname=js['dbname'],
            user=js['user'],
            password=js['password'],
            port = js['port'])

    return conn

def get_data_DB(conn, community,report_startdate_str, report_enddate_str, nem_ix, vnem_btm):
    
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
    output_list = []
#####################
    ##CARE
    query = f"""SELECT COUNT(*) AS pvOnly_CARE_count
                FROM subscribers s
                LEFT JOIN units u ON s.unit_number = u.unit_number
                WHERE s.subscriber_status = 'Active'
                AND s.utility_discount_program LIKE '%CARE%'
                AND u.vnem_btm = '{vnem_btm}'
                AND u.nem_ix = '{nem_ix}'
                AND s.community = '{community}'
                AND (CAST(s.move_out AS DATE) >= '{report_startdate_str}' OR s.move_out IS NULL)
                AND CAST(s.move_in AS DATE) <= '{report_enddate_str}'
                AND CAST(u.commission_date AS DATE) <= '{report_enddate_str}';"""
    cur.execute(query)
    data=cur.fetchall()
    CARE_count = data[0][0]

    ##nonCARE
    query = f"""SELECT COUNT(*) AS pvOnly_CARE_count
                FROM subscribers s
                LEFT JOIN units u ON s.unit_number = u.unit_number
                WHERE s.subscriber_status = 'Active'
                AND (s.utility_discount_program NOT LIKE '%CARE%' OR s.utility_discount_program IS NULL)
                AND u.vnem_btm = '{vnem_btm}'
                AND u.nem_ix = '{nem_ix}'
                AND s.community = '{community}'
                AND (CAST(s.move_out AS DATE) >= '{report_startdate_str}' OR s.move_out IS NULL)
                AND CAST(s.move_in AS DATE) <= '{report_enddate_str}'
                AND CAST(u.commission_date AS DATE) <= '{report_enddate_str}';"""
    cur.execute(query)
    data=cur.fetchall()
    nonCARE_count = data[0][0]

    #total units
    query = f"""SELECT COUNT(*) AS total_unit_count
                FROM units
                WHERE community = '{community}'
                AND nem_ix = '{nem_ix}'
                AND vnem_btm = '{vnem_btm}';
                """

    cur.execute(query)
    data=cur.fetchall()
    total_count = data[0][0]
    non_subscribed_count = total_count - nonCARE_count - CARE_count

#####################

    #get units table
    query = f"""SELECT * FROM units
            WHERE community = '{community}';"""
        #execute query
    cur.execute(query)
    data=cur.fetchall()
        #output into dataframe
    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    units_df = pd.DataFrame(data=data,columns=cols)

    #get subscriber table
    query = f"""SELECT * FROM subscribers
            WHERE community = '{community}'
            AND subscriber_status = 'Active';"""
        #execute query
    cur.execute(query)
    data=cur.fetchall()
        #output into dataframe
    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    subscriber_df = pd.DataFrame(data=data,columns=cols)


    DB_calcs_output = {'CARE_count':CARE_count,
                    'nonCARE_count':nonCARE_count,
                    'non_subscribed_count':non_subscribed_count}
    
    output_list.append(DB_calcs_output)

    output_df = pd.DataFrame(output_list)
    
    cur.close()

    return output_df, units_df, subscriber_df