import pandas as pd
import io
import boto3
import datetime as dt
import numpy as np
from utils import reformatSettlementData
from utils import touMapper
from utils.connector import pearlxFlexAPIConnect

import glob


def productionAnalysis(auth, community, subscriber_df, commission_occupied_df, start_date,end_date, nem_ix):
    
    # Get all commissioned units from subscribers database
    commissioned_units_list = commission_occupied_df['unit_number'].unique()
    start_date_timestamp = pd.to_datetime(start_date)
    end_date_timestamp = pd.to_datetime(end_date) + pd.Timedelta(days = 1)
    analysis_end_date_str = end_date_timestamp.strftime('%Y-%m-%d')

    commission_occupied_df = commission_occupied_df.copy()
    commission_occupied_df['move_in'] = pd.to_datetime(commission_occupied_df['move_in'])
    commission_occupied_df['move_out'] = pd.to_datetime(commission_occupied_df['move_out'])

    # Get all active sites from PearlX Flex
    sites_df = pearlxFlexAPIConnect.pearlXFlexGetSites(auth)

    # Filter PearlX Flex site response by community
    if community == 'High Desert Villas':
        sites_df = sites_df.loc[sites_df['site_address'] == 'Jasmine Street 16850, Victorville, United States 92395']
    elif community == 'Quail Ridge':
        sites_df = sites_df.loc[sites_df['site_address'] == 'East Thornton Avenue 409, Hemet, United States 92543']
    
    # Parse site_name to get unit_number
    sites_df['unit_number'] = sites_df['site_name'].apply(lambda x: x.split(' ')[-1]).str.title()

    # From sites_df, get only commissioned units
    commissioned_sites_df = sites_df.loc[sites_df['unit_number'].isin(commissioned_units_list)].copy()

    bessPV_list = []
    pvOnly_list = []
    event_performance_list = []

    # Get data for sites in sites_df for only commissioned_units
    missing_settlement_intervals = 0
    missing_units = 0
    for commission_df_index, commission_df_row in commissioned_sites_df.iterrows():
        try:
            site_id = commission_df_row['id']
            # Get unit_number from site_df
            unit_number = commission_df_row['unit_number']
            # Get tenants from subscribers database
            temp_occupancy = commission_occupied_df.loc[commission_occupied_df['unit_number'] == unit_number]
            system_config = temp_occupancy['vnem_btm'].iloc[0]
            commission_date = temp_occupancy['commission_date'].iloc[0]

            # Get start date for analysis, max of commission date and analysis start date
            analysis_start_date_obj = max(pd.to_datetime(commission_date), start_date_timestamp)
            analysis_start_date_str = analysis_start_date_obj.strftime('%Y-%m-%d')

            expected_settlements_intervals = pd.date_range(analysis_start_date_str, end_date + ' 23:45:00', freq = '15min', inclusive='both')
        
            expected_intervals = ((end_date_timestamp - analysis_start_date_obj).total_seconds() / 60 / 60) * 4
            
            # Get DER ids from PearlX Flex
            der_df = pearlxFlexAPIConnect.pearlXFlexDERId(auth, site_id)

            # If there is a battery id get battery data
            if 'battery' in der_df['der_type'].unique():
                der_id = der_df.loc[der_df['der_type'] == 'battery','id'].iloc[0]
                storage_id = der_df.loc[der_df['der_type'] == 'storage','id'].iloc[0]

                bess_df = pearlxFlexAPIConnect.pearlXFlexGetProductionMeterData(auth, site_id, der_id, 'storage-data', start_date, end_date)
                battery_events = pearlxFlexAPIConnect.pearlXFlexGetEvents(auth, site_id, storage_id, start_date, end_date)
                bess_df['timestamp'] = pd.to_datetime(bess_df['timestamp'])
                bess_df.replace({'battery_lifetime_discharged':{'None':np.nan}}, inplace = True)
                bess_df['battery_lifetime_discharged'] = bess_df['battery_lifetime_discharged'].astype(float)
                bess_df.sort_values(by = ['timestamp'],inplace = True)

                bess_df['Wh'] = bess_df['battery_lifetime_discharged'].diff()
                bess_df['Wh'] = bess_df['Wh'].clip(lower=0)
                bess_df['timestamp_start'] = bess_df['timestamp'].shift(1)

                bess_df['timestamp_diff'] = (bess_df['timestamp'] - bess_df['timestamp_start']).dt.total_seconds() / 3600
                bess_df['power'] = bess_df['Wh'] / bess_df['timestamp_diff']
                bess_df.loc[bess_df['power']>5000,'Wh']=5000
                

                bess_df.rename(columns = {'timestamp':'timestamp_end'},inplace = True)
                bess_df.dropna(subset='timestamp_start',inplace=True)
                bess_df.reset_index(inplace = True)

            # If there is a inverter id get inverter data
            if 'inverter' in der_df['der_type'].unique():
                inverter_id = der_df.loc[der_df['der_type'] == 'inverter','id'].iloc[0]
                inverter_df = pearlxFlexAPIConnect.pearlXFlexGetProductionMeterData(auth, site_id, inverter_id, 'meter-data', analysis_start_date_str, analysis_end_date_str)
                
                inverter_df['timestamp'] = pd.to_datetime(inverter_df['timestamp'])
                inverter_df['inverter_production_meter'] = inverter_df['inverter_production_meter'].astype(float)
                inverter_df.sort_values(by = ['timestamp'],inplace = True)

                inverter_df['Wh'] = inverter_df['inverter_production_meter'].diff()

                inverter_df['timestamp_start'] = inverter_df['timestamp'].shift(1)

                inverter_df.rename(columns = {'timestamp':'timestamp_end'},inplace = True)
                inverter_df.dropna(subset='timestamp_start',inplace=True)
                inverter_df.reset_index(inplace = True)
                
            # If unit is a PV + Storage unit reformat data
            if ('storage' in der_df['der_type'].unique()) and ('inverter' in der_df['der_type'].unique()):

                # Itererate through all the events
                if not battery_events.empty:
                    # Get all events that were delivered and are either a charge or discharge command
                    del_events = battery_events.loc[(battery_events['status'] == 'Delivered') & (battery_events['commandUri'].isin(['dischargeStorage']))].copy()
                    for index, row in del_events.iterrows():
                        event_start = row['startTimeLocal']
                        event_end = row['endTimeLocal']
                        event_command = row['commandUri']
                        event_id = row['id']

                        # if event_command == 'dischargeStorage':
                        power_command = float(row['parameters']['dischargePowerPercent'])

                        # elif event_command == 'chargeStorage':
                        #     power_command = row['parameters']['chargePowerPercent']

                        # else:
                        #     power_command = np.nan
                        
                        # get all timestamp ends where that are greater than the event and all timestamp starts that are less than the event end. The first row and the last row will likely start / end outside of the event times
                        temp_event_df = bess_df.loc[(bess_df['timestamp_end'] >= event_start) & (bess_df['timestamp_start'] <= event_end)].copy()
                        
                        if not temp_event_df.empty:
                            temp_event_df = temp_event_df[['timestamp_start','timestamp_end','Wh']]
                            temp_event_df = reformatSettlementData.settlementFormatShoulders(temp_event_df, event_start, event_end)
                            temp_event_df['eventCommand'] = event_command
                            temp_event_df['eventId'] = event_id
                            temp_event_df['powerCommand'] = power_command
                            event_performance_dict = {
                                'unit_number':unit_number,
                                'eventCommand':event_command,
                                'eventId':event_id,
                                'eventStart':event_start,
                                'eventEnd':event_end,
                                'powerCommand': power_command,
                                'targetEventEnergy':((event_end - event_start).total_seconds()/60/60) * (power_command / 100) * 5*1000,
                                'actualEventEnergy':temp_event_df['Wh'].sum()
                                }

                            event_performance_list.append(event_performance_dict)    
                
            
            # IF unit is a PV Only unit
            if 'inverter' in der_df['der_type'].unique():
                formatted_inverter_df = reformatSettlementData.settlementFormat(inverter_df[['timestamp_start','timestamp_end','Wh']])
                
                formatted_inverter_df = formatted_inverter_df.loc[(formatted_inverter_df['timestamp_start_hour'] >= analysis_start_date_str) & (formatted_inverter_df['timestamp_end_hour'] <= analysis_end_date_str)]
                formatted_inverter_df['unit_number'] = unit_number
                formatted_inverter_df['vnem_btm'] = system_config

                formatted_inverter_df.rename(columns = {'timestamp_start_hour':'timestamp_start','timestamp_end_hour':'timestamp_end'},inplace=True)

                missing_settlement_intervals += (expected_intervals - formatted_inverter_df.shape[0])
            
                formatted_inverter_df['occFlag'] = 0
                # Add flag for intervals where the unit was occupied
                for index, row in temp_occupancy.iterrows():
                    move_in = row['move_in']
                    analysis_end_date = row['move_out']
                    if not pd.isnull(analysis_end_date):
                        analysis_end_date = (analysis_end_date + pd.Timedelta(days = 1)).replace(hour = 0, minute = 0, second = 0)
                        formatted_inverter_df.loc[(formatted_inverter_df['timestamp_start']>=move_in) & (formatted_inverter_df['timestamp_end']<=analysis_end_date),'occFlag'] = 1
                    else:
                        formatted_inverter_df.loc[(formatted_inverter_df['timestamp_start']>=move_in),'occFlag'] = 1
                
                pvOnly_list.append(formatted_inverter_df)
        except Exception as e:
            missing_units += 1
            print(e)
            print(commission_df_row)
            pass


    # bess_all_df = pd.concat(bessPV_list)
    pv_all_df = pd.concat(pvOnly_list)
    pv_all_df.drop(columns = 'Wh',inplace = True)
    pv_all_df.rename(columns = {'total_production':'Wh'},inplace=True)
    pv_all_df['Wh'].clip(lower = 0, inplace=True)
    pv_all_df['timestamp_diff'] = pv_all_df['timestamp_start'].diff()
    pv_all_df['timestamp_diff'] = pv_all_df['timestamp_diff'].dt.total_seconds() / 3600
    pv_all_df['powerCalc'] = pv_all_df['Wh'] / pv_all_df['timestamp_diff']
    pv_all_df.loc[pv_all_df['powerCalc'] > 7600, 'Wh'] = 0
    pv_all_df = pv_all_df.drop(columns=['timestamp_diff','powerCalc'])
    units_evaluated = len(pv_all_df['unit_number'].unique())
    #prod/timediff, if > 
    pv_all_df.to_csv('allproductiondata.csv')
    # bess_all_df['Wh'].clip(lower = 0, inplace=True)
    pv_all_df['Wh'].clip(lower = 0, inplace = True)
    pv_all_df = pd.merge(pv_all_df, subscriber_df[['unit_number', 'utility_discount_program']], on='unit_number', how='left')
    
    occupiedCARE_bessPV_production = pv_all_df.loc[
        (pv_all_df['occFlag'] == 1) &
        (pv_all_df['vnem_btm'] == 'BTM-PVBESS') &
        (pv_all_df['utility_discount_program'].str.contains('CARE', na=False))
    ].copy()
    occCARE_bessPV_units = len(occupiedCARE_bessPV_production['unit_number'].unique())

    occupiedNonCARE_bessPV_production = pv_all_df.loc[
        (pv_all_df['occFlag'] == 1) &
        (pv_all_df['vnem_btm'] == 'BTM-PVBESS') &
        (~pv_all_df['utility_discount_program'].str.contains('CARE', na=False))
    ].copy()
    occNonCARE_bessPV_units = len(occupiedNonCARE_bessPV_production['unit_number'].unique())

    vacant_bessPV_production = pv_all_df.loc[(pv_all_df['occFlag'] == 0) & (pv_all_df['vnem_btm'] == 'BTM-PVBESS')].copy()
    vacant_bessPV_units = len(vacant_bessPV_production['unit_number'].unique())

    occupiedCARE_pvOnly_production = pv_all_df.loc[
        (pv_all_df['occFlag'] == 1) &
        (pv_all_df['vnem_btm'] == 'BTM-PV') &
        (pv_all_df['utility_discount_program'].str.contains('CARE', na=False))
    ].copy()
    occCARE_pvOnly_units = len(occupiedCARE_pvOnly_production['unit_number'].unique())

    occupiedNonCARE_pvOnly_production = pv_all_df.loc[
        (pv_all_df['occFlag'] == 1) &
        (pv_all_df['vnem_btm'] == 'BTM-PV') &
        (~pv_all_df['utility_discount_program'].str.contains('CARE', na=False))
    ].copy()
    occNonCARE_pvOnly_units = len(occupiedNonCARE_pvOnly_production['unit_number'].unique())

    vacant_pvOnly_production = pv_all_df.loc[(pv_all_df['occFlag'] == 0) & (pv_all_df['vnem_btm'] == 'BTM-PV')].copy()
    vacant_pvOnly_units = len(vacant_pvOnly_production['unit_number'].unique())

    output_list = []
    
    # Calculate production output based on PV or PV + BESS and Vacant or Occupied
    df_total_production_bessPV_occCARE = occupiedCARE_bessPV_production[['timestamp_start','timestamp_end','Wh']].groupby(by = ['timestamp_start', 'timestamp_end']).sum().reset_index()
    if not df_total_production_bessPV_occCARE.empty:
        bessPV_tou_occ_care = touProductionAnalysis(df_total_production_bessPV_occCARE, 'bessPVOccupiedCARE_'+nem_ix,start_date,end_date)
        bessPV_tou_occ_care['units_included'] = occCARE_bessPV_units
        output_list.append(bessPV_tou_occ_care)

    df_total_production_bessPV_occNonCARE = occupiedNonCARE_bessPV_production[['timestamp_start','timestamp_end','Wh']].groupby(by = ['timestamp_start', 'timestamp_end']).sum().reset_index()
    if not df_total_production_bessPV_occNonCARE.empty:
        bessPV_tou_occ_noncare = touProductionAnalysis(df_total_production_bessPV_occNonCARE, 'bessPVOccupiedNonCARE_'+nem_ix,start_date,end_date)
        bessPV_tou_occ_noncare['units_included'] = occNonCARE_bessPV_units
        output_list.append(bessPV_tou_occ_noncare)
    
    df_total_production_bessPV_vacant = vacant_bessPV_production[['timestamp_start','timestamp_end','Wh']].groupby(by = ['timestamp_start', 'timestamp_end']).sum().reset_index()
    if not df_total_production_bessPV_vacant.empty:
        bessPV_tou_vacant = touProductionAnalysis(df_total_production_bessPV_vacant, 'bessPVVacant_'+nem_ix,start_date,end_date)
        bessPV_tou_vacant['units_included'] = vacant_bessPV_units
        output_list.append(bessPV_tou_vacant)

    df_total_production_pvOnly_occCARE = occupiedCARE_pvOnly_production[['timestamp_start','timestamp_end','Wh']].groupby(by = ['timestamp_start', 'timestamp_end']).sum().reset_index()
    if not df_total_production_pvOnly_occCARE.empty:
        pvOnly_tou_occCARE = touProductionAnalysis(df_total_production_pvOnly_occCARE, 'pvOnlyOccupiedCARE_'+nem_ix,start_date,end_date)
        pvOnly_tou_occCARE['units_included'] = occCARE_pvOnly_units
        output_list.append(pvOnly_tou_occCARE)

    df_total_production_pvOnly_occNonCARE = occupiedNonCARE_pvOnly_production[['timestamp_start','timestamp_end','Wh']].groupby(by = ['timestamp_start', 'timestamp_end']).sum().reset_index()
    if not df_total_production_pvOnly_occNonCARE.empty:
        pvOnly_tou_occNonCARE = touProductionAnalysis(df_total_production_pvOnly_occNonCARE, 'pvOnlyOccupiedNonCARE_'+nem_ix,start_date,end_date)
        pvOnly_tou_occNonCARE['units_included'] = occNonCARE_pvOnly_units
        output_list.append(pvOnly_tou_occNonCARE)
        
    df_total_production_pvOnly_vacant = vacant_pvOnly_production[['timestamp_start','timestamp_end','Wh']].groupby(by = ['timestamp_start', 'timestamp_end']).sum().reset_index()
    if not df_total_production_pvOnly_vacant.empty:
        pvOnly_tou_vacant = touProductionAnalysis(df_total_production_pvOnly_vacant, 'pvOnlyVacant_'+nem_ix,start_date,end_date)
        pvOnly_tou_vacant['units_included'] = vacant_pvOnly_units
        output_list.append(pvOnly_tou_vacant)

    missing_intervals_df = pd.DataFrame(data = {
        'Missing Intervals':missing_settlement_intervals,
        'Units Evaluated': units_evaluated
    }, index=['Data Metrics'])
    
    if event_performance_list:
        eventPerformance_df = pd.DataFrame(event_performance_list)
        eventPerformance_df.to_csv('events.csv')
        eventPerformanceSummary_dict = {
            'total Target Event Energy [kWhdc]':eventPerformance_df['targetEventEnergy'].sum()/1000,
            'total Actual Event Energy [kWhdc]':eventPerformance_df['actualEventEnergy'].sum()/1000,
            'total Events [#]':eventPerformance_df.shape[0],
            'unique systems dispatched [#]':len(eventPerformance_df['unit_number'].unique())
        }

        eventPerformanceSummary_df = pd.DataFrame(eventPerformanceSummary_dict,index = ['values']).transpose()
    else:
        eventPerformanceSummary_df = pd.DataFrame()

    # print(pd.concat(output_list + [missing_intervals_df]))
    return pd.concat(output_list + [missing_intervals_df]), eventPerformanceSummary_df

def touProductionAnalysis(df, row_name, startDate_str, endDate_str):
    
    """ TOU Production analysis
    
    Arguments:
        df {pandas dataframe} -- processed pandas dataframe with usage data {'timestamp_end','Wh}
            timestamp_end - datetime HE
            timestamp_start - datetime HE
            Wh - in watt-hours

    Keyword Arguments:

    Returns:
        [pandas dataframe] -- df with data formatted to settlement intervals
    """  

    df['timestamp_start'] = pd.to_datetime(df['timestamp_start'])
    df_filter_time = df.loc[(df['timestamp_start']>= startDate_str) & (df['timestamp_start']<= endDate_str + " 23:59:59")].copy()
    
    if df_filter_time.empty:
        return None
        # raise ValueError('Data does not contain startdate and endate time ranges')

    TOU_gen_df = touMapper.touMapper('utils/tariffMap - TOUD49.xlsx', df,row_name,startDate_str, endDate_str)
    
    touGen_df = TOU_gen_df.drop(columns = ['start','end']) / 1000 # Wh to kWh to MWh
    # touGen_df.rename(columns = {
    #     "S Peak": "Peak Production (MWh)",
    #     "S Mid Peak": "Mid Peak Production (MWh)",
    #     "S Off Peak": "Off Peak Production (MWh)",
    #     "W Mid Peak": "Peak Production (MWh)",
    #     "W Off Peak": "Mid Peak Production (MWh)",
    #     "W Super Off": "Off Peak Production (MWh)"
    # },inplace = True)
    
    return touGen_df