import pandas as pd
import numpy as np
import datetime as dt
import json

import dash
from dash import Dash, dcc, html, Input, Output, callback, State, dash_table
import dash_auth
import plotly as plotly
import plotly.graph_objects as go

from utils import subscriber_db_calcs
import glob

today_datetime = dt.datetime.today()

current_date_dt = today_datetime.date()
month_start_dt = today_datetime.date().replace(day = 1, month = 1)

current_date_str = current_date_dt.strftime('%Y-%m-%d')
month_startdate_str = month_start_dt.strftime('%Y-%m-%d')

with open("creds.json") as f:
    data=f.read()
credentials = json.loads(data)

VALID_USERNAME_PASSWORD_PAIRS = {
    'hello': 'world'
}

app = dash.Dash(__name__)

auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
)

reportingParams = pd.read_csv('reportParams.csv')
nscr_rates = pd.read_csv('NSCR Rates.csv')
nscr_rates['Relevant Period'] = pd.to_datetime(nscr_rates['Relevant Period'], format = '%m/%d/%y')
app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Img(
                                        src=app.get_asset_url("images/static/Pearlx-logo.png"),
                                        className = "logo",
                                        style = {
                                            "float":"left",
                                            "width":"20%",
                                            "height":"20%",
                                            }),
                                        html.Br(),
                                        html.H5(
                                            'PearlX Operations Report',
                                            style = {
                                                "color":"#3f2aa5",
                                                "margin-bottom":"0px",
                                                "float":"left",
                                                "padding-left":"10px",
                                                "font-weight":"bold"
                                            }
                                        )
                                    ],className = 'row'
                                ),
                                html.Br(),
                                html.Div(
                                    [
                                        html.Div(id = 'property-info',
                                        style = {
                                            "float":"left"
                                        },
                                        )
                                    ],
                                    className = 'row'
                                ),

                        ],
                            
                    style={
                        "float":"left"
                        }
                    ),
                ],
                    className="row"
                ),
                html.Br(),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div([
                                    dcc.Dropdown(['Quail Ridge','High Desert Villas'], value = 'High Desert Villas',id = 'community-dropdown')],className = 'six columns'
                                )
                        ],className = 'row'),
                        html.Br(),
                        html.Div([
                            html.Div([
                                'Report Range',
                                dcc.DatePickerRange(
                                    id='report-range-date',
                                    min_date_allowed=dt.date(2022, 1, 1),
                                    month_format='MMM Do, YY',
                                    start_date=month_start_dt,
                                    end_date = current_date_dt
                                )
                                ]),
                        ], className = 'row'),
                        html.Br(),

                        html.Div(
                            [
                                html.Button('Refresh', id = 'refresh-data-button', n_clicks=0, className='row'),
                                ], className = 'row'),
                        
                        html.Br(),
                        html.Div(
                            [
                                dcc.Graph(
                                    id = 'commission-graph',
                                    style={
                                        'textAlign':'center'
                                    }
                                )
                            ],
                            className="row"
                            ),
                        html.Br(),
                        html.Div(
                            [                               
                                html.H6(
                                    "Commissioned Units - MTD",
                                    style = {
                                        'font-family':'sans-serif',
                                        "font-weight":"bold",
                                        "color":"#3f2aa5",
                                        "font-size":"14px"
                                        }
                                    ),
                                html.Div(id = 'live-update-table-one',
                        style = {"margin-bottom":"7px"})
                            ],
                            className = "six columns"
                        ),

                        html.Div(
                            [                               
                                html.H6(
                                    "Commissioned Units - YTD",
                                    style = {
                                        'font-family':'sans-serif',
                                        "font-weight":"bold",
                                        "color":"#3f2aa5",
                                        "font-size":"14px"
                                        }
                                    ),
                                html.Div(id = 'live-update-table-two',
                        style = {"margin-bottom":"7px"})
                            ],
                            className = "six columns"
                        ),
                                                
                        # dcc.Interval(id = 'interval-component',
                        # interval = 1000*10,
                        # n_intervals = 0)
                    ]
                    ,className="row", style = {"padding-left":"10px"}),
                
                html.Hr(),
                html.Div(
                    [
                        dcc.Graph(
                            id = 'billables-graph',
                            style={
                                'textAlign':'center'
                            }
                        )
                    ],
                    className="row"
                    ),    
                html.Hr(),
                html.Div(
                    [
                        dcc.Graph(
                            id = 'volc-graph',
                            style={
                                'textAlign':'center'
                            }
                        )
                    ],
                    className="row"
                    ),    
                html.Hr(),
                html.Div(
                    [
                        dcc.Graph(
                            id = 'production-graph',
                            style={
                                'textAlign':'center'
                            }
                        )
                    ],
                    className="row"
                    ),                                    
                html.Hr(),
                html.Div(
                    [
                        dcc.Graph(
                            id = 'production-bucketing-graph',
                            style={
                                'textAlign':'center'
                            }
                        )
                    ],
                    className="row"
                    ),               
                html.Hr(),
                html.Div(
                    [
                        dcc.Graph(
                            id = 'production-dist-graph',
                            style={
                                'textAlign':'center'
                            }
                        )
                    ],
                    className="row"
                    ),
            ]
        ),

    ]
)


@callback(Output('live-update-table-one', 'children'),
          Output('live-update-table-two', 'children'),
          Output('property-info', 'children'),
          Output('commission-graph','figure'),
          Output('production-graph','figure'),
          Output('production-dist-graph','figure'),
          Output('production-bucketing-graph','figure'),
          Output('volc-graph','figure'),
          Output('billables-graph','figure'),
          Input('refresh-data-button', 'n_clicks'),
          State('community-dropdown','value'),
          State('report-range-date','start_date'),
          State('report-range-date','end_date'))
def update_metrics(n_clicks, community, report_start_date, report_end_date):
    
    reportStart_datetime = dt.datetime.strptime(report_start_date, '%Y-%m-%d')
    reportYear = reportStart_datetime.year
    prevYear = reportYear - 1
    reportStart_month = reportStart_datetime.month
    reportStart_day = reportStart_datetime.day

    report_end_datetime = dt.datetime.strptime(report_end_date, '%Y-%m-%d')
    report_end_month = report_end_datetime.month
    report_end_day = report_end_datetime.day

    communityTitle = community.title()
    if reportStart_datetime > report_end_datetime:

        return 'Report start time greater than report end time'
    
    # Values underwritten
    reportingParams_working = reportingParams.loc[reportingParams['community'] == communityTitle]
    occupancy_finModel = reportingParams_working['occupancy'].iloc[0]
    care_finModel = reportingParams_working['care'].iloc[0]

    ## Get subscriber, units, and property info
    db_data = subscriber_db_calcs.runSubscriberCalcs(credentials, community, report_start_date, report_end_date)

    addressInfo = db_data[2]['address'].iloc[0]
    unitsInfo = db_data[2]['total_units'].iloc[0]
    utility_info = db_data[2]['iou'].iloc[0]
    
    # All units
    units_table_all = db_data[3]

    # Only commissioned units
    units_table = units_table_all.loc[~units_table_all['commission_date'].isnull()].copy()

    # Get Last Year Prod File
    prevYear_files = glob.glob(f'Production Data Summary/{community}/{prevYear}/*.csv')
    
    # get previous years production files
    if prevYear_files:
        
        prevYear_prod_df = pd.read_csv(prevYear_files[0],index_col = 0)
        prevYear_prod_df['date'] = pd.to_datetime(prevYear_prod_df['date'])
        prevYear_prod_df['Month'] = prevYear_prod_df['date'].dt.month
        prevYear_prod_df['Day'] = prevYear_prod_df['date'].dt.day
        
        prevYear_prod_group = prevYear_prod_df[['Month','total_production']].groupby(by = ['Month']).sum().reset_index()
        prevYear_prod_group['total_production_mwh'] = prevYear_prod_group['total_production']/1000
        
    else:
        # If no file found fill with zeros
        prevYear_prod_group = pd.DataFrame(data = {'Month':list(range(1,13)), 'total_production_mwh':[0] * 12})
    
    # Get all current year production files
    prodFiles = glob.glob(f'Production Data Summary/{communityTitle}/*csv')
    
    # Parse out date in filename
    prodFilesDate_list = [file.split('/')[-1].split('.')[0] for file in prodFiles]

    # convert text date over to datetime and sort
    prodFilesDate_list = list(map(lambda x: dt.datetime.strptime(x, '%d %b %y'),prodFilesDate_list))
    prodDates_array = np.array(np.sort(np.array(prodFilesDate_list)))
    
    # Get latest production file date
    prodFile = prodDates_array[-1].strftime('%-d %b %y').upper()

    # Read latest production file
    prod_df = pd.read_csv(f'Production Data Summary/{communityTitle}/{prodFile}.csv')
    prod_df['date'] = pd.to_datetime(prod_df['date'])
    prod_df['Month'] = prod_df['date'].dt.month
    prod_df['Month-Year'] = prod_df['date'].dt.strftime('%b %Y')

    # Only get production data within current report range
    prod_df = prod_df.loc[((prod_df['date'] >= report_start_date) & (prod_df['date'] <= report_end_date))].copy()

    prod_group = prod_df[['Month','Month-Year','total_production']].groupby(by = ['Month','Month-Year']).sum().reset_index()
    prod_group['total_production_mwh'] = prod_group['total_production']/1000

    # Calculate total production within report range
    ytdActualProduction = prod_group['total_production_mwh'].sum()
    
    ytdProdActual_df = pd.DataFrame({'date':['YTD'],
                                       'total_production_mwh':[ytdActualProduction],
                                       'Month-Year':['YTD']}, index = [0])
    
    # Output dataframe for plotting
    prod_group_plot = pd.concat([prod_group,ytdProdActual_df])

    ## View the distribution of the production data across units
    prod_dist = prod_df.loc[((prod_df['date'] >= report_start_date) & (prod_df['date'] <= report_end_date))].copy()
    units_temp = units_table.loc[units_table['commission_date'] <= report_start_date, ['unit_number','vnem_btm']].copy()
    prod_dist = prod_dist.groupby(by = ['unit','vnem_btm']).agg({'total_production':'sum'}).reset_index()

    # Only look at distribution for units that were fully commissioned within the reporting range
    prod_dist = units_temp.merge(prod_dist, left_on = ['unit_number','vnem_btm'], right_on = ['unit','vnem_btm'], how = 'left')
    prod_dist.fillna(value = {'total_production':0},inplace = True)

    # Get commissioned units database output
    commission_df = db_data[0]
    commission_df.sort_values(by = ['endDate'], inplace = True)

    commission_df['date'] = pd.to_datetime(commission_df['endDate']).dt.date

    # Convert commissioned unit days over percentages
    commission_df['Occupancy % - Commissioned Units'] = commission_df['Commissioned & Occupied Days'] / commission_df['Commissioned Days']
    commission_df['CARE %'] = commission_df['Days with Units on CARE'] / commission_df['Commissioned & Occupied Days']


    # commissionSummary is a YTD summary
    # Get first and last entry from commissioned df return
    commissionSummary_dict = {'Starting Units Commissioned':commission_df['Starting Units Commissioned'].iloc[0],
                              'Ending Units Commissioned':commission_df['Ending Units Commissioned'].iloc[-1]}

    commissionSummarySum_dict = commission_df[[
        'Commissioned & Occupied Days',
        'Commissioned Days',
        'Days with Units on CARE',
        'Days with Units Not on CARE']].sum().to_dict()
    
    commissionSummary_dict.update(commissionSummarySum_dict)
    commissionSummary_df = pd.DataFrame(commissionSummary_dict, index = [0])

    # Calculate occpuancy and care percentages
    commissionSummary_df['Occupancy % - Commissioned Units'] = commissionSummary_df['Commissioned & Occupied Days'] / commissionSummary_df['Commissioned Days']
    commissionSummary_df['CARE %'] = commissionSummary_df['Days with Units on CARE'] / commissionSummary_df['Commissioned & Occupied Days']

    ## Convert Float to Percetange
    commissionSummary_df['Occupancy % - Commissioned Units'] = '{:.1%}'.format(commissionSummary_df['Occupancy % - Commissioned Units'].iloc[0])
    commissionSummary_df['CARE %'] = '{:.1%}'.format(commissionSummary_df['CARE %'].iloc[0])

    commissionSummary_df = commissionSummary_df[['Starting Units Commissioned', 'Ending Units Commissioned','Occupancy % - Commissioned Units','CARE %']]
    commissionSummary_df = commissionSummary_df.T
    commissionSummary_df.reset_index(inplace=True)
    commissionSummary_df.rename(columns={"index":"Commission Units Summary", 0:'Actual'},inplace=True)
    
    # commission_df_table is MTD summary
    commission_df_table = commission_df[['Starting Units Commissioned', 'Ending Units Commissioned','Occupancy % - Commissioned Units', 'CARE %']].iloc[-1]
    commission_df_table['Occupancy % - Commissioned Units'] = '{:.0%}'.format(commission_df_table['Occupancy % - Commissioned Units'])
    commission_df_table['CARE %'] = '{:.0%}'.format(commission_df_table['CARE %'])
    
    commission_df_table = pd.DataFrame(commission_df_table)
    commission_df_table['Modeled'] = [unitsInfo,unitsInfo,'{:.0%}'.format(occupancy_finModel),'{:.0%}'.format(care_finModel)]

    commission_df_table.reset_index(inplace = True)
    commission_df_table.rename(columns = {'index':'Commission Units Summary',0:'Actual'},inplace = True)

    ### Modeled settlement values
    modeledSettlements_fn = glob.glob(f'budgetData/{communityTitle}/*.csv')
    modeledSettlements = pd.read_csv(modeledSettlements_fn[0])

    # Merge with underwriting parameters
    modeledSettlements = modeledSettlements.merge(reportingParams_working, left_on = ['vnem_btm'], right_on = ['vnem_btm'], how = 'left')
    modeledSettlements['start'] = pd.to_datetime(modeledSettlements['start'], format = '%m/%d/%y')
    modeledSettlements['VOLC'] = modeledSettlements['gross_energy_bill_non_care'] * (1-modeledSettlements['care']) * modeledSettlements['occupancy'] + modeledSettlements['gross_energy_bill_care'] * (modeledSettlements['care']) * modeledSettlements['occupancy']
    
    # get unit counts for entire community not just commissoned
    unit_counts = units_table_all.groupby(by = ['vnem_btm']).agg({'unit_number':'count'}).reset_index()
    unit_counts.rename(columns = {'unit_number':'unit_count'},inplace = True)

    # Merge with unit counts for the different vnem_btm IX
    modeledSettlements = modeledSettlements.merge(unit_counts, left_on = ['vnem_btm'], right_on= ['vnem_btm'], how = 'left')
    modeledSettlements['VOLC'] = modeledSettlements['VOLC'] * modeledSettlements['unit_count']
    modeledSettlements['total_peak_production'] = modeledSettlements['peak_production'] * modeledSettlements['unit_count']
    modeledSettlements['total_production'] = modeledSettlements['production'] * modeledSettlements['unit_count']
    modeledSettlements = modeledSettlements.merge(nscr_rates, left_on = ['start','utility'], right_on = ['Relevant Period','iou'], how = 'left')
    modeledSettlements['NSCR'] = modeledSettlements['NSCR'].ffill()

    modeledBillables = modeledSettlements.copy()
    modeledBillables['Subscription Non CARE'] = (modeledBillables['gross_energy_bill_non_care'] - modeledBillables['net_energy_bill_non_care'].clip(lower = 0))
    modeledBillables['Subscription CARE'] = (modeledBillables['gross_energy_bill_care'] - modeledBillables['net_energy_bill_care'].clip(lower = 0))

    modeledBillables['Total Energy Positive Charges Non CARE'] = modeledBillables['net_energy_bill_non_care'].clip(lower = 0)
    modeledBillables['Total Energy Export Credits Non CARE'] = modeledBillables['net_energy_bill_non_care'].clip(upper = 0)
    
    modeledBillables['Total Energy Positive Charges CARE'] = modeledBillables['net_energy_bill_care'].clip(lower = 0)
    modeledBillables['Total Energy Export Credits CARE'] = modeledBillables['net_energy_bill_care'].clip(upper = 0)

    modeledCumlCharges = modeledBillables.groupby(by = ['vnem_btm'])[['Total Energy Positive Charges Non CARE',
                                                                      'Total Energy Export Credits Non CARE',
                                                                      'net_energy_bill_non_care',
                                                                      'Total Energy Positive Charges CARE',
                                                                      'Total Energy Export Credits CARE',
                                                                      'net_energy_bill_care',
                                                                      'net_load']].apply(lambda x: x.cumsum()).reset_index()
    
    modeledCumlCharges.rename(columns = {'Total Energy Positive Charges Non CARE':'Cuml Energy Charges Non CARE',
                                         'Total Energy Export Credits Non CARE':'Cuml Energy Export Credits Non CARE',
                                         'net_energy_bill_non_care':'Cuml Energy Export Credit Bank Non CARE',
                                         'Total Energy Positive Charges CARE': 'Cuml Energy Charges CARE',
                                         'Total Energy Export Credits CARE':'Cuml Energy Export Credits CARE',
                                         'net_energy_bill_care':'Cuml Energy Export Credit Bank CARE',
                                         'net_load':'Cuml Net Load'}, inplace = True)
    modeledCumlCharges.drop(columns = ['vnem_btm'],inplace = True)
    
    modeledBillables = modeledBillables.merge(modeledCumlCharges, left_index = True, right_on = ['level_1'], how = 'left')
    modeledBillablesp1 = modeledBillables.groupby(by = ['vnem_btm'])[['Cuml Energy Export Credits Non CARE','Cuml Energy Charges Non CARE',
                                                                      'Cuml Energy Export Credits CARE','Cuml Energy Charges CARE']].shift(1)
    
    modeledBillablesp1.rename(columns = {'Cuml Energy Export Credits Non CARE':'Export Credits Cuml Shift Non CARE',
                                         'Cuml Energy Charges Non CARE':'Energy Charges Cuml Shift Non CARE',
                                         'Cuml Energy Export Credits CARE':'Export Credits Cuml Shift CARE',
                                         'Cuml Energy Charges CARE':'Energy Charges Cuml Shift CARE'},inplace = True)
    
    modeledBillables = modeledBillables.merge(modeledBillablesp1, left_index = True, right_index=True, how = 'left')
    modeledBillables.fillna(value = {'Export Credits Cuml Shift Non CAREt':0,
                                     'Energy Charges Cuml Shift Non CARE':0,
                                     'Energy Credits Cuml Shift CARE':0,
                                     'Energy Charges Cuml Shift CARE':0},inplace=True)

    # modeledBillables['Net Surplus Volume'] =  modeledBillables['Cuml Net Load'].clip(upper = 0)
    modeledBillables['Remaining Current Month Credits Non CARE'] = (modeledBillables['Export Credits Cuml Shift Non CARE'] + modeledBillables['Energy Charges Cuml Shift Non CARE']).clip(upper = 0)
    modeledBillables['Remaining Current Month Credits flipped Non CARE'] = modeledBillables['Remaining Current Month Credits Non CARE'] * -1
    modeledBillables['Credits Utilized Current Month Non CARE'] = modeledBillables[['Remaining Current Month Credits flipped Non CARE','Total Energy Positive Charges Non CARE']].min(axis = 1)
    modeledBillables['Billables Non CARE'] = modeledBillables['Subscription Non CARE'] + modeledBillables['Credits Utilized Current Month Non CARE'] + -1*modeledBillables['net_load'].clip(upper = 0)  * modeledBillables['NSCR']
    modeledBillables['Billables Non CARE'] = modeledBillables['Billables Non CARE'] * (1-modeledBillables['discount_rate'])
    modeledBillables['Total Billables Non CARE'] = modeledBillables['Billables Non CARE'] * modeledBillables['unit_count']

    modeledBillables['Remaining Current Month Credits CARE'] = (modeledBillables['Export Credits Cuml Shift CARE'] + modeledBillables['Energy Charges Cuml Shift CARE']).clip(upper = 0)
    modeledBillables['Remaining Current Month Credits flipped CARE'] = modeledBillables['Remaining Current Month Credits CARE'] * -1
    modeledBillables['Credits Utilized Current Month CARE'] = modeledBillables[['Remaining Current Month Credits flipped CARE','Total Energy Positive Charges CARE']].min(axis = 1)
    modeledBillables['Billables CARE'] = modeledBillables['Subscription CARE'] + modeledBillables['Credits Utilized Current Month CARE'] + -1*modeledBillables['net_load'].clip(upper = 0)  * modeledBillables['NSCR']
    modeledBillables['Billables CARE'] = modeledBillables['Billables CARE'] * (1-modeledBillables['discount_rate'])
    modeledBillables['Total Billables CARE'] = modeledBillables['Billables CARE'] * modeledBillables['unit_count']
    
    modeledBillables['Total Billables'] = modeledBillables['Total Billables Non CARE'] * (1-modeledSettlements['care']) * modeledSettlements['occupancy'] + modeledBillables['Total Billables CARE'] * (modeledSettlements['care']) * modeledSettlements['occupancy']
    modeledBillables['Month-Year'] = modeledBillables['start'].dt.strftime('%b %Y')
    modeledBillablesPlot = modeledBillables.groupby(by = ['Month-Year','start']).agg({'Total Billables':'sum'}).reset_index()
    modeledBillablesPlot.sort_values(by = ['start'],inplace = True)
    # Billables YTD Calc
    modeledBillablesYTD = modeledBillables.copy()    

    ## Adjust for modeling period
    modeledBillablesYTD = modeledBillablesYTD.loc[modeledBillablesYTD['start'] <= report_end_datetime]

    ## Get Last NSCR rate set before the report end date
    nscr_rate_ytd = modeledBillablesYTD['NSCR'].iloc[-1]

    modeledBillablesYTD = modeledBillablesYTD.groupby(by = ['vnem_btm']).agg({'gross_energy_bill_non_care':'sum', 'net_energy_bill_non_care':'sum',
                                                                              'gross_energy_bill_care':'sum', 'net_energy_bill_care':'sum',
                                                                              'net_load':'sum','unit_count':'max','discount_rate':'max',
                                                                              'care':'max','occupancy':'max'}).reset_index()
    
    modeledBillablesYTD['net_energy_bill_non_care'] = modeledBillablesYTD['net_energy_bill_non_care'].clip(lower = 0)
    modeledBillablesYTD['net_energy_bill_care'] = modeledBillablesYTD['net_energy_bill_care'].clip(lower = 0)

    modeledBillablesYTD['Total Subscriptions Non CARE'] = modeledBillablesYTD['gross_energy_bill_non_care'] - modeledBillablesYTD['net_energy_bill_non_care']
    modeledBillablesYTD['Total Subscriptions CARE'] = modeledBillablesYTD['gross_energy_bill_care'] - modeledBillablesYTD['net_energy_bill_care']
    modeledBillablesYTD['Net Surplus Value $'] = -1*modeledBillablesYTD['net_load'].clip(upper = 0) * nscr_rate_ytd
    modeledBillablesYTD['Billables Non CARE'] = (modeledBillablesYTD['Total Subscriptions Non CARE'] + modeledBillablesYTD['Net Surplus Value $'] ) * (1-modeledBillablesYTD['discount_rate'])
    modeledBillablesYTD['Billables CARE'] = (modeledBillablesYTD['Total Subscriptions CARE'] + modeledBillablesYTD['Net Surplus Value $']) * (1-modeledBillablesYTD['discount_rate'])
    
    modeledBillablesYTD['Total Billables Non CARE'] = modeledBillablesYTD['Billables Non CARE'] * modeledBillablesYTD['unit_count']
    modeledBillablesYTD['Total Billables CARE'] =  modeledBillablesYTD['Billables CARE'] * modeledBillablesYTD['unit_count']

    modeledBillablesYTD['Total Billables'] = modeledBillablesYTD['Total Billables Non CARE'] * (1-modeledBillablesYTD['care']) * modeledBillablesYTD['occupancy'] + modeledBillablesYTD['Total Billables CARE'] * (modeledBillablesYTD['care']) * modeledBillablesYTD['occupancy']
    ytd_billablessModeled_df = pd.DataFrame({'Month-Year':['YTD'],
                                             'Total Billables':[modeledBillablesYTD['Total Billables'].sum()]}, index = [0])
    modeledBillablesPlot = pd.concat([modeledBillablesPlot, ytd_billablessModeled_df])

    # create summary dataframe grouped by different vnem_btm types
    monthly_modeled_plot = modeledSettlements.groupby(by = ['start','end','vnem_btm']).agg({'VOLC':'sum','total_peak_production':'sum','total_production':'sum'}).reset_index()
    monthly_modeled_plot['start'] = pd.to_datetime(monthly_modeled_plot['start'], format= '%m/%d/%y')
    monthly_modeled_plot['end'] = pd.to_datetime(monthly_modeled_plot['end'], format= '%m/%d/%y')
    monthly_modeled_plot['Month-Year'] = monthly_modeled_plot['start'].dt.strftime('%b %Y')

    # Summary for all vnem_btm types
    monthly_all_plot = monthly_modeled_plot.groupby(by = ['start','Month-Year']).agg({'VOLC':'sum','total_production':'sum'}).reset_index()
    monthly_all_plot['total_production_mwh'] = monthly_all_plot['total_production'] / 1000
    monthly_all_plot.sort_values(by = ['start'], inplace = True)

    ytd_modeledVOLC = monthly_all_plot.loc[monthly_all_plot['start'] <= report_end_datetime,'VOLC'].sum()
    ytd_modeledProduction = monthly_all_plot.loc[monthly_all_plot['start'] <= report_end_datetime,'total_production_mwh'].sum()
    ytd_modeled_df = pd.DataFrame({'start':['YTD'],
                                       'end':['YTD'],
                                       'VOLC':[ytd_modeledVOLC],
                                       'total_production_mwh':[ytd_modeledProduction],
                                       'Month-Year':['YTD']}, index = [0])
    
    modeledYTD_plot_df = pd.concat([monthly_all_plot,ytd_modeled_df])

    # Summary for BTM-PVBESS specfically
    monthlyPeakPlot_df = monthly_modeled_plot.loc[monthly_modeled_plot['vnem_btm'] == 'BTM-PVBESS'].copy()
    monthlyPeakPlot_df = monthlyPeakPlot_df.groupby(by = ['start','Month-Year']).agg({'total_peak_production':'sum'}).reset_index()
    ytdMonthlyPeakProduction = monthlyPeakPlot_df.loc[monthlyPeakPlot_df['start'] <= report_end_datetime,'total_peak_production'].sum()
    
    ytd_modeledPeak_df = pd.DataFrame({'start':['YTD'],
                                       'total_peak_production':[ytdMonthlyPeakProduction],
                                       'Month-Year':['YTD']}, index = [0])
    
    modeledPeakYTD_plot_df = pd.concat([monthlyPeakPlot_df,ytd_modeledPeak_df])

    # Get acutal settlement files for the last 12 months
    settlement_files = glob.glob(f'actualSettlements/{communityTitle}/*.csv')
    settlementFilesDate_list = [file.split('/')[-1].split('.')[0] for file in settlement_files]

    settlementFilesDate_list = list(map(lambda x: dt.datetime.strptime(x, '%d %b %y'),settlementFilesDate_list))
    settlementFiles_array = np.array(np.sort(np.array(settlementFilesDate_list)))[-12:]
    settlementFilename_list = [settlementDatetime.strftime('%-d %b %y').upper() for settlementDatetime in settlementFiles_array]

    settlements_df_list = []
    for settlementFileDate in settlementFilename_list:
        temp_settlement = pd.read_csv(f'actualSettlements/{communityTitle}/{settlementFileDate}.csv')
        settlements_df_list.append(temp_settlement)
    
    all_settlements = pd.concat(settlements_df_list)
    all_settlements['start'] = pd.to_datetime(all_settlements['start'], format = '%m/%d/%y')
    all_settlements.sort_values(by = ['start'],inplace = True)
    all_settlements['Month-start'] = all_settlements['start'].dt.month.astype(str) + '/' + str(1) + '/' + all_settlements['start'].dt.year.astype(str)
    all_settlements['Month-start'] = pd.to_datetime(all_settlements['Month-start'])
    all_settlements = all_settlements.merge(nscr_rates, left_on = ['Month-start','iou'], right_on = ['Relevant Period','iou'], how = 'left')

    # Settle each tenant in the settlement file based on CARE vs Non CARE for the entire bill and just the energy charges
    all_settlements['Gross Bill'] = all_settlements['gross_bill_non_care']
    all_settlements.loc[all_settlements['care'] == 'Y','Gross Bill'] = all_settlements.loc[all_settlements['care'] == 'Y','gross_bill_care']

    all_settlements['Net Bill'] = all_settlements['net_bill_non_care']
    all_settlements.loc[all_settlements['care'] == 'Y','Net Bill'] = all_settlements.loc[all_settlements['care'] == 'Y','net_bill_care']

    all_settlements['Gross Energy Charges'] = all_settlements['gross_energy_bill_non_care']
    all_settlements.loc[all_settlements['care'] == 'Y','Gross Energy Charges'] = all_settlements.loc[all_settlements['care'] == 'Y','gross_energy_bill_care']

    all_settlements['Net Energy Charges'] = all_settlements['net_energy_bill_non_care']
    all_settlements.loc[all_settlements['care'] == 'Y','Net Energy Charges'] = all_settlements.loc[all_settlements['care'] == 'Y','net_energy_bill_care']

    all_settlements['Total Energy Positive Charges'] = all_settlements['Net Energy Charges'].clip(lower = 0)
    all_settlements['Total Energy Export Credits'] = all_settlements['Net Energy Charges'].clip(upper = 0)
    
    # Settlements calcs
    all_settlements['Subscription'] = all_settlements['Gross Bill'] - all_settlements['Net Bill']
    
    actualCumlCharges = all_settlements.groupby(by = ['name'])[['Total Energy Positive Charges',
                                                                      'Total Energy Export Credits',
                                                                      'Net Bill',
                                                                      'net_load']].apply(lambda x: x.cumsum()).reset_index()
    
    actualCumlCharges.rename(columns = {'Total Energy Positive Charges':'Cuml Energy Charges',
                                         'Total Energy Export Credits':'Cuml Energy Export Credits',
                                         'Net Bill':'Cuml Energy Export Credit Bank',
                                         'net_load':'Cuml Net Load'}, inplace = True)
    actualCumlCharges.drop(columns = ['name'],inplace = True)
    
    all_settlements = all_settlements.merge(actualCumlCharges, left_index = True, right_on = ['level_1'], how = 'left')
    all_settlementsp1 = all_settlements.groupby(by = ['name'])[['Cuml Energy Export Credits','Cuml Energy Charges']].shift(1)
    
    all_settlementsp1.rename(columns = {'Cuml Energy Export Credits':'Export Credits Cuml Shift',
                                         'Cuml Energy Charges':'Energy Charges Cuml Shift'},inplace = True)
    
    all_settlements = all_settlements.merge(all_settlementsp1, left_index = True, right_index=True, how = 'left')
    all_settlements.fillna(value = {'Export Credits Cuml Shift':0,
                                     'Energy Charges Cuml Shift':0,},inplace=True)

    all_settlements['Remaining Current Month Credits'] = (all_settlements['Export Credits Cuml Shift'] + all_settlements['Energy Charges Cuml Shift']).clip(upper = 0)
    all_settlements['Remaining Current Month Credits flipped'] = all_settlements['Remaining Current Month Credits'] * -1
    all_settlements['Credits Utilized Current Month'] = all_settlements[['Remaining Current Month Credits flipped','Total Energy Positive Charges']].min(axis = 1)
    all_settlements['Billables'] = all_settlements['Subscription'] + all_settlements['Credits Utilized Current Month'] + -1*all_settlements['net_load'].clip(upper = 0)  * all_settlements['NSCR']
    all_settlements['Billables'] = all_settlements['Billables'] * (1-all_settlements['discount_rate'])
    
    all_settlements['Month-Year'] = all_settlements['start'].dt.strftime('%b %Y')
    all_settlementsPlot = all_settlements.groupby(by = ['Month-Year','start']).agg({'Billables':'sum'}).reset_index()
    all_settlementsPlot.sort_values(by = ['start'],inplace = True)
    
    # Billables YTD Calc
    all_settlementsYTD = all_settlements.copy()    

    ## Adjust for modeling period
    all_settlementsYTD = all_settlementsYTD.loc[all_settlementsYTD['start'] <= report_end_datetime]

    ## Get Last NSCR rate set before the report end date
    nscr_rate_ytd = all_settlementsYTD['NSCR'].iloc[-1]

    all_settlementsYTD = all_settlementsYTD.groupby(by = ['name']).agg({'Gross Energy Charges':'sum', 'Net Energy Charges':'sum',
                                                                              'net_load':'sum','discount_rate':'max'}).reset_index()
    
    all_settlementsYTD['Net Energy Charges'] = all_settlementsYTD['Net Energy Charges'].clip(lower = 0)

    all_settlementsYTD['Total Subscriptions'] = all_settlementsYTD['Gross Energy Charges'] - all_settlementsYTD['Net Energy Charges']
    all_settlementsYTD['Net Surplus Value $'] = -1*all_settlementsYTD['net_load'].clip(upper = 0) * nscr_rate_ytd
    all_settlementsYTD['Billables'] = all_settlementsYTD['Total Subscriptions'] + all_settlementsYTD['Net Surplus Value $']

    ytd_billablessModeled_df = pd.DataFrame({'Month-Year':['YTD'],
                                             'Billables':[all_settlementsYTD['Billables'].sum()]}, index = [0])
    all_settlementsPlot = pd.concat([all_settlementsPlot, ytd_billablessModeled_df])

    # create summary dataframe grouped by different vnem_btm types
    monthly_modeled_plot = modeledSettlements.groupby(by = ['start','end','vnem_btm']).agg({'VOLC':'sum','total_peak_production':'sum','total_production':'sum'}).reset_index()
    monthly_modeled_plot['start'] = pd.to_datetime(monthly_modeled_plot['start'], format= '%m/%d/%y')
    monthly_modeled_plot['end'] = pd.to_datetime(monthly_modeled_plot['end'], format= '%m/%d/%y')
    monthly_modeled_plot['Month-Year'] = monthly_modeled_plot['start'].dt.strftime('%b %Y')

    # Summary for all vnem_btm types
    monthly_all_plot = monthly_modeled_plot.groupby(by = ['start','Month-Year']).agg({'VOLC':'sum','total_production':'sum'}).reset_index()
    monthly_all_plot['total_production_mwh'] = monthly_all_plot['total_production'] / 1000
    monthly_all_plot.sort_values(by = ['start'], inplace = True)

    ytd_modeledVOLC = monthly_all_plot.loc[monthly_all_plot['start'] <= report_end_datetime,'VOLC'].sum()
    ytd_modeledProduction = monthly_all_plot.loc[monthly_all_plot['start'] <= report_end_datetime,'total_production_mwh'].sum()
    ytd_modeled_df = pd.DataFrame({'start':['YTD'],
                                       'end':['YTD'],
                                       'VOLC':[ytd_modeledVOLC],
                                       'total_production_mwh':[ytd_modeledProduction],
                                       'Month-Year':['YTD']}, index = [0])
    
    modeledYTD_plot_df = pd.concat([monthly_all_plot,ytd_modeled_df])    

    # Create plotting df for actual VOLC (settled)
    actual_plot = all_settlements.groupby(by = ['start']).agg({'Gross Energy Charges':'sum','peak_production':'sum'}).reset_index()
    actual_plot['Month-Year'] = actual_plot['start'].dt.strftime('%b %Y')

    ytd_actual_volc = actual_plot['Gross Energy Charges'].sum()
    ytd_actual_volc_df = pd.DataFrame({'start':['YTD'],'Gross Energy Charges':[ytd_actual_volc],'peak_production':[0],'Month-Year':['YTD']})
    actual_plot = pd.concat([actual_plot, ytd_actual_volc_df])

    # Create plotting df for actual peak production (settled)
    peak_actual_plot = all_settlements.loc[all_settlements['vnem_btm'] == 'BTM-PVBESS'].groupby(by = ['start']).agg({'peak_production':'sum'}).reset_index()
    peak_actual_plot['Month'] = peak_actual_plot['start'].dt.month
    peak_actual_plot['Month-Year'] = peak_actual_plot['start'].dt.strftime('%b %Y')
    
    ytd_actual_peak = peak_actual_plot['peak_production'].sum()
    ytd_actual_df = pd.DataFrame({'start':['YTD'],'peak_production':[ytd_actual_peak],'Month-Year':['YTD']}, index = [0])
    peak_actual_plot = pd.concat([peak_actual_plot,ytd_actual_df])
    
    property_info = html.P(
        [
            community,
            html.Br(),
            addressInfo,
            html.Br(),
            community,
            html.Br(),
            "Unit Count: " + str(unitsInfo)
        ],
        style={
            "float":"left",
            "font-size":"14px",
            "padding-left":"10px",
            "text-align":"left",
            "font-style":"sans-serif",
            "margin-bottom":"0px",
        }
    ),

    table_mtd = dash_table.DataTable(
            data=commission_df_table.to_dict('records'),
            columns=[{'id': c, 'name': c} for c in commission_df_table.columns],
            style_cell={'textAlign': 'left'},
            style_cell_conditional=[
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Actual'},
                    'textAlign': 'center'
                },
                {
                    'if': {'column_id': 'Modeled'},
                    'textAlign': 'center'
                }                
            ],
            style_header={
                'backgroundColor': "#3f2aa5",
                'color': 'white',
                "font-family":"sans-serif",
                'fontSize':15
            },
            style_data={
                "font-family":"sans-serif",
                'fontSize':15
            },
        )

    table_ytd= dash_table.DataTable(
            data=commissionSummary_df.to_dict('records'),
            columns=[{'id': c, 'name': c} for c in commissionSummary_df.columns],
            style_cell={'textAlign': 'left'},
            style_cell_conditional=[
                {
                    'if': {'column_id': 'Commission Units Summary'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Actual'},
                    'textAlign': 'center'
                }
            ],
            style_header={
                'backgroundColor': "#3f2aa5",
                'color': 'white',
                "font-family":"sans-serif",
                'fontSize':15
            },
            style_data={
                "font-family":"sans-serif",
                'fontSize':15
            },
        )

    trace_1_1 = go.Scatter(x = commission_df['date'], y = commission_df['Occupancy % - Commissioned Units'], name = "Occupancy % - Commissioned Units", mode = "lines+markers",yaxis = "y", marker=dict(color='#5ee76c'))
    trace_1_2 = go.Scatter(x = commission_df['date'], y = commission_df['CARE %'], name = "CARE %", mode = "lines+markers",yaxis = "y",marker=dict(color='#433b96'))

    fig_1 = go.Figure({
            "data": [trace_1_1, trace_1_2],
            "layout": go.Layout(title = "<b>Historical Monthly Subscriber Profile<b>",
                                barmode = 'relative',
                        xaxis = dict(title = "<b>Month</b>", zerolinewidth = 2, zerolinecolor = 'white', linecolor = 'white'),
                        yaxis = dict(title = "<b>Percentage</b>", zerolinecolor = 'white', linecolor = 'white', showgrid = True,nticks=6,tickformat = ".1%"),
                        legend = dict(orientation = "h",yanchor="bottom",y=-0.35,xanchor="left",x=-0.05),
                        margin = dict(l=80,b=0,t=50,r=30),
                        height = 350
                               )})

    # Trace historical production
    prevYear_month_merge = prevYear_prod_group.merge(prod_group[['Month','Month-Year']], left_on = ['Month'], right_on = ['Month'], how = 'right')

    ytdPrevYearProduction = prevYear_prod_group['total_production_mwh'].sum()
    
    ytdPrevYearProduction_df = pd.DataFrame({'total_production_mwh':[ytdPrevYearProduction],
                                       'Month-Year':['YTD']}, index = [0])
    
    prevYear_month_merge = pd.concat([prevYear_month_merge,ytdPrevYearProduction_df])
    prevYear_month_merge.fillna(value = {'total_production_mwh':0},inplace = True)
    
    trace_2_1 = go.Bar(x = modeledYTD_plot_df['Month-Year'], y = modeledYTD_plot_df['total_production_mwh'], name = "Modeled Underwritten - Full Month",yaxis = "y", marker_color='#433b96', opacity=0.75, text = modeledYTD_plot_df['total_production_mwh'].astype(int), textposition='outside')
    trace_2_2 = go.Bar(x = modeledYTD_plot_df['Month-Year'], y = modeledYTD_plot_df['total_production_mwh'], name = "Modeled Budgeted - Full Month",yaxis = "y", marker_color='#433b96', opacity=0.75, text = modeledYTD_plot_df['total_production_mwh'].astype(int), textposition='outside')
    trace_2_3 = go.Bar(x = prevYear_month_merge['Month-Year'], y = prevYear_month_merge['total_production_mwh'], name = "Last Year - Full Month",yaxis = "y",marker_color='#41c5eb', opacity=0.75, text = prevYear_month_merge['total_production_mwh'].astype(int), textposition='outside')
    trace_2_4 = go.Bar(x = prod_group_plot['Month-Year'], y = prod_group_plot['total_production_mwh'], name = "Current Year",yaxis = "y",marker_color='#41c5eb', opacity=0.5, text = prod_group_plot['total_production_mwh'].astype(int), textposition='outside')
    
    max_y_fig2  = max([modeledYTD_plot_df['total_production_mwh'].max(),prevYear_month_merge['total_production_mwh'].max(),prod_group_plot['total_production_mwh'].max()]) * 1.25
    fig_2 = go.Figure({
            "data": [trace_2_1, trace_2_2, trace_2_3,trace_2_4],
            "layout": go.Layout(title = "<b>Monthly Aggregated Community Production - Operational Data<b>",
                                barmode = 'group',
                                xaxis = dict(title = "<b>Month</b>", zerolinewidth = 2, zerolinecolor = 'white', linecolor = 'white'),
                                yaxis = dict(title = "<b>Production (MWh)</b>", zerolinecolor = 'white', linecolor = 'white', showgrid = True,nticks=6, range = [0,max_y_fig2]),
                                legend = dict(orientation = "h",yanchor="bottom",y=-0.35,xanchor="left",x=-0.05),
                                margin = dict(l=80,b=0,t=50,r=30),
                                height = 350
                               )})

    # Trace per unit production variance
    prod_dist_list = []
    colors = ['#433b96', '#41c5eb', '#5ee76c']
    plot_count = 0
    for systemConfig in prod_dist['vnem_btm'].unique():

        trace_temp = go.Histogram(x = prod_dist.loc[prod_dist['vnem_btm'] == systemConfig,'total_production'].to_numpy(), name = systemConfig, marker_color = colors[plot_count], opacity=0.5)
        prod_dist_list.append(trace_temp)
        plot_count += 1

    fig_3 = go.Figure({
            "data": prod_dist_list,
            "layout": go.Layout(title = "<b>Production Distribution - Reporting Range - Operational Data<b>",
                                barmode='stack',
                                xaxis = dict(title = "<b>Production kWh</b>", zerolinewidth = 2, zerolinecolor = 'white', linecolor = 'white'),
                                yaxis = dict(title = "<b>Count</b>", zerolinecolor = 'white', linecolor = 'white', showgrid = True,nticks=6),
                                legend = dict(orientation = "h",yanchor="bottom",y=-0.35,xanchor="left",x=-0.05),
                                margin = dict(l=80,b=0,t=50,r=30),
                                height = 350
                               )})

    trace_4_1 = go.Bar(x = modeledPeakYTD_plot_df['Month-Year'],
                       y = modeledPeakYTD_plot_df['total_peak_production'], 
                       name = "Modeled Peak",
                       yaxis = "y1",
                       marker_color='#433b96', 
                       opacity = 0.5,
                       text = modeledPeakYTD_plot_df['total_peak_production'].astype(int),
                       textposition='outside')
    trace_4_2 = go.Bar(x = peak_actual_plot['Month-Year'], 
                       y = peak_actual_plot['peak_production'], 
                       name = "Actual Peak",
                       yaxis = "y1",
                       marker_color='#41c5eb',
                       opacity = 0.5,
                       text = peak_actual_plot['peak_production'].astype(int),
                       textposition='outside')
    
    max_y_fig4 = max([modeledPeakYTD_plot_df['total_peak_production'].max(),peak_actual_plot['peak_production'].max()]) * 1.25
    fig_4 = go.Figure({
            "data": [trace_4_1,trace_4_2],
            "layout": go.Layout(title = "<b>Peak Production Volumes BTM BESS<b>",
                                barmode = 'group',                                
                                xaxis = dict(title = "<b>Month</b>", zerolinewidth = 2, zerolinecolor = 'white', linecolor = 'white'),
                                yaxis = dict(title = '<b>Energy (kWh)<b>',zerolinecolor = 'white', linecolor = 'white', showgrid = False,tickformat = "{:,.2f}", range = [0, max_y_fig4]),
                                legend = dict(orientation = "h",yanchor="bottom",y=-0.35,xanchor="left",x=-0.05),
                                margin = dict(l=80,b=0,t=50,r=30),
                                height = 350,
                            )})

    trace_5_1 = go.Bar(x = modeledYTD_plot_df['Month-Year'], 
                       y = modeledYTD_plot_df['VOLC'], 
                       name = "Modeled VOLC",
                       yaxis = "y1",
                       marker_color='#433b96', 
                       opacity = 0.5, 
                       text = modeledYTD_plot_df['VOLC'].astype(int), 
                       textposition='outside')
    trace_5_2 = go.Bar(x = actual_plot['Month-Year'], 
                       y = actual_plot['Gross Energy Charges'], 
                       name = "Actual VOLC",
                       yaxis = "y1",
                       marker_color='#41c5eb', 
                       opacity = 0.5,
                       text = actual_plot['Gross Energy Charges'].astype(int), 
                       textposition='outside')
    max_y_fig5 = max([modeledYTD_plot_df['VOLC'].max(),actual_plot['Gross Energy Charges'].max()]) * 1.25
    fig_5 = go.Figure({
            "data": [trace_5_1,trace_5_2],
            "layout": go.Layout(title = "<b>Value of Load Consumed<b>",
                                barmode = 'group',                                
                                xaxis = dict(title = "<b>Month</b>", zerolinewidth = 2, zerolinecolor = 'white', linecolor = 'white'),
                                yaxis = dict(title = '<b>VOLC ($)<b>',zerolinecolor = 'white', linecolor = 'white', showgrid = False,tickformat = "$,", range = [0, max_y_fig5]),
                                legend = dict(orientation = "h",yanchor="bottom",y=-0.35,xanchor="left",x=-0.05),
                                margin = dict(l=80,b=0,t=50,r=30),
                                height = 350,
                            )})

    trace_6_1 = go.Bar(x = modeledBillablesPlot['Month-Year'], 
                       y = modeledBillablesPlot['Total Billables'], 
                       name = "Modeled Billables",
                       yaxis = "y1",
                       marker_color='#433b96', 
                       opacity = 0.5)
    trace_6_2 = go.Bar(x = all_settlementsPlot['Month-Year'], 
                       y = all_settlementsPlot['Billables'], 
                       name = "Actual Billables",
                       yaxis = "y1",
                       marker_color='#41c5eb', 
                       opacity = 0.5,
                       text = all_settlementsPlot['Billables'].astype(int), 
                       textposition='outside')
    
    max_y_fig6 = max([modeledBillablesPlot['Total Billables'].max(), all_settlementsPlot['Billables'].max()]) * 1.25

    fig_6 = go.Figure({
            "data": [trace_6_1,trace_6_2],
            "layout": go.Layout(title = "<b>Billables<b>",
                                barmode = 'group',                                
                                xaxis = dict(title = "<b>Month</b>", zerolinewidth = 2, zerolinecolor = 'white', linecolor = 'white'),
                                yaxis = dict(title = '<b>Billables ($)<b>',zerolinecolor = 'white', linecolor = 'white', showgrid = False,tickformat = "$,", range = [0, max_y_fig6]),
                                legend = dict(orientation = "h",yanchor="bottom",y=-0.35,xanchor="left",x=-0.05),
                                margin = dict(l=80,b=0,t=50,r=30),
                                height = 350,
                            )})

    return table_mtd,table_ytd, property_info, fig_1, fig_2, fig_3, fig_4, fig_5, fig_6


if __name__ == "__main__":
    app.run_server(debug = True)
