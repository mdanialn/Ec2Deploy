## EIA API
import pandas as pd
import requests
import json


def getEIACoolingDays(start_date = '2023-01-01', end_date = '2023-01-31'):

    with open('creds_eia.json') as f:
        data = json.load(f)

    api_key = data['api_key']

    url = f'https://api.eia.gov/v2/steo/data/?frequency=monthly&data[0]=value&start={start_date}&end={end_date}&sort[0][column]=period&sort[0][direction]=desc&sort[1][column]=seriesId&sort[1][direction]=asc&offset=0&length=5000&facets[seriesId][]=ZWCD_PAC&facets[seriesId][]=ZWCD_PAC_10YR&api_key={api_key}'

    response = requests.get(url)

    if response.status_code == 200:

        response_data = response.json()
        out_df = pd.DataFrame(response_data['response']['data']).sort_values(by = ['period']).head(40)

        return out_df


if __name__ == '__main__':
    print(getEIACoolingDays())