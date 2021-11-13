import pandas as pd
import requests
from common.bq_upload import df_to_bqupload

# requests variables
base_url = 'https://api.stocktwits.com/api/2/'
trending_url = 'trending/symbols.json'
params = {}

# bq variables
project = 'stock-data-331621'
table_name = 'sentiment.stocktwits_trending'


def run_function(request):

    try:
        r = requests.get(base_url + trending_url, params=params)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
    else:
        response_content = r.json()['symbols']

        df = pd.DataFrame(response_content)

        # remove unwanted keys
        df.drop(['id', 'aliases', 'is_following', 'has_pricing'], axis=1, inplace=True)
        # rename columns
        df.rename(columns={'symbol': 'ticker', 'title': 'company_name'}, inplace=True)
        # add ticker rank
        df.insert(2, 'ticker_rank', df.index + 1)
        # insert timestamp
        df.insert(0, 'timestamp',  pd.Timestamp.utcnow())
        # load to bigquery
        df_to_bqupload(project=project, table_name=table_name, df=df)

        return f"Load completed at {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"
