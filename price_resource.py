import pandas as pd

def load_price(symbol, year):
    url = 'data/price/{}_{}.csv'.format(symbol, str(year))
    df = pd.read_csv(url, index_col=0, low_memory=False)
    return df

def load_price_to_2020(symbol):
    df = pd.DataFrame()
    for year in range(2015, 2021):
        try:
            tmp_df = load_price_ochlv(symbol, year)
            df = df.append(tmp_df, sort=True)
        except:
            pass

    return df

def load_price_ochlv(symbol, year):
    url = 'data/price_hlcov/{}_{}.csv'.format(symbol, str(year))
    df = pd.read_csv(url, index_col=0, low_memory=False)
    df['date_time'] = pd.to_datetime(df.time, unit='s')
    return df

def load_price_ochlv_to_2020(symbol):
    df = pd.DataFrame()
    for year in range(2015, 2021):
        try:
            tmp_df = load_price(symbol, year)
            df = df.append(tmp_df, sort=True)
        except:
            pass

    return df
