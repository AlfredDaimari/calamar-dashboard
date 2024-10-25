# file that consists of getting price functions using the yahoo finance api
import yfinance as yf
import pandas as pd
import datetime


def get_price(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    :param ticker: security ticker (must be present in yahoo finance)
    :param csv_file: file to save the price information
    :param start: starting date
    :param end: ending date
    """
    print(f"{str(datetime.datetime.now())}: downloading {ticker} from yahoo finance")
    df: pd.DataFrame = yf.download(ticker, start=start, end=end)

    # reset multi-level index
    df = df.reset_index()
    df.columns = df.columns.get_level_values(0)
    df = df.set_index("Date")
    return df
