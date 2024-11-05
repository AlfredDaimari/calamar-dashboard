# file that consists of getting price functions using the yahoo finance api
import logging

import yfinance as yf
import pandas as pd
import datetime
import calamar_backend.time as time

# disable logging in yahoo finance
logger = logging.getLogger("yfinance")
logger.disabled = True
logger.propagate = False


def download_price(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    :param ticker: security ticker (must be present in yahoo finance)
    :param csv_file: file to save the price information
    :param start: starting date (YYYY-MM-DD)
    :param end: ending date
    """
    print(
        f"{str(datetime.datetime.now())}: "
        f"downloading {ticker} from yahoo finance"
    )

    # set minimum date
    cur_date = time.get_current_date()
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d")
    final_date = min(cur_date, end_date)
    end = final_date.strftime("%Y-%m-%d")

    df: pd.DataFrame = yf.download(ticker, start, end, progress=False)

    if len(df) == 0:
        print(
            f"{str(datetime.datetime.now())}: "
            f"{ticker} download from yahoo finance failed"
        )
        return df

    # reset multi-level index
    df = df.reset_index()
    df.columns = df.columns.get_level_values(0)

    # create new dates
    df["new_date"] = df.apply(time.convert_yf_date_to_strf, axis=1)
    df = df.drop("Date", axis=1)
    df["Date"] = df["new_date"]
    df = df.drop("new_date", axis=1)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date")

    return df
