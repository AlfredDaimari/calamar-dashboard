"""
Database utils functions:
    Create:
    - create index table
    - create bank statement table
    - create index nav table
    - create trade_report table
    - create trade nav table

    Update:
    - update index table 
    - update bank statement table
    - update trade_report table
"""

import pandas as pd
import sqlite3
import os
import datetime
from calamar_backend.price import get_price as yf_get_price
from calamar_backend.maps import TickerMap


class Database:
    """
    Handles database utility functions
    """

    def __init__(self):
        self.db_name = os.getenv("CALAMAR_DB")
        self.__tm = TickerMap()

        if self.db_name != None:
            self.conn = sqlite3.connect(self.db_name)
        else:
            raise Exception(
                f"{str(datetime.datetime.now())}: environment variable 'CALAMAR_DB' not set"
            )

    def create_index_table(self, ticker: str, start: str, end: str) -> None:
        """
        :parameter ticker: zerodha ticker
        :parameter start: start date for query
        :parameter end: end date for query
        """
        yf_ticker = self.__tm.get(ticker)
        df: pd.DataFrame = yf_get_price(yf_ticker, start, end)
        df.to_sql(
            f"{ticker}_price",
            self.conn,
            index=True,
            if_exists="replace",
            index_label="Date",
        )

    def create_bank_statment_table(self) -> None:
        bank_statement_file = os.getenv("ZERODHA_BANK_STATEMENT")

        if bank_statement_file is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: environment variable 'ZERODHA_BANK_STATEMENT' not set"
            )
        bank_statment_df = pd.read_csv(bank_statement_file)

        # clean zerodha bank statement file

    def create_trade_report_table(self) -> None:
        trade_report_file = os.getenv("ZERODHA_TRADE_REPORT")

        if trade_report_file is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: environment variable 'ZERODHA_TRADE_REPORT' not set"
            )
        df = pd.read_csv(trade_report_file)

        # set date as index
        df["Trade Date"] = pd.to_datetime(df["Trade Date"])
        df = df.set_index("Trade Date")

        df.to_sql(
            "trade_report",
            self.conn,
            index=True,
            if_exists="replace",
            index_label="Trade Date",
        )

    def create_index_nav_table(self, ticker: str) -> None:
        pass

    def create_trade_nav_table(self) -> None:
        pass

    # Update utility functions
