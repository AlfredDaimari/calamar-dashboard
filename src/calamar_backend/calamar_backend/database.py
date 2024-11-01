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
import typing
import tqdm

import calamar_backend.time as time
import calamar_backend.time as time
from calamar_backend.table_interface import (
    BankStatement as BNK,
    IndexNAV,
    TradeReport,
    Index,
    Portfolio,
)
from calamar_backend import errors
from calamar_backend.maps import TickerMap


class Database:
    """
    Handles database utility functions
    """

    def __init__(self):
        self.db_name = os.getenv("CALAMAR_DB")
        self.__tm = TickerMap()

        if self.db_name is not None:
            self.conn = sqlite3.connect(self.db_name)
        else:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'CALAMAR_DB' not set"
            )

    def create_index_table(self, ticker: str, start: str, end: str) -> None:
        """
        :parameter ticker: zerodha ticker
        :parameter start: start date for query
        :parameter end: end date for query
        """
        Index.set(ticker)
        Index.set_date(start, end)
        Index.create_new_table(self.conn)

    def create_bank_statment_table(self) -> None:
        BNK.create_new_table(self.conn)

    def create_trade_report_table(self) -> None:
        TradeReport.create_new_table(self.conn)

    def create_index_nav_table(self, ticker: str) -> None:
        """
        - Setup nav for index on day zero till today - 1
        - Iterate through each day, create index nav on every trading day
        """
        index_nav_table_last_date = time.get_current_date()

        # create new table
        IndexNAV.set(ticker)
        IndexNAV.create_new_table(self.conn)
        IndexNAV.create_index(self.conn)

        # add day zero bank statements
        day_zero_bnk_statements = BNK.get_day_zero(self.conn)
        date = time.convert_date_to_strf(day_zero_bnk_statements[-1].date)

        ticker_index_nav = IndexNAV(date, ticker, 0.0, 0.0)
        for bnk_st in day_zero_bnk_statements:
            ticker_index_nav.add_to_nav(bnk_st)

        # calculate day zero index nav
        ticker_index_nav.calculate_index_nav(self.conn)
        ticker_index_nav.insert(self.conn)
        ticker_index_nav.reset()

        # add every day from day zero to current date to the index nav
        st_date = ticker_index_nav.date + datetime.timedelta(days=1)
        print("\n")

        # create progress bar
        with tqdm.tqdm(
            total=(index_nav_table_last_date - st_date).days,
            desc=f"Writing daily nav to {ticker}_index_nav table",
            leave=True,
        ) as pbar:
            """
            - get bank statements for current day
            - add bank statments for calculating day_payin or day_payout
            - add day_payin or day_payout to amount_invested
            - calculate day nav
            - write to database
            - reset ticker_index_nav for next day
            """
            for day in time.range_date(st_date, index_nav_table_last_date):
                bnk_statements = []

                try:
                    bnk_statements = BNK.get(self.conn, day)

                    # adding bank statements
                    for bnk_st in bnk_statements:
                        ticker_index_nav.add_to_nav(bnk_st)

                    # setting new date
                    ticker_index_nav.date = day
                    ticker_index_nav.calculate_index_nav(self.conn)
                    ticker_index_nav.insert(self.conn)

                    # reset calculation for the next day
                    ticker_index_nav.reset()

                except errors.DayClosePriceNotFoundError:
                    if len(bnk_statements) != 0:
                        raise Exception(
                            "Error: bank statments exists, "
                            "but market was closed"
                        )
                    else:
                        continue

                except Exception as e:
                    print("i", day)
                    raise Exception(
                        f"{datetime.datetime.now()}:"
                        "db:create_index_nav_table: "
                        f"something went really wrong! - {e}"
                    )

                finally:
                    # pbar update
                    pbar.update(1)

    def create_portfolio_table(self) -> None:
        """
        - Create portfolio report table
        - Write to portfolio report table
        - Remove problematic securities
        """
        lst_date = time.get_current_date()
        st_date = None

        # create portfolio table
        Portfolio.create_new_table(self.conn)
        Portfolio.create_index(self.conn)

        # day zero trades
        trades: list[TradeReport] = TradeReport.get_day_zero(self.conn)
        st_date = trades[0].date
        for trade in trades:
            Portfolio.add_to_portfolio(trade)

        # add day zero portfolio
        Portfolio.insert_all(self.conn, time.convert_date_to_strf(st_date))

        # run from (day zero + 1) to today - 1
        st_date += datetime.timedelta(1)
        with tqdm.tqdm(
            total=(lst_date - st_date).days, desc="creating portfolio report"
        ) as pbar:
            """
            - get trades on date
            - add trades to the portfolio table
            - delete problematic securities from table
            """
            for day in time.range_date(st_date, lst_date):
                trades = TradeReport.get(self.conn, day)

                for trade in trades:
                    Portfolio.add_to_portfolio(trade)

                # write to table
                Portfolio.insert_all(self.conn, time.convert_date_to_strf(day))
                pbar.update(1)

        # reset to day one for clean portfolio report
        st_date -= datetime.timedelta(1)
        # drop problematic rows from portfolio report
