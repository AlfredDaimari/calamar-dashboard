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
import sqlite3
import os
import datetime
import typing
import tqdm

import calamar_backend.time as time
from calamar_backend.table_interface import (
    BankStatement as BNK,
    IndexNAV,
    TradeReport,
    Index,
    Portfolio,
)
from calamar_backend.table_row_interface import (
    IndexNAVRow,
    TradeReportRow,
)
from calamar_backend import errors


class Database:
    """
    Handles database utility functions
    """

    def __init__(self):
        self.db_name = os.getenv("CALAMAR_DB")

        if self.db_name is not None:
            self.conn = sqlite3.connect(self.db_name)
        else:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'CALAMAR_DB' not set"
            )

        # connect to various tables
        self.bnk_table = BNK()
        self.tr_table = TradeReport()
        self.pft_table = Portfolio()
        self.index_nav_table: typing.Optional[IndexNAV] = None
        self.index_table: typing.Optional[Index] = None

    def change_index_table(self, ticker: str, start="", end=""):
        self.index_table = Index(ticker, start, end)

    def change_index_nav_table(self, ticker: str):
        self.index_nav_table = IndexNAV(ticker)

    def create_index_table(self, ticker: str, start: str, end: str):
        """
        :parameter ticker: zerodha ticker
        :parameter start: start date for query
        :parameter end: end date for query
        """
        self.change_index_table(ticker, start, end)

        if self.index_table is not None:
            self.index_table.create_new_table(self.conn)

    def create_bank_statment_table(self) -> None:
        self.bnk_table.create_new_table(self.conn)

    def create_trade_report_table(self):
        self.tr_table.create_new_table(self.conn)

    def create_index_nav_table(self, ticker: str) -> None:
        """
        - Setup nav for index on day zero till today - 1
        - Iterate through each day, create index nav on every trading day
        """
        cur_date = time.get_current_date()
        self.change_index_nav_table(ticker)

        if self.index_nav_table is not None:
            # create new table
            self.index_nav_table.create_new_table(self.conn)
            self.index_nav_table.create_index(self.conn)

            date = self.bnk_table.get_day_zero_date(self.conn)
            date = time.convert_date_to_strf(date)

            row_index_nav = IndexNAVRow(
                date, self.index_nav_table.ticker, 0.0, 0.0, 0.0, 0.0, 0.0
            )

            # add day zero bank statements to nav
            self.__add_day_zero_bnk_statements_to_index_nav(row_index_nav)

            # add nav from day one to current date to the index nav
            start_date = row_index_nav.date + datetime.timedelta(days=1)
            self.__add_interval_bnk_statements_to_index_nav(
                start_date, cur_date, row_index_nav
            )

    def create_portfolio_table(self):
        """
        - Create portfolio report table
        - Write to portfolio report table
        - Remove problematic securities
        """
        cur_date = time.get_current_date()
        start_date = None

        # create portfolio table
        self.pft_table.create_new_table(self.conn)
        self.pft_table.create_index(self.conn)

        # day zero trades
        trades: list[TradeReportRow] = self.tr_table.get_day_zero(self.conn)
        start_date = trades[0].date

        for trade in trades:
            self.pft_table.add_to_portfolio(trade)

        # add day zero portfolio
        self.pft_table.insert_all(
            self.conn, time.convert_date_to_strf(start_date)
        )
        start_date += datetime.timedelta(1)

        # add trades to portfolio from day 1 to current date
        self.__add_interval_trades_to_portfolio(start_date, cur_date)

    def __add_day_zero_bnk_statements_to_index_nav(
        self, row_index_nav: IndexNAVRow
    ):
        """
        Add day zero bank statements as amount invested to index nav table
        row_index_nav :parameter: A dummy initialized row object which can be
        used to insert into the table
        """
        if self.index_nav_table is None:
            raise Exception(
                f"{datetime.datetime.now()}: " "index_nav_table not set"
            )

        else:
            # add day zero bank statements
            day_zero_bnk_statements = self.bnk_table.get_day_zero(self.conn)
            for bnk_st in day_zero_bnk_statements:
                row_index_nav.add_to_nav(bnk_st)

            # calculate day zero index nav
            row_index_nav.calculate_index_nav(self.conn)
            self.index_nav_table.insert(self.conn, row_index_nav)
            row_index_nav.reset()

    def __add_interval_bnk_statements_to_index_nav(
        self,
        start_date: datetime.datetime,
        last_date: datetime.datetime,
        row_index_nav: IndexNAVRow,
    ) -> None:
        """
        Add bank statements in a time interval to the index nav
        row_index_nav :parameter: The last inserted row in the index nav table
        before the parameter start date
        """

        ticker = (
            self.index_nav_table.ticker
            if self.index_nav_table is not None
            else None
        )

        # create progress bar
        with tqdm.tqdm(
            total=(last_date - start_date).days,
            desc=f"Writing daily nav to {ticker}_index_nav table",
            leave=False,
        ) as pbar:
            """
            - get bank statements for current day
            - add bank statments for calculating day_payin or day_payout
            - add day_payin or day_payout to amount_invested
            - calculate day nav
            - write to database
            - reset ticker_index_nav for next day
            """
            for day in time.range_date(start_date, last_date):
                bnk_statements = []

                try:
                    bnk_statements = self.bnk_table.get(self.conn, day)

                    # adding bank statements
                    for bnk_st in bnk_statements:
                        row_index_nav.add_to_nav(bnk_st)

                    # setting new date
                    row_index_nav.date = day
                    row_index_nav.calculate_index_nav(self.conn)

                    if self.index_nav_table is not None:
                        self.index_nav_table.insert(self.conn, row_index_nav)
                    else:
                        raise Exception(
                            f"{datetime.datetime.now()}: index nav "
                            "table not set"
                        )

                    # reset calculation for the next day
                    row_index_nav.reset()

                except errors.DayClosePriceNotFoundError:
                    if len(bnk_statements) != 0:
                        raise Exception(
                            "Error: bank statments exists, "
                            "but market was closed"
                        )
                    else:
                        continue

                except Exception as e: 
                    raise Exception(
                        f"{datetime.datetime.now()}:"
                        "db:create_index_nav_table: "
                        f"something went really wrong! - {e}"
                    )

                finally:
                    # pbar update
                    pbar.update(1)

    def __add_interval_trades_to_portfolio(
        self, start_date: datetime.datetime, last_date: datetime.datetime
    ):
        """
        Add trades in an interval to portfolio table
        """
        with tqdm.tqdm(
            total=(last_date - start_date).days,
            desc="creating portfolio report",
        ) as pbar:
            """
            - get trades on date
            - add trades to the portfolio table
            """
            for day in time.range_date(start_date, last_date):
                trades = self.tr_table.get(self.conn, day)

                for trade in trades:
                    self.pft_table.add_to_portfolio(trade)

                # write to table
                self.pft_table.insert_all(
                    self.conn, time.convert_date_to_strf(day)
                )
                pbar.update(1)
