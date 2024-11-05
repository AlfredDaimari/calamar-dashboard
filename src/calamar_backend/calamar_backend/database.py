"""
Database utils functions:
    Create:
    - create index table
    - create bank statement table
    - create index nav table
    - create trade_report table
    - create portfolio table
    - create portfolio nav table

    TODO:
    - create sharpe ratio table
    - create sortino ratio table
    - create omega ratio table
    - create calamar ratio table
    - create sharpe ratio optimization table (impacts over 6 months)
    - create omega ratio optimization table (impacts over 6 months)

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
    PortfolioNAV,
    TradeReport,
    Index,
    Portfolio,
)
from calamar_backend.table_row_interface import (
    IndexNAVRow,
    IndexRow,
    PortfolioNAVRow,
    PortfolioRow,
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
        self.pft_nav_table = PortfolioNAV()
        self.index_nav_table: typing.Optional[IndexNAV] = None
        self.index_table: typing.Optional[Index] = None

    def change_index_table(self, ticker: str, start="", end="") -> None:
        self.index_table = Index(ticker, start, end)

    def change_index_nav_table(self, ticker: str) -> None:
        self.index_nav_table = IndexNAV(ticker)

    def create_index_table(self, ticker: str, start: str) -> None:
        """
        :parameter ticker: zerodha ticker
        :parameter start: start date for query
        :parameter end: end date for query
        """
        end = time.get_current_date()
        end_str = time.convert_date_to_strf_yf(end)

        self.change_index_table(ticker, start, end_str)

        if self.index_table is not None:
            self.index_table.create_new_table(self.conn)

    def create_bank_statment_table(self) -> None:
        self.bnk_table.create_new_table(self.conn)

    def create_trade_report_table(self) -> None:
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

    def create_portfolio_table(self) -> None:
        """
        - Create portfolio report table
        - Write to portfolio report table using interval
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

    def create_portfolio_nav_table(self) -> None:
        """
        - Create portfolio nav table
        - Write to portfolio nav table using interval
        """

        cur_date = time.get_current_date()
        self.pft_nav_table.create_new_table(self.conn)
        self.pft_nav_table.create_index(self.conn)

        # day zero portfolio sec
        day_zero = self.pft_table.get_day_zero_date(self.conn)
        pft_secs: list[PortfolioRow] = self.pft_table.get(self.conn, day_zero)

        portfolio_nav_row = PortfolioNAVRow(
            time.convert_date_to_strf(day_zero), 0
        )

        # add to nav on day zero
        for sec in pft_secs:
            portfolio_nav_row.add_to_nav(sec)
        self.pft_nav_table.insert(self.conn, portfolio_nav_row)

        start_date = day_zero + datetime.timedelta(days=1)
        # add nav rows using interval
        self.__add_interval_nav_to_portfolio_nav(
            start_date, cur_date, portfolio_nav_row
        )

    def __add_day_zero_bnk_statements_to_index_nav(
        self, row_index_nav: IndexNAVRow
    ) -> None:
        """
        Add day zero bank statements as amount invested to index nav table
        row_index_nav :parameter: A dummy initialized row object which can be
        used to insert into the table
        """
        if self.index_nav_table is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: " "index_nav_table not set"
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
                            f"{str(datetime.datetime.now())}: index nav "
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
                        f"{str(datetime.datetime.now())}:"
                        "db:create_index_nav_table: "
                        f"something went really wrong! - {e}"
                    )

                finally:
                    # pbar update
                    pbar.update(1)

    def __add_interval_trades_to_portfolio(
        self, start_date: datetime.datetime, last_date: datetime.datetime
    ) -> None:
        """
        Add trades in an interval to portfolio table
        """
        with tqdm.tqdm(
            total=(last_date - start_date).days,
            desc="creating portfolio report",
            leave=False,
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
                # only write to table if prices exist on that day
                self.change_index_table("nifty50")
                if self.index_table is not None:
                    rows: list[IndexRow] = self.index_table.get(self.conn, day)

                    if len(rows) > 0:
                        self.pft_table.insert_all(
                            self.conn, time.convert_date_to_strf(day)
                        )

                    pbar.update(1)

    def __add_interval_nav_to_portfolio_nav(
        self,
        start_date: datetime.datetime,
        last_date: datetime.datetime,
        pft_nav_row: PortfolioNAVRow,
    ) -> None:
        """
        Calculate net asset value of portfolio in an interval
        """

        with tqdm.tqdm(
            total=(last_date - start_date).days,
            desc="create portfolio nav",
            leave=False,
        ) as pbar:
            """
            - get portfolio on date
            - add securities to the nav
            """
            for day in time.range_date(start_date, last_date):
                psecs: list[PortfolioRow] = self.pft_table.get(self.conn, day)

                # reset for the new day
                pft_nav_row.date = day
                pft_nav_row.nav = 0

                for sec in psecs:
                    pft_nav_row.add_to_nav(sec)

                if pft_nav_row.nav > 0:  # only add when nav has positive value
                    self.pft_nav_table.insert(self.conn, pft_nav_row)

                pbar.update(1)
