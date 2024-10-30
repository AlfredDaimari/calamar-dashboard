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
from calamar_backend import errors
from calamar_backend.price import download_price as yf_get_price
from calamar_backend.maps import TickerMap
from calamar_backend.interface import BankStatement, IndexNav, SecurityTrade
from calamar_backend.interface import TradeNav, Time


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
                f"{str(datetime.datetime.now())}: "
                "environment variable 'ZERODHA_BANK_STATEMENT' not set"
            )

        bank_statement_df = pd.read_csv(bank_statement_file)

        # clean zerodha bank statement file
        bank_statement_df = bank_statement_df.dropna()
        cln_df = self.clean_zerodha_bank_statement_file(bank_statement_df)

        # set posting_date as index
        cln_df["posting_date"] = pd.to_datetime(cln_df["posting_date"])
        cln_df = cln_df.set_index("posting_date")
        cln_df = cln_df.sort_values(by="posting_date")

        cln_df.to_sql(
            "bank_statement",
            self.conn,
            index=True,
            if_exists="replace",
            index_label="posting_date",
        )

    def create_trade_report_table(self) -> None:
        """
        Inserts data in file $ZERODHA_TRADE_REPORT into trade report table
        """
        trade_report_file = os.getenv("ZERODHA_TRADE_REPORT")

        if trade_report_file is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'ZERODHA_TRADE_REPORT' not set"
            )
        df = pd.read_csv(trade_report_file)
        df = df.dropna()

        # set date as index
        df["Trade Date"] = pd.to_datetime(df["Trade Date"])
        df = df.set_index("Trade Date")
        df = df.sort_values(by="Trade Date")

        df.to_sql(
            "trade_report",
            self.conn,
            index=True,
            if_exists="replace",
            index_label="Trade Date",
        )

    def create_index_nav_table(self, ticker: str) -> None:
        """
        - Setup nav for index on day zero till today - 1
        - Iterate through each day, create index nav on every trading day
        """
        index_nav_table_last_date = Time.get_current_date()

        # create index nav table
        cursor = self.conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {ticker}_index_nav")
        cursor.execute(IndexNav.create_table_query(ticker))
        cursor.execute(
            f"""CREATE INDEX idx_date ON {ticker}_index_nav("Date")"""
        )

        day_zero_bnk_statements = self.get_day_zero_bank_statements()
        ticker_index_nav = IndexNav(
            day_zero_bnk_statements[-1].date, ticker, 0.0, 0.0
        )

        for bnk_st in day_zero_bnk_statements:
            ticker_index_nav.add_to_nav(bnk_st)

        # calculate day zero index nav
        ticker_index_nav.calculate_index_nav(self.conn)
        cursor.execute(ticker_index_nav.insert_table_query())
        self.conn.commit()
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
            for day in Time.range_date(st_date, index_nav_table_last_date):
                bnk_statements = []

                try:
                    bnk_statements = self.get_bank_statements(day)
                    # adding bank statements
                    for bnk_st in bnk_statements:
                        ticker_index_nav.add_to_nav(bnk_st)

                    # setting new date
                    ticker_index_nav.date = day
                    ticker_index_nav.calculate_index_nav(self.conn)
                    query = ticker_index_nav.insert_table_query()
                    cursor.execute(query)
                    self.conn.commit()

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

    def create_portfolio_nav_table(self) -> TradeNav:
        """
        - Create unclean temporary table
        - Calculate the error trades
        - Calculate the clean trade table
        - Remove the unclean table
        - Using clean table, calculate the net asset value
        """
        # create table, unclean and clean
        cursor = self.conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS unc_portfolio_report")
        cursor.execute(f"DROP TABLE IF EXISTS cln_portfolio_report")
        cursor.execute(SecurityTrade.create_table_query(False))
        cursor.execute(SecurityTrade.create_table_query(True))
        cursor.execute(
            f"""CREATE INDEX idx_date_unc ON unc_portfolio_report("Date")"""
        )
        cursor.execute(
            f"""CREATE INDEX idx_date_cln ON cln_portfolio_report("Date")"""
        )
        self.conn.commit()

        # day zero trades
        trades = self.get_day_zero_trades()
        trade_nav_unc = TradeNav(trades[0].date)
        for trade in trades:
            trade_nav_unc.add_to_portfolio(trade)

        # day zero portfolio
        # run from day zero to today - 1
        # move correct data to cln_portfolio_report
        # using cln_portfolio_report, calculate nav
        return trade_nav_unc

    # pandas utility functions
    def clean_zerodha_bank_statement_file(
        self, df: pd.DataFrame
    ) -> pd.DataFrame:
        df["ind_txn"] = df.apply(BankStatement.is_bank_statement, axis=1)
        # deal with error code later
        df = df[df["ind_txn"]]
        df = df.drop("ind_txn", axis=1)

        return df

    # database utility functions
    def __get_day_zero_bnk_state(self) -> str:
        """
        Returns day zero for bank settlements as a string
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM bank_statement LIMIT 1")
        rows = cursor.fetchall()
        return BankStatement(*rows[0]).get_date_strf()

    def __get_day_zero_trade_report(self) -> str:
        """
        Returns day zero for trading as a string
        """
        cursor = self.conn.cursor()
        cursor.execute(SecurityTrade.day_zero_query())
        rows = cursor.fetchall()
        return SecurityTrade(*rows[0]).get_date_strf()

    def get_bank_statements(
        self, date: datetime.datetime
    ) -> list[BankStatement]:
        """
        Get bank statements on :parameter date
        """
        cursor = self.conn.cursor()
        cursor.execute(BankStatement.get_bnk_statement_query(date))
        rows = cursor.fetchall()
        return list(map(BankStatement.create_bnk_statement, rows))

    def get_day_zero_bank_statements(self) -> list[BankStatement]:
        day_zero = self.__get_day_zero_bnk_state()

        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT * FROM bank_statement WHERE posting_date ='{day_zero}'",
        )
        rows = cursor.fetchall()

        return list(map(BankStatement.create_bnk_statement, rows))

    def get_day_zero_trades(self) -> list[SecurityTrade]:
        day_zero = self.__get_day_zero_trade_report()
        cursor = self.conn.cursor()
        cursor.execute(
            SecurityTrade.get_query(
                datetime.datetime.strptime(day_zero, Time.DATE_FORMAT)
            )
        )
        rows = cursor.fetchall()

        return list(map(SecurityTrade.create_security_trade, rows))
