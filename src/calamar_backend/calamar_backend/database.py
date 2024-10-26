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
from calamar_backend.price import get_price as yf_get_price
from calamar_backend.maps import TickerMap
from calamar_backend.interface import BankStatement, IndexNav, TradeNav, Time


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
        Inserts data in file $ZERODHA_TRADE_REPORT (env var) into trade report table
        """
        trade_report_file = os.getenv("ZERODHA_TRADE_REPORT")

        if trade_report_file is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: environment variable 'ZERODHA_TRADE_REPORT' not set"
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

    def create_index_nav_table(self, ticker: str):
        """
        - Setup nav for index on day zero till today - 1
        - Iterate through each day, on every day price exists for index, append index nav
        """
        index_nav_table_last_date = Time.get_current_date()

        # create index nav table

        day_zero_bnk_statements = self.get_day_zero_bank_statements()
        ticker_index_nav = IndexNav(day_zero_bnk_statements[-1].date, ticker, 0.0, 0.0)

        for bnk_st in day_zero_bnk_statements:
            ticker_index_nav.add_to_nav(bnk_st)

        # calculate day zero index nav
        ticker_index_nav.calculate_index_nav(self.conn)
        return ticker_index_nav

    def __insert_index_nav_table(self, ind_nav: IndexNav) -> None:
        pass

    def create_trade_nav_table(self) -> None:
        pass

    def __insert_trade_nav_table(self, trade_nav: TradeNav) -> None:
        pass

    # pandas utility functions
    def clean_zerodha_bank_statement_file(self, df: pd.DataFrame) -> pd.DataFrame:
        df["ind_txn"] = df.apply(self.__is_bank_settlement, axis=1)
        # deal with error code later
        df = df[df["ind_txn"]]
        df = df.drop("ind_txn", axis=1)

        return df

    def __is_bank_settlement(self, row: pd.Series | dict) -> bool:
        """
        Row or pd.Series structure
        {
            particulars:,
            posting_date:,
            cost_center:,
            voucher_type:,
            debit:,
            credit:,
            net_balance
        }
        """

        bank_txn_1 = "Bank Payments"
        bank_txn_2 = "Bank Receipts"
        debit_cost_center_keyword = "STARMF - Z"

        if bank_txn_1 in row["voucher_type"] or bank_txn_2 in row["voucher_type"]:
            return True

        if debit_cost_center_keyword in row["cost_center"]:
            return True

        return False

    # database utility functions
    def __get_day_zero_bnk_state(self) -> str:
        """
        Returns day zero for bank settlements as string
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM bank_statement LIMIT 1")
        rows = cursor.fetchall()
        return BankStatement.create_bnk_statement(rows[0]).get_date_strf()

    def get_bank_statements(self, date: str) -> list[BankStatement]:
        return []

    def get_day_zero_bank_statements(self) -> list[BankStatement]:
        day_zero = self.__get_day_zero_bnk_state()

        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT * FROM bank_statement WHERE posting_date ='{day_zero}'",
        )
        rows = cursor.fetchall()

        return list(map(BankStatement.create_bnk_statement, rows))
