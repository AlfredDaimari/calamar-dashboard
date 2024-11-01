"""
Utility Table Classes
    - TradeReport: manage trading report table
    - BankStatement: zerodha bank statement table
    - Portfolio: manage portfolio report table
    - Index: index table for nse index
    - IndexNav: index nav table 
    - PortfolioNav: portfolio nav table 
"""

import datetime
import sqlite3
import pandas as pd
import typing
import os
import abc

import calamar_backend.time as time
import calamar_backend.maps as mp
import calamar_backend.errors as er
import calamar_backend.table_row_interface as inf_row

from calamar_backend.price import download_price as yf_get_price

mp_ = mp.TickerMap()


class Table(abc.ABC):
    _table = None

    @abc.abstractmethod
    def get_query(cls, date: datetime.datetime) -> str:
        """
        Query table by date
        Note: first column (position 0) should contain date string
        """
        raise NotImplementedError

    @abc.abstractmethod
    def create_table_rows(cls, row):
        """
        Returns:
            cls: an object of the table class
        """
        raise NotImplementedError

    def create_index(self, conn: sqlite3.Connection) -> None:
        cursor = conn.cursor()
        cursor.execute(f"CREATE INDEX {self._table}_idx ON {self._table}(Date)")

    def insert(self, conn: sqlite3.Connection, row: inf_row.Row) -> None:
        cursor = conn.cursor()
        if self._table is not None:
            cursor.execute(row.insert_query(self._table))
        else:
            raise Exception(f"{datetime.datetime.now()}:table not set")
        conn.commit()

    def insert_mul(
        self, conn: sqlite3.Connection, table_rows: list[inf_row.Row]
    ) -> None:
        """
        Insert multiple rows at once
        table_rows :parameter: a list containing objects of Table subclasses
        """
        cursor = conn.cursor()
        for row in table_rows:
            if self._table is not None:
                cursor.execute(row.insert_query(self._table))
            else:
                raise Exception(f"{datetime.datetime.now()}: table not set")
        conn.commit()

    @abc.abstractmethod
    def _create_table(cls, conn: sqlite3.Connection) -> None:
        raise NotImplementedError

    def _delete_table(self, conn: sqlite3.Connection) -> None:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {self._table}")
        conn.commit()

    def create_new_table(self, conn: sqlite3.Connection) -> None:
        self._delete_table(conn)
        self._create_table(conn)

    def get_day_zero_query(self) -> str:
        return f"SELECT * FROM {self._table} LIMIT 1"

    def get_day_zero(self, conn: sqlite3.Connection):
        """
        Get all rows on day zero

        Returns:
            list[cls]
        """
        cursor = conn.cursor()
        cursor.execute(self.get_day_zero_query())

        row_zero: tuple[str, ...] = cursor.fetchall()[0]
        day_zero_str = row_zero[0]
        day_zero = time.convert_date_strf_to_strp(day_zero_str)

        cursor.execute(self.get_query(day_zero))
        rows: list[tuple[str, ...]] = cursor.fetchall()
        return list(map(self.create_table_rows, rows))

    def get(self, conn: sqlite3.Connection, date: datetime.datetime):
        """
        Returns:
            list[cls]: returns a list of objects that represent the cls table
            row
        """
        cursor = conn.cursor()
        cursor.execute(self.get_query(date))
        rows: list[tuple[str, ...]] = cursor.fetchall()
        return list(map(self.create_table_rows, rows))


class BankStatement(Table):
    def __init__(self):
        file = os.getenv("ZERODHA_BANK_STATEMENT")

        if file is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'ZERODHA_BANK_STATEMENT' not set"
            )

        # set bank statement file
        self.bank_statement_file: str = file
        self._table = "bank_statement"

    def create_table_rows(
        self, row: typing.Tuple[str, str, str, float, float]
    ) -> inf_row.BankStatementRow:
        return inf_row.BankStatementRow(*row)

    def _create_table(self, conn: sqlite3.Connection) -> None:
        df = pd.read_csv(self.bank_statement_file)
        df = df.dropna()
        clean_df = self.__clean_zerodha_bank_statement_file(df)

        # set posting_date as index
        clean_df["posting_date"] = pd.to_datetime(clean_df["posting_date"])
        clean_df = clean_df.rename(columns={"posting_date": "Date"})
        clean_df = clean_df.set_index("Date")
        clean_df = clean_df.sort_values(by="Date")

        clean_df.to_sql(
            self._table,
            conn,
            index=True,
            if_exists="replace",
            index_label="Date",
        )

    def get_query(self, date: datetime.datetime) -> str:
        date_str = time.convert_date_to_strf(date)
        return (
            "SELECT Date, particulars, cost_center, debit, credit "
            f"FROM {self._table} WHERE Date = '{date_str}'"
        )

    def __clean_zerodha_bank_statement_file(
        self, df: pd.DataFrame
    ) -> pd.DataFrame:
        df["ind_txn"] = df.apply(
            inf_row.BankStatementRow.is_valid_bank_statement, axis=1
        )
        # deal with error code later
        df = df[df["ind_txn"]]
        df = df.drop("ind_txn", axis=1)

        return df


class TradeReport(Table):
    def __init__(self):
        file = os.getenv("ZERODHA_TRADE_REPORT")
        if file is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'ZERODHA_TRADE_REPORT' not set"
            )

        self.trade_report_file: str = file
        self._table = "trade_report"

    def get_query(self, date: datetime.datetime) -> str:
        return (
            'SELECT Date, Symbol, ISIN, "Trade Type", '
            f"quantity FROM {self._table} WHERE Date = "
            f"""'{time.convert_date_to_strf(date)}'"""
        )

    def create_table_rows(
        self, row: typing.Tuple[str, str, str, str, int]
    ) -> inf_row.TradeReportRow:
        return inf_row.TradeReportRow(*row)

    def _create_table(self, conn: sqlite3.Connection) -> None:
        """
        Inserts data in file $ZERODHA_TRADE_REPORT into trade report table
        """
        df = pd.read_csv(self.trade_report_file)
        df = df.dropna()

        # set date as index
        df["Trade Date"] = pd.to_datetime(df["Trade Date"])
        df = df.rename(columns={"Trade Date": "Date"})
        df = df.set_index("Date")
        df = df.sort_values(by="Date")

        df.to_sql(
            self._table,
            conn,
            index=True,
            if_exists="replace",
            index_label="Date",
        )


class Index(Table):
    """
    Holds historic prices for an NSE index
    """

    def __init__(self, ticker: str, start: str = "", end: str = ""):
        """
        start, end needs to be set only when you want to create a table
        """
        self.yf_ticker = mp_.get(ticker)
        self.start = start
        self.end = end
        self._table = f"{ticker}_price"

    def create_table_rows(
        self, row: typing.Tuple[str, float]
    ) -> inf_row.IndexRow:
        return inf_row.IndexRow(*row)

    def get_query(self, date: datetime.datetime) -> str:
        return (
            f"SELECT Date, Close FROM {self._table} WHERE Date="
            f"'{time.convert_date_to_strf(date)}'"
        )

    def _create_table(self, conn: sqlite3.Connection) -> None:
        if (self.start == "") or (self.end == ""):
            raise Exception(
                f"{datetime.datetime.now()}:Index._create_table: start, end not set"
            )

        df: pd.DataFrame = yf_get_price(self.yf_ticker, self.start, self.end)
        df.to_sql(
            self._table,
            conn,
            index=True,
            if_exists="replace",
            index_label="Date",
        )


class IndexNAV(Table):
    def __init__(self, ticker: str):
        self.ticker = ticker
        self._table = f"{ticker}_index_nav"

    def _create_table(self, conn: sqlite3.Connection) -> None:
        """
        Table structure:
        Date: datetime
        ticker: string
        day_payin: float
        day_payout: float
        amount_invested: float
        units: float
        nav: float
        """
        query = (
            f"""CREATE TABLE {self._table} ("Date" DATE, "ticker" TEXT,"""
            f'"day_payin" REAL, "day_payout" REAL, "amount_invested" REAL,'
            f'"units" REAL, "nav" REAL)'
        )

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

    def create_table_rows(
        self, row: typing.Tuple[str, str, float, float, float, float, float]
    ) -> inf_row.IndexNAVRow:
        return inf_row.IndexNAVRow(*row)

    def get_query(self, date: datetime.datetime) -> str:
        return (
            f"SELECT * FROM {self._table} WHERE Date = '"
            f"{time.convert_date_to_strf(date)}'"
        )


class Portfolio(Table):
    def __init__(self):
        self._table = "portfolio_report"
        self.portfolio: typing.Dict[str, inf_row.TradeReportRow] = {}

    def _create_table(self, conn: sqlite3.Connection) -> None:
        cursor = conn.cursor()
        cursor.execute(
            f"""CREATE TABLE {self._table} """
            '("Date" DATE, "ticker" TEXT, "isin" TEXT,"quantity" REAL)'
        )
        conn.commit()

    def get_query(self, date: datetime.datetime) -> str:
        return (
            f"SELECT * FROM {self._table} "
            f"WHERE Date = '{time.convert_date_to_strf(date)}'"
        )

    def create_table_rows(
        self, row: typing.Tuple[str, str, str, float]
    ) -> inf_row.PortfolioRow:
        return inf_row.PortfolioRow(*row)

    def add_to_portfolio(self, trade: inf_row.TradeReportRow) -> None:
        if trade.ticker in self.portfolio:
            if trade.is_buy:
                self.portfolio[trade.ticker].quantity += trade.quantity
            else:
                self.portfolio[trade.ticker].quantity -= trade.quantity
        else:
            trade.quantity = trade.quantity if trade.is_buy else -trade.quantity
            self.portfolio[trade.ticker] = trade

    def remove_ne_quantity(self) -> None:
        """
        Removes negative quantities from portfolio
        """
        neg_tickers = []
        for ticker in self.portfolio:
            if self.portfolio[ticker].quantity <= 0:
                neg_tickers.append(ticker)

        for ticker in neg_tickers:
            self.portfolio.pop(ticker)

    def insert_all(self, conn: sqlite3.Connection, date: str) -> None:
        """
        Inserts all securities in the cls.portfolio
        """
        self.remove_ne_quantity()
        table_rows = []

        for key in self.portfolio.keys():
            pf = self.portfolio[key]
            portfolio_sec = inf_row.PortfolioRow(
                date, pf.ticker, pf.isin, pf.quantity
            )
            table_rows.append(portfolio_sec)

        self.insert_mul(conn, table_rows)


class PortfolioNAV(Table):
    pass
