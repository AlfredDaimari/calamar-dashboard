"""
Utility Table Classes
    - TradeReport: manage trading report
    - BankStatement: Zerodha bank statement representation
    - Portfolio: manage portfolio report
    - Index: index table for nse index
    - IndexNav: index nav on a trading day
    - PortfolioNav: portfolio nav on a trading day
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
from calamar_backend.price import download_price as yf_get_price

"""
Utility functions
"""

mp_ = mp.TickerMap()


class Table(abc.ABC):
    _table = None

    @classmethod
    @abc.abstractmethod
    def get_query(cls, date: datetime.datetime) -> str:
        """
        Query table by date
        Note: first column (position 0) should contain date string
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def create_table_rows(cls, row):
        """
        Returns:
            cls: an object of the table class
        """
        raise NotImplementedError

    @abc.abstractmethod
    def insert_query(self) -> str:
        raise NotImplementedError

    @classmethod
    def create_index(cls, conn: sqlite3.Connection) -> None:
        cursor = conn.cursor()
        cursor.execute(f"CREATE INDEX {cls._table}_idx ON {cls._table}(Date)")

    def insert(self, conn: sqlite3.Connection) -> None:
        cursor = conn.cursor()
        cursor.execute(self.insert_query())
        conn.commit()

    @classmethod
    def insert_mul(cls, conn: sqlite3.Connection, table_rows) -> None:
        """
        Insert multiple rows at once
        table_rows :parameter: a list containing objects of Table subclasses
        """
        cursor = conn.cursor()
        for row in table_rows:
            cursor.execute(row.insert_query())
        conn.commit()

    @classmethod
    @abc.abstractmethod
    def _create_table(cls, conn: sqlite3.Connection) -> None:
        raise NotImplementedError

    @classmethod
    def _delete_table(cls, conn: sqlite3.Connection) -> None:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {cls._table}")
        conn.commit()

    @classmethod
    def create_new_table(cls, conn: sqlite3.Connection) -> None:
        cls._delete_table(conn)
        cls._create_table(conn)

    @classmethod
    def get_day_zero_query(cls) -> str:
        return f"SELECT * FROM {cls._table} LIMIT 1"

    @classmethod
    def get_day_zero(cls, conn: sqlite3.Connection):
        """
        Get all rows on day zero

        Returns:
            list[cls]
        """
        cursor = conn.cursor()
        cursor.execute(cls.get_day_zero_query())

        row_zero: tuple[str, ...] = cursor.fetchall()[0]
        day_zero_str = row_zero[0]
        day_zero = time.convert_date_strf_to_strp(day_zero_str)

        cursor.execute(cls.get_query(day_zero))
        rows: list[tuple[str, ...]] = cursor.fetchall()
        return list(map(cls.create_table_rows, rows))

    @classmethod
    def get(cls, conn: sqlite3.Connection, date: datetime.datetime):
        """
        Returns:
            list[cls]: returns a list of objects that represent the cls table
            row
        """
        cursor = conn.cursor()
        cursor.execute(cls.get_query(date))
        rows: list[tuple[str, ...]] = cursor.fetchall()
        return list(map(cls.create_table_rows, rows))


class BankStatement(Table):
    bank_statement_file = None
    _table: str = "bank_statement"

    def __init__(
        self,
        date: str,
        particulars: str,
        cost_center: str,
        debit: float,
        credit: float,
    ):
        self.date = time.convert_date_strf_to_strp(date)
        self.particulars: str = particulars
        self.cost_center: str = cost_center
        self.debit = debit
        self.credit = credit

    def insert_query(self) -> str:
        # implement later
        return ""

    @classmethod
    def create_table_rows(cls, row: typing.Tuple[str, str, str, float, float]):
        return cls(*row)

    @classmethod
    def _create_table(cls, conn: sqlite3.Connection) -> None:
        # get bank statement file
        cls.bank_statement_file = os.getenv("ZERODHA_BANK_STATEMENT")

        if cls.bank_statement_file is None:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'ZERODHA_BANK_STATEMENT' not set"
            )

        df = pd.read_csv(cls.bank_statement_file)
        df = df.dropna()
        clean_df = cls.__clean_zerodha_bank_statement_file(df)

        # set posting_date as index
        clean_df["posting_date"] = pd.to_datetime(clean_df["posting_date"])
        clean_df = clean_df.rename(columns={"posting_date": "Date"})
        clean_df = clean_df.set_index("Date")
        clean_df = clean_df.sort_values(by="Date")

        clean_df.to_sql(
            "bank_statement",
            conn,
            index=True,
            if_exists="replace",
            index_label="Date",
        )

    @classmethod
    def get_query(cls, date: datetime.datetime) -> str:
        date_str = time.convert_date_to_strf(date)
        return (
            "SELECT Date, particulars, cost_center, debit, credit "
            f"FROM bank_statement WHERE Date = '{date_str}'"
        )

    def __str__(self) -> str:
        return (
            f"(Date:{self.date} par:{self.particulars} "
            f"cost_center{self.cost_center} debit:{-self.debit})"
        )

    @classmethod
    def __clean_zerodha_bank_statement_file(
        cls, df: pd.DataFrame
    ) -> pd.DataFrame:
        df["ind_txn"] = df.apply(cls.__is_valid_bank_statement, axis=1)
        # deal with error code later
        df = df[df["ind_txn"]]
        df = df.drop("ind_txn", axis=1)

        return df

    def is_credit_debit(self) -> tuple[bool, float]:
        """
        Returns:
                (True, credit_amount:float) for credit
                (False, debit_amount:float) for debit
        """
        credit_keyword = "Funds added using"
        if credit_keyword in self.particulars:
            assert self.credit > 0
            return (True, self.credit)
        else:
            assert self.debit > 0
            return (False, self.debit)

    @classmethod
    def __is_valid_bank_statement(cls, row: pd.Series) -> bool:
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

        if (
            bank_txn_1 in row["voucher_type"]
            or bank_txn_2 in row["voucher_type"]
        ):
            return True

        if debit_cost_center_keyword in row["cost_center"]:
            return True

        return False


class TradeReport(Table):
    _table = "trade_report"

    def __init__(
        self, date: str, symbol: str, isin: str, type_: str, quantity: int
    ):
        self.date = time.convert_date_strf_to_strp(date)
        self.ticker = symbol
        self.isin = isin
        self.is_buy: bool = True if type_ == "buy" else False
        self.quantity = quantity

    @classmethod
    def get_query(cls, date: datetime.datetime) -> str:
        return (
            'SELECT Date, Symbol, ISIN, "Trade Type", '
            "quantity FROM trade_report WHERE Date = "
            f"""'{time.convert_date_to_strf(date)}'"""
        )

    @classmethod
    def create_table_rows(cls, row: typing.Tuple[str, str, str, str, int]):
        return cls(*row)

    @classmethod
    def _create_table(cls, conn: sqlite3.Connection) -> None:
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
        df = df.rename(columns={"Trade Date": "Date"})
        df = df.set_index("Date")
        df = df.sort_values(by="Date")

        df.to_sql(
            "trade_report",
            conn,
            index=True,
            if_exists="replace",
            index_label="Date",
        )

    def insert_query(self) -> str:
        # implement later
        return ""

    def __str__(self) -> str:
        return f"(ticker: {self.ticker} q:{self.quantity} buy:{self.is_buy})"


class Index(Table):
    """
    Holds historic prices for an NSE index
    """

    start = ""
    end = ""
    yf_ticker = ""

    def __init__(self, date: str, close: float):
        self.date = time.convert_date_strf_to_strp(date)
        self.close = close

    @classmethod
    def set(cls, index_ticker: str):
        """
        Always set before calling create table method
        """
        cls._table = f"{index_ticker}_price"
        cls.yf_ticker = mp_.get(index_ticker)

    @classmethod
    def set_date(cls, start: str, end: str):
        cls.start = start
        cls.end = end

    @classmethod
    def create_table_rows(cls, row: typing.Tuple[str, float]):
        return cls(*row)

    def insert_query(self) -> str:
        # implement later
        return ""

    @classmethod
    def get_query(cls, date: datetime.datetime) -> str:
        return (
            f"SELECT Date, Close FROM {cls._table} WHERE Date="
            f"'{time.convert_date_to_strf(date)}'"
        )

    @classmethod
    def _create_table(cls, conn: sqlite3.Connection) -> None:
        df: pd.DataFrame = yf_get_price(cls.yf_ticker, cls.start, cls.end)
        df.to_sql(
            cls._table,
            conn,
            index=True,
            if_exists="replace",
            index_label="Date",
        )

    def __str__(self) -> str:
        return f"(Date:{self.date} Close:{self.close})"


class IndexNAV(Table):
    def __init__(
        self,
        date: str,
        ticker: str,
        amount_invested: float,
        nav: float,
    ):
        self.date = time.convert_date_strf_to_strp(date)
        self.ticker = ticker
        self.amount_invested = amount_invested

        self.day_payin: float = 0.0
        self.day_payout: float = 0.0
        self.nav = nav
        self.units: float = 0

    @classmethod
    def set(cls, ticker: str):
        IndexNAV._table = f"{ticker}_index_nav"

    def insert_query(self) -> str:
        return (
            f"INSERT INTO {IndexNAV._table} (Date, ticker, day_payin, "
            f"day_payout, amount_invested, units, nav) "
            f"VALUES ( '{time.convert_date_to_strf(self.date)}', "
            f"'{self.ticker}', {self.day_payin}, {self.day_payout}, "
            f"{self.amount_invested}, {self.units}, {self.nav} )"
        )

    @classmethod
    def _create_table(cls, conn: sqlite3.Connection) -> None:
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
            f"""CREATE TABLE {cls._table} ("Date" DATE, "ticker" TEXT,"""
            f'"day_payin" REAL, "day_payout" REAL, "amount_invested" REAL,'
            f'"units" REAL, "nav" REAL)'
        )

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

    @classmethod
    def create_table_rows(
        cls, row: typing.Tuple[str, str, float, float, float, float, float]
    ):
        date: str = row[0]
        ticker: str = row[1]
        amount_invested: float = row[4]
        nav: float = row[6]
        day_payin: float = row[2]
        day_payout: float = row[3]
        units: float = row[5]

        # initialize object with correct values
        cls_row = cls(date, ticker, amount_invested, nav)
        cls_row.day_payin = day_payin
        cls_row.day_payout = day_payout
        cls_row.units = units
        return cls_row

    def __str__(self) -> str:
        in_n_out = self.day_payin if self.day_payin else self.day_payout
        return (
            f"(Date:{self.date} ticker: {self.ticker} "
            f"in_n_out: {in_n_out} nav: {self.nav} units: {self.units})"
        )

    @classmethod
    def get_query(cls, date: datetime.datetime) -> str:
        return (
            f"SELECT * FROM {cls._table} WHERE Date = '"
            f"{time.convert_date_to_strf(date)}'"
        )

    def reset(self) -> None:
        """
        This function resets the days activities, so that the next
        days' activities can be added to the object
        """
        self.day_payin = 0
        self.day_payout = 0
        self.nav = 0

    def calculate_index_nav(self, conn: sqlite3.Connection) -> None:
        """
        Update the current index nav using self.date day Close
        Warning: The function should only be called only after all
        bnk transactions have been added for the day
        """
        # get index date price
        day_index_price = 0
        Index.set(self.ticker)
        rows: list[Index] = Index.get(conn, self.date)

        if len(rows) == 0:
            raise er.DayClosePriceNotFoundError
        else:
            day_index_price = rows[-1].close

        day_in_n_out = self.day_payin - self.day_payout

        # calculate the amount added or removed that day
        if day_in_n_out > 0:
            self.day_payin = day_in_n_out
            self.amount_invested += self.day_payin
            self.day_payout = 0

        if day_in_n_out < 0:
            self.day_payout = self.day_payout - self.day_payin
            self.amount_invested -= self.day_payout
            self.day_payin = 0

        # calculate the units added or removed that day
        if self.day_payin > 0:
            self.units += self.day_payin / day_index_price
        if self.day_payout > 0:
            self.units -= self.day_payout / day_index_price

        self.nav = self.units * day_index_price

    def add_to_nav(self, bnk_st: BankStatement) -> None:
        [is_credit, amount] = bnk_st.is_credit_debit()

        if is_credit:
            self.day_payin += amount
        else:
            self.day_payout += amount


class Portfolio(Table):
    _table = "portfolio_report"
    portfolio: typing.Dict[str, TradeReport] = {}

    def __init__(self, date: str, ticker: str, isin: str, quantity: float):
        self.date = time.convert_date_strf_to_strp(date)
        self.ticker = ticker
        self.isin = isin
        self.quantity = quantity

    @classmethod
    def _create_table(cls, conn: sqlite3.Connection) -> None:
        cursor = conn.cursor()
        cursor.execute(
            f"""CREATE TABLE portfolio_report """
            '("Date" DATE, "ticker" TEXT, "isin" TEXT,"quantity" REAL)'
        )
        conn.commit()

    @classmethod
    def get_query(cls, date: datetime.datetime) -> str:
        return (
            f"SELECT * FROM portfolio_report "
            f"WHERE Date = '{time.convert_date_to_strf(date)}'"
        )

    def insert_query(self) -> str:
        return (
            "INSERT INTO portfolio_report (Date, ticker, isin, quantity) VALUES"
            f"""('{time.convert_date_to_strf(self.date)}','{self.ticker}',"""
            f"'{self.isin}', {self.quantity})"
        )

    @classmethod
    def create_table_rows(cls, row: typing.Tuple[str, str, str, float]):
        return cls(*row)

    @classmethod
    def add_to_portfolio(cls, trade: TradeReport) -> None:
        if trade.ticker in cls.portfolio:
            if trade.is_buy:
                cls.portfolio[trade.ticker].quantity += trade.quantity
            else:
                cls.portfolio[trade.ticker].quantity -= trade.quantity
        else:
            trade.quantity = trade.quantity if trade.is_buy else -trade.quantity
            cls.portfolio[trade.ticker] = trade

    @classmethod
    def remove_ne_quantity(cls) -> None:
        """
        Removes negative quantities from portfolio
        """
        neg_tickers = []
        for ticker in cls.portfolio:
            if cls.portfolio[ticker].quantity <= 0:
                neg_tickers.append(ticker)

        for ticker in neg_tickers:
            cls.portfolio.pop(ticker)

    @classmethod
    def insert_all(cls, conn: sqlite3.Connection, date: str) -> None:
        """
        Inserts all securities in the cls.portfolio
        """
        cls.remove_ne_quantity()
        table_rows = []
        for key in cls.portfolio.keys():
            pf = cls.portfolio[key]
            portfolio_sec = Portfolio(date, pf.ticker, pf.isin, pf.quantity)
            table_rows.append(portfolio_sec)
        cls.insert_mul(conn, table_rows)

    def __str__(self):
        return (
            f"(Date:{self.date} ticker:{self.ticker}"
            f"quantity:{self.quantity})"
        )


class PortfolioNAV(Table):
    pass
