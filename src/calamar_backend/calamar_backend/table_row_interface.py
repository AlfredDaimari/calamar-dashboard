"""
Utility Table Row Classes
    - TradeReportRow: trade report row class
    - BankStatementRow
    - PortfolioRow
    - IndexRow
    - IndexNavRow
    - PortfolioNavRow
"""
import abc
import pandas as pd
import typing
import sqlite3
import datetime

import calamar_backend.time as time
import calamar_backend.errors as er
import calamar_backend.table_interface as inf_tb
from calamar_backend.database_csv import db_csv


class Row(abc.ABC):
    @abc.abstractmethod
    def insert_query(self, table: str) -> str:
        raise NotImplementedError


class BankStatementRow(Row):
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

    def insert_query(self, table: str) -> str:
        raise NotImplementedError

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
    def is_valid_bank_statement(cls, row: pd.Series) -> bool:
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

    def __str__(self) -> str:
        return (
            f"(Date:{self.date} par:{self.particulars} "
            f"cost_center:{self.cost_center} debit:{-self.debit})"
        )


class TradeReportRow(Row):
    def __init__(
        self, date: str, symbol: str, isin: str, type_: str, quantity: int
    ):
        self.date = time.convert_date_strf_to_strp(date)
        self.ticker = symbol
        self.isin = isin
        self.is_buy: bool = True if type_ == "buy" else False
        self.quantity = quantity

    def insert_query(self, table) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return f"(ticker:{self.ticker} q:{self.quantity} buy:{self.is_buy})"


class IndexRow(Row):
    def __init__(self, date: str, close: float):
        self.date = time.convert_date_strf_to_strp(date)
        self.close = close

    def insert_query(self, table: str) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return f"(Date:{self.date} Close:{self.close})"


class IndexNAVRow(Row):
    def __init__(
        self,
        date: str,
        ticker: str,
        day_payin: float,
        day_payout: float,
        amount_invested: float,
        units: float,
        nav: float,
    ):
        self.date = time.convert_date_strf_to_strp(date)
        self.ticker = ticker
        self.amount_invested = amount_invested

        self.day_payin: float = day_payin
        self.day_payout: float = day_payout
        self.nav: float = nav
        self.units: float = units

    def insert_query(self, table: str) -> str:
        return (
            f"INSERT INTO {table} (Date, ticker, day_payin, "
            f"day_payout, amount_invested, units, nav) "
            f"VALUES ( '{time.convert_date_to_strf(self.date)}', "
            f"'{self.ticker}', {self.day_payin}, {self.day_payout}, "
            f"{self.amount_invested}, {self.units}, {self.nav} )"
        )

    def reset(self) -> None:
        """
        This function resets the days activities, so that the next
        days' activities can be added to the object
        """
        self.day_payin = 0
        self.day_payout = 0
        self.nav = 0

    def __str__(self) -> str:
        in_n_out = self.day_payin if self.day_payin else self.day_payout
        return (
            f"(Date:{self.date} ticker:{self.ticker} "
            f"in_n_out:{in_n_out} nav:{self.nav} units:{self.units})"
        )

    def calculate_index_nav(
        self,
        conn: sqlite3.Connection,
    ) -> None:
        """
        Update the current index nav using self.date day Close
        Warning: The function should only be called only after all
        bnk transactions have been added for the day
        """
        # get index date price
        day_index_price = 0
        index_db = inf_tb.Index(self.ticker)
        rows: typing.Sequence[IndexRow] = index_db.get(conn, self.date)

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

    def add_to_nav(
        self,
        bnk_st: BankStatementRow,
    ) -> None:
        [is_credit, amount] = bnk_st.is_credit_debit()

        if is_credit:
            self.day_payin += amount
        else:
            self.day_payout += amount


class PortfolioRow(Row):
    def __init__(self, date: str, ticker: str, isin: str, quantity: float):
        self.date = time.convert_date_strf_to_strp(date)
        self.ticker = ticker
        self.isin = isin
        self.quantity = quantity

    def insert_query(self, table: str) -> str:
        return (
            f"INSERT INTO {table} (Date, ticker, isin, quantity) VALUES"
            f"""('{time.convert_date_to_strf(self.date)}','{self.ticker}',"""
            f"'{self.isin}', {self.quantity})"
        )

    def __str__(self):
        return (
            f"(Date:{self.date} ticker:{self.ticker} "
            f"quantity:{self.quantity})"
        )


class PortfolioNAVRow(Row):
    def __init__(self, date: str, nav: float = 0):
        self.date = time.convert_date_strf_to_strp(date)
        self.nav = nav

    def insert_query(self, table: str) -> str:
        return (
            f"INSERT INTO {table} (Date, nav) VALUES "
            f"('{time.convert_date_to_strf(self.date)}', {self.nav})"
        )

    def __str__(self):
        return f"(Date:{self.date}) nav:{self.nav}"

    def add_to_nav(
        self,
        portfolio_sec: PortfolioRow,
    ) -> None:
        isin = portfolio_sec.isin
        ticker = portfolio_sec.ticker

        yf_pd_series = db_csv.read(isin, self.date, ticker)[-1]

        if isinstance(yf_pd_series, pd.Series):
            price = yf_pd_series["Close"]
            if isinstance(price, float):
                self.nav += price * portfolio_sec.quantity
        else:
            raise Exception(
                f"{str(datetime.datetime.now())}: PortfolioNAV."
                "add_to_portfolio_nav"
            )
