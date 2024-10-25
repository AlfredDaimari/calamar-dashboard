# file to define the different data structures to be used in baseline library
from typing import Type
import datetime
import sqlite3

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class Time:
    """
    Time utils
    """

    def __init__(self, date: str):
        self.sql__date_format = f"{DATE_FORMAT}+00:00"
        self.date = datetime.datetime.strptime(date, DATE_FORMAT)

    def get_date_strf(self):
        return self.date.strftime(DATE_FORMAT)

    def get_date_strf_index_sql(self):
        """
        Get date in the index sql format style
        """
        return self.date.strftime(f"{DATE_FORMAT}+00:00")


class BankSettlement(Time):
    def __init__(
        self,
        posting_date: str = "",
        particulars: str = "",
        cost_center: str = "",
        voucher_type: str = "",
        debit: float = 0.0,
        credit: float = 0.0,
        net_balance: float = 0.0,
    ):
        Time.__init__(self, posting_date)

        self.posting_date = self.date
        self.particulars = particulars
        self.cost_center = cost_center
        self.voucher_type = (voucher_type,)
        self.debit = debit
        self.credit = credit
        self.net_balance = net_balance

    @staticmethod
    def create_bnk_statement(db_tuple: tuple[str, str, str, str, float, float, float]):
        """
        Takes in the tuple returned from sqlite3 and converts it into BankSettlement object
        """
        return BankSettlement(*db_tuple)

    def __str__(self) -> str:
        return f"Settlement: {self.particulars} credit:{self.credit} debit:{self.debit}"


class IndexNav(Time):
    def __init__(self, date: str, ticker: str, amount_invested: float, nav: float):
        Time.__init__(self, date)
        self.ticker = ticker
        self.amount_invested = amount_invested
        self.nav = nav

    def calculate_index_nav(self, conn: sqlite3.Connection) -> None:
        """
        Update the current index nav using self.date day Close
        """
        pass

    def add_to_nav(self, bnk_st: BankSettlement) -> None:
        pass


class TradeNav(Time):
    def __init__(self):
        pass


class SecurityTrade:
    def __init__(self):
        pass
