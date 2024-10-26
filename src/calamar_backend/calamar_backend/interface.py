# file to define the different data structures to be used in baseline library
import datetime
import sqlite3
import pandas as pd
from calamar_backend.errors import DayClosePriceNotFoundError


class Time:
    """
    Time utils
    """

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    YF_DATE_FORMAT = f"{DATE_FORMAT}+00:00"

    def __init__(self, date: str):
        self.date = datetime.datetime.strptime(date, self.DATE_FORMAT)

    def get_date_strf(self):
        return self.date.strftime(self.DATE_FORMAT)

    @staticmethod
    def get_current_date() -> datetime.datetime:
        utc_now = datetime.datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        utc_now_minus_1 = utc_now - datetime.timedelta(days=1)
        return utc_now_minus_1

    @classmethod
    def convert_date_to_strf(cls, date: datetime.datetime):
        return date.strftime(cls.DATE_FORMAT)

    @classmethod
    def convert_yf_date_to_strf(cls, row) -> str:
        """
        Utility function to convert the yf date format into Time class date format
        """
        date: pd.Timestamp = row["Date"]
        return date.strftime(cls.DATE_FORMAT)


class BankStatement(Time):
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
        return BankStatement(*db_tuple)

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

    def __str__(self) -> str:
        return f"Statement:{self.particulars} credit:{self.credit} debit:{self.debit}"


class IndexNav(Time):
    def __init__(
        self,
        date: str | datetime.datetime,
        ticker: str,
        amount_invested: float,
        nav: float,
    ):
        if isinstance(date, str):
            Time.__init__(self, date)
        else:
            self.date = date

        self.ticker = ticker
        self.amount_invested = amount_invested
        self.day_payin: float = 0.0
        self.day_payout: float = 0.0
        self.nav = nav
        self.units: float = 0

    def calculate_index_nav(self, conn: sqlite3.Connection) -> None:
        """
        Update the current index nav using self.date day Close
        Warning: This function should only be called only after all bnk transactions
        have been added for the day
        """
        # get index date price
        day_index_price = 0
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT Close FROM {self.ticker}_price WHERE Date='{self.get_date_strf()}'"
        )
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise DayClosePriceNotFoundError
        else:
            day_index_price = rows[-1][0]

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

    def __str__(self) -> str:
        in_n_out = self.day_payin if self.day_payin else self.day_payout
        return f"Date:{self.get_date_strf()} ticker: {self.ticker} in_n_out: {in_n_out} nav:{self.nav} units:{self.units}"

    @staticmethod
    def create_table_query(ticker: str) -> str:
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
        return f"CREATE TABLE {ticker}_index_nav (Date DATE NOT NULL, ticker TEXT NOT NULL, day_payin REAL NOT NULL, day_payout REAL NOT NULL, amount_invested REAL NOT NULL, units REAL NOT NULL, nav REAL NOT NULL, PRIMARY KEY (Date))"

    def insert_table_query(self) -> str:
        return f"INSERT INTO {self.ticker}_index_nav (Date, ticker, day_payin, day_payout, amount_invested, units, nav) VALUES ( '{self.get_date_strf()}', '{self.ticker}', {self.day_payin}, {self.day_payout}, {self.amount_invested}, {self.units}, {self.nav} )"


class TradeNav(Time):
    def __init__(self):
        pass


class SecurityTrade:
    def __init__(self):
        pass
