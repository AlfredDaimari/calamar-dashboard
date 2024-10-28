import pandas as pd
import numpy as np
from baseline.price import get_security_price
from datetime import datetime

"""
index nav according to zerodha credit debit:
api input(zerodha_debit_credit_csv, yahoo ticker for index, output_json_file)
-> using csv create day payin, payout, amount_invested dictionary
-> write amount_invested to json file
-> use the security database to check if the needed data is present
-> create daily nav with total index amount (might need to use sqlite database to reduce memory size)

TODO:
    daily nav update
"""

"""
portfolio nav according to zerodha buy sell trade reports:
api input(zerodha buy sell trade report, output_json_file)
-> using csv, remove mis trades and create an only cnc trades dictionary
-> remove the negative quantity securities
-> remove the manually selected negative quantity securities
-> check for zerodha ticker to yahoo ticker mappings file 
-> check the data files needed for creating an nav for portfolio
-> using the Security Database, create a nav for the portfolio

TODO:
    daily nav update
"""


class BasePortfolio:
    def __init__(self):
        print("init: base portfolio")
        self.df = None
        self.ticker_mapping = {}

    def __read_csv(self, file: str) -> None:
        print(f"read: reading zerodha trade report - {file}")
        self.df = pd.read_csv(file)

    def __remove_mis_trades(self) -> None:
        """
        In the trade report, remove the mis trades, leaves only the cnc trades
        """
        print("remove_mis_trades: Removing mis trades in zerodha trade report")

        # create a basket of BaseSecurity at day close on each day traded
        """
        json structure of basket
        'day details in some human readable format': List[BaseSecurity]
        """

    def __create_portfolio_nav(self) -> None:
        """
        Create portfolio nav history from the first buy day
        Warning: this function should only be run after setting up a ticker mapping
        """
        print("create_portfolio_nav: Creating portfolio nav since inception")

    def __create_portfolio_nav_extensive(
        self, output_portfolio_json: str
    ) -> None:
        """
        Create an extensive portfolio nav history on each day from inception till last trading day
        """
        print(
            f"create_portfolio_nav_extensive: portfolio extensive nav saved to file - {output_portfolio_json}"
        )

    # ======= from here are functions related to ticker setup ======= #
    def __output_ticker(self, file: str) -> None:
        """
        Output a file with all the tickers found in the zerodha csv in a file
        """
        print(
            f"output_bs_ticker: Writing porfolio ticker symbols collected since inception from zerodha csv file to {file}"
        )
