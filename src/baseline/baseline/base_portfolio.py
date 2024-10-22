import pandas as pd
import numpy as np
from baseline.price import get_security_price
from datetime import datetime


class BasePortfolio:
    def __init__(self):
        print("init: base portfolio")
        self.df = None
        self.ticker_mapping = {}

    def read_csv(self, file: str) -> None:
        print(f"read: reading csv file {file}")
        self.df = pd.read_csv(file)

    def create_baseline_portfolio(self) -> None:
        """
        Creates a portfolio with two asset types
        - stock assets: This consists of equity, etfs, gold, bonds, etc, anything that is purchasable in the stock market
        - cash: when stock assets get sold, this is what is gets converted into

        Future feature:
        - remove cash alongside zerodha csv files for payin and payouts
        """
        print(
            "create_bs_portfolio: Creating a baseline portfolio csv using zerodha csv file"
        )

        # create a basket of BaseSecurity at day close on each day traded
        """
        json structure of basket
        'day details in some human readable format': List[BaseSecurity]
        """

    def create_portfolio_nav_inception(self, bs_portfolio_json: str) -> None:
        """
        Create a portfolio nav history from the first buy day
        Warning: this function should only be run after setting up a ticker mapping
        """
        print("create_bs_nav: Creating portfolio nav using baseline portfolio json")

    def create_portfolio_nav_trading_day(
        self, bs_portfolio_json: str, date: str
    ) -> None:
        """
        Create a portfolio nav history on the current trading day and append information to the current bs portfolio json file
        """

    def to_json_baseline(self, file: str) -> None:
        """
        Create a baseline portfolio json
        :parameter file: json file name
        """
        print(f"create_bs_json: Outputting a baseline portfolio csv to {file}")

    def to_json_nav(self, file: str) -> None:
        """
        Create a baseline portfolio csv
        :parameter file: portfolio holdings csv file name
        """
        print(f"create_nav_json: Outputting a baseline portfolio csv to {file}")

    # ======= from here are functions related to ticker setup ======= #
    def output_ticker(self, file: str) -> None:
        """
        Output a file with all the tickers found in the zerodha csv in a file
        """
        print(
            f"output_bs_ticker: Outputting a baseline porfolio ticker symbols collected since inception from zerodha csv file to {file}"
        )

    def map_ticker_to_portfolio(self, file: str) -> None:
        """
        Create a ticker mapping from the portfolio to yahoo finance api
        """
        print(
            f"create_bs_ticker_map: Create a ticker map from yahoo finance api to base portfolio using {file}"
        )

    # ====== fro here are functions relateed to yahoo finance api ===== #
    def __get_baseline_portfolio_price(self) -> None:
        """
        Using yahoo finance, get the prices for all tickers in the baseline portfolio
        """

        # Baseline portfolio will have a basket of stocks only on traded days
        # Iterate over baseline portfolio day over day
        pass
