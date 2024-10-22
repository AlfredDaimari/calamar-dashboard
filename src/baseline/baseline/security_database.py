# file to handle reading the csv files and get price information
import heapq
import pandas as pd


class SecurityDatabase:
    def __init__(self, ticker: str, fy: str):
        print(f"reading csv: {ticker} for financial year {fy}")

    def query(self, day: str):
        """
        query the csv for a days price
        """

    def add(self):
        """
        add a latest day close price to the csv
        """
        pass


# Limit the size of security database in memory to 20 MB by popping out LRU (only for reading)
class SecurityDatabaseHeap:
    def __init__(self, size=45):
        self.size = size
        self.heap = []

    def query(self, ticker: str, day: str):
        pass


def add_latest_trading_day_close_price(ticker_file):
    """
    Updates each securities price at the end of trading day with day close
    """
    pass
