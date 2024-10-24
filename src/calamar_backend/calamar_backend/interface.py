# file to define the different data structures to be used in baseline library
from typing import Type


class BaseSecurity:
    def __init__(self, ticker: str):
        self.ticker: str = ticker
        self.units: float = 0


class PricedSecurity(BaseSecurity):
    def __init__(self, ticker: str):
        BaseSecurity.__init__(self, ticker)
        self.price: float = 0
