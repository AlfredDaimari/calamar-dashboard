# utility class to map my ticker to yahoo tickers
import yaml
import datetime
import os


class TickerMap:
    """
    Converts zerodha tickers to yahoo tickers
    """

    def __init__(self):
        self.map_yaml = os.getenv("TICKER_MAP")

        if self.map_yaml is not None:
            with open(self.map_yaml, "r") as file:
                self.map = yaml.safe_load(file)
        else:
            raise Exception(
                f"{str(datetime.datetime.now())}: "
                "environment variable 'TICKER_MAP' not set"
            )

    def get(self, ticker: str) -> str:
        """
        :parameter ticker: zerodha ticker
        """
        try:
            yticker: str = self.map[ticker]
        except KeyError:
            raise Exception(
                f"{str(datetime.datetime.now())}: zerodha ticker "
                f"- {ticker} not defined in file {self.map_yaml}"
            )
        return yticker


calamar_ticker_map = TickerMap()
