# file that consists of getting price functions using the yahoo finance api
import yfinance as yf


def get_security_price(ticker: str, csv_file: str, start: str, end: str) -> None:
    """
    :param ticker: security ticker (must be present in yahoo finance)
    :param csv_file: file to save the price information
    :param start: starting date
    :param end: ending date
    """
    pass
