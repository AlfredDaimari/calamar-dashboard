def ticker_to_yf_ticker(ticker: str) -> str:
    """
    zerodha ticker to yahoo ticker
    """
    return ticker + ".NS"
