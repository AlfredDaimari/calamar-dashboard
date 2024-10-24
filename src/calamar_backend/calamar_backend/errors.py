# file for writing error coditions

class DayClosePriceNotFoundError(Exception):
    def __init__(self):
        Exception.__init__(self)
