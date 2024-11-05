"""
    Time utils
"""

import datetime
import pandas as pd
import typing

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
YF_DATE_FORMAT = "%Y-%m-%d"


def convert_date_strf_to_strp(date: str) -> datetime.datetime:
    return datetime.datetime.strptime(date, DATE_FORMAT)


def get_current_date() -> datetime.datetime:
    utc_now = datetime.datetime.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    utc_now_minus_1 = utc_now - datetime.timedelta(days=1)
    return utc_now_minus_1


def convert_date_to_strf(date: datetime.datetime):
    return date.strftime(DATE_FORMAT)


def convert_date_to_strf_yf(date: datetime.datetime):
    return date.strftime(YF_DATE_FORMAT)


def convert_yf_date_to_strf(row) -> str:
    """
    Utility function to convert yf date into Time class date format
    """
    date: pd.Timestamp = row["Date"]
    return date.strftime(DATE_FORMAT)


def range_date(
    start: datetime.datetime, end: datetime.datetime
) -> typing.Generator[datetime.datetime, None, None]:
    current_date: datetime.datetime = start
    while current_date <= end:
        yield current_date
        current_date += datetime.timedelta(days=1)


def date_fy(date: datetime.datetime) -> int:
    """
    Return the financial year of date
    """

    if date.month < 4:
        return date.year
    else:
        return date.year + 1


def date_fy_start_end(date: datetime.datetime) -> tuple[str, str]:
    """
    Returns the start and end date of financial year

    Note: taking two extra days to account for errors
    """
    if date.month < 4:
        return (f"{date.year - 1}-04-01", f"{date.year}-04-02")
    else:
        return (f"{date.year}-04-01", f"{date.year + 1}-04-02")


def date_in_fy_start_end(fy: int) -> tuple[str, str]:
    """
    Note: taking two extra days to account for errors
    """
    return (f"{fy-1}-03-31", f"{fy}-04-02")
